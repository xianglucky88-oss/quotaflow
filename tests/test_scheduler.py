"""调度器：确定性冲突规则、可解释跳过、重启恢复、多机构夹具。

冲突规则（固定优先级元组，越小越先）：

``(截止紧迫度, 防饥饿位, 重试位, -机构优先级, 预期成本, -已等待轮次, 名称)``

即：截止 > 防饥饿 > 重试 > 优先级 > 缓存成本 > 等待轮次 > 名称。
"""
import json

from src.quotaflow import conditional, scheduler
from tests.conftest import build, load_fixture, run_rounds, summary

NOW = 1_000_000.0


def simple_scheduler(now=NOW, **kwargs):
    sources = kwargs.pop("sources", [
        {"name": "alpha", "priority": 10, "batch_limit": 5},
        {"name": "beta", "priority": 5, "batch_limit": 5},
    ])
    cursors = kwargs.pop("cursors", {
        "alpha": {"committed": 0, "head": 50},
        "beta": {"committed": 0, "head": 50},
    })
    return scheduler.Scheduler(sources, budget_limit=kwargs.pop("budget_limit", 100),
                               cursors=cursors, now=now, **kwargs)


# --------------------------------------------------------------- 基础选择
def test_higher_priority_goes_first():
    sch = simple_scheduler()
    first = sch.next_task()
    assert first.source == "alpha"
    assert first.interval == [0, 5]
    assert first.reason == scheduler.DISPATCH_ADVANCE


def test_failed_interval_is_retried_before_advancing():
    sch = simple_scheduler(sources=[{"name": "a", "batch_limit": 5,
                                     "backoff_base": 1}],
                           cursors={"a": {"committed": 0, "head": 20}})
    d = sch.next_task()
    sch.fail(d, "503")
    # 失败后不能把游标当成已推进
    assert sch.coverage()["a"]["committed"] == 0
    # 退避期内不会立刻重试
    assert sch.next_task() is None
    assert sch.stop_reason == scheduler.SKIPPED_BACKOFF
    sch.advance(2)
    retry = sch.next_task()
    assert retry.reason == scheduler.DISPATCH_RETRY
    assert retry.interval == [0, 5]


def test_partial_progress_only_confirms_prefix():
    sch = simple_scheduler(sources=[{"name": "a", "batch_limit": 10}],
                           cursors={"a": {"committed": 0, "head": 20}})
    d = sch.next_task()
    sch.record_partial(d, 4)
    assert sch.coverage()["a"]["committed"] == 4
    assert sch.coverage()["a"]["unconfirmed"] == [{"start": 4, "end": 10}]
    nxt = sch.next_task()
    assert nxt.reason == scheduler.DISPATCH_RETRY
    assert nxt.interval == [4, 10]


def test_completed_sources_are_skipped_as_complete():
    sch = simple_scheduler(sources=[{"name": "a", "batch_limit": 5}],
                           cursors={"a": {"committed": 5, "head": 5}})
    d = sch.next_task()
    assert d is None
    assert sch.stop_reason == scheduler.SKIPPED_COMPLETE
    assert sch.deferrals()[-1]["source"] == "a"
    assert sch.deferrals()[-1]["reason"] == "complete"


# --------------------------------------------------------------- 冲突规则
def test_deadline_outranks_priority_and_fairness():
    now = NOW
    sources = [
        {"name": "hot", "priority": 100, "batch_limit": 5, "deadline": now + 86400},
        {"name": "due", "priority": 1, "batch_limit": 5, "deadline": now + 60},
    ]
    cursors = {"hot": {"committed": 0, "head": 1000},
               "due": {"committed": 0, "head": 10}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors,
                              now=now, config={"starvation_rounds": 2})
    d = sch.next_task()
    assert d.source == "due"
    assert d.deadline_tier == 0


def test_deadline_higher_than_starvation():
    now = NOW
    sources = [
        {"name": "urgent", "priority": 1, "batch_limit": 5,
         "deadline": now + 30},
        {"name": "starving", "priority": 2, "batch_limit": 5},
    ]
    # 剩余工作量 20 条、单批 5 条、每批 5 秒、剩余 30 秒 => 仍可能赶上截止
    cursors = {"urgent": {"committed": 0, "head": 20},
               "starving": {"committed": 0, "head": 100}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors,
                              now=now, config={"starvation_rounds": 1})
    # 先让两家都积累等待轮次，制造“starving 已饥饿”的局面
    for _ in range(2):
        d = sch.next_task()
        sch.complete(d)
        sch.advance(1)
    # 虽然 starving 优先级更高，urgent 已进入临界窗口 => 截止先于防饥饿
    d = sch.next_task()
    assert d.source == "urgent"
    assert d.deadline_tier == 0
    rows = {r["source"]: r for r in sch.explain()}
    assert rows["starving"]["starved"] is True
    assert rows["urgent"]["rank"][0] == 0


def test_unreachable_deadline_does_not_starve_others():
    """截止已不可能赶上时不能再以“紧迫”为由无限抢占，否则会饿死别家。"""
    now = NOW
    sources = [
        {"name": "huge", "priority": 1, "batch_limit": 1,
         "deadline": now + 100},
        {"name": "small", "priority": 100, "batch_limit": 5},
    ]
    cursors = {"huge": {"committed": 0, "head": 10000},
               "small": {"committed": 0, "head": 100}}
    sch = scheduler.Scheduler(sources, budget_limit=10000, cursors=cursors,
                              now=now, config={"starvation_rounds": 2,
                                               "round_seconds": 1.0})
    order = []
    for _ in range(4):
        d = sch.next_task()
        order.append(d.source)
        sch.complete(d)
        sch.advance(1)
    rows = {r["source"]: r for r in sch.explain()}
    assert rows["huge"]["deadline_tier"] == 2      # 退回普通竞争，而非“紧迫”
    assert "small" in order                        # 防饥饿仍然生效
    state = sch.deadline_state("huge")
    assert state["reachable"] is False


def test_starvation_overtakes_priority():
    sources = [
        {"name": "hog", "priority": 100, "batch_limit": 5},
        {"name": "quiet", "priority": 1, "batch_limit": 5},
    ]
    cursors = {"hog": {"committed": 0, "head": 1000},
               "quiet": {"committed": 0, "head": 1000}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors,
                              now=NOW, config={"starvation_rounds": 2,
                                               "max_per_source": 1})
    order = []
    for _ in range(6):
        d = sch.next_task()
        order.append(d.source)
        sch.complete(d)
        sch.advance(1)
    # 高优先级不会被永久垄断：quiet 在等待 2 轮后一定被派发
    assert order.count("quiet") >= 2
    assert order[0] == "hog"
    assert "quiet" in order[1:3]


def test_starvation_and_deadline_tie_is_deterministic():
    """同一紧迫度、同一饥饿位时，按优先级再按名称确定排序。"""
    sources = [
        {"name": "b", "priority": 5, "batch_limit": 5},
        {"name": "a", "priority": 5, "batch_limit": 5},
    ]
    cursors = {"a": {"committed": 0, "head": 10}, "b": {"committed": 0, "head": 10}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors, now=NOW)
    order = []
    for _ in range(2):
        d = sch.next_task()
        order.append(d.source)
        sch.complete(d)
    assert order == ["a", "b"]


def test_priority_beats_cache_cost_only_after_retry_and_fairness():
    sources = [{"name": "expensive", "priority": 5, "batch_limit": 5},
               {"name": "cached", "priority": 1, "batch_limit": 5}]
    cursors = {"expensive": {"committed": 0, "head": 10},
               "cached": {"committed": 0, "head": 10}}
    cache = conditional.CacheStore()
    cache.put("cached", (0, 5), body={"rows": []})
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors,
                              cache=cache, now=NOW)
    # 优先级高者先; 缓存命中者不扣预算但在同一轮内排后
    decisions = sch.next_batch(2)
    dispatched = [d.source for d in decisions if d.dispatched]
    assert dispatched == ["expensive", "cached"]
    assert int(sch.ledger.remaining) == 99


# --------------------------------------------------------------- 预算争抢
def test_expensive_source_charges_its_cost_and_cannot_overspend():
    sources = [
        {"name": "heavy", "priority": 10, "batch_limit": 3, "cost": 3},
        {"name": "light", "priority": 1, "batch_limit": 3},
    ]
    cursors = {"heavy": {"committed": 0, "head": 10},
               "light": {"committed": 0, "head": 10}}
    sch = scheduler.Scheduler(sources, budget_limit=4, cursors=cursors, now=NOW)
    batch = sch.next_batch(2)
    by_source = {d.source: d for d in batch}
    assert by_source["heavy"].dispatched
    assert by_source["heavy"].cost == 3
    # 只剩 1，light 仍可派发；再要 3 就不可能了
    assert by_source["light"].dispatched
    assert sch.ledger.spent == {"heavy": 3, "light": 1}
    assert sch.next_task() is None
    assert sch.stop_reason == scheduler.SKIPPED_GLOBAL_BUDGET


def test_reservation_deadlock_is_overridden_once():
    sources = [
        {"name": "x", "priority": 5, "batch_limit": 2, "reserve": 1},
        {"name": "y", "priority": 1, "batch_limit": 2, "reserve": 1},
    ]
    cursors = {"x": {"committed": 0, "head": 10}, "y": {"committed": 0, "head": 10}}
    sch = scheduler.Scheduler(sources, budget_limit=1, cursors=cursors, now=NOW)
    batch = sch.next_batch(2)
    winner = next(d for d in batch if d.dispatched)
    assert winner.source == "x"           # 排名最靠前者被放行
    assert "reservation overridden" in winner.detail
    loser = next(d for d in batch if not d.dispatched)
    assert loser.reason == scheduler.SKIPPED_RESERVED
    # 只放行一次，预算已尽
    assert sch.next_task() is None
    assert sch.stop_reason == scheduler.SKIPPED_GLOBAL_BUDGET


def test_batch_full_is_audited():
    sources = [{"name": "a", "batch_limit": 5}, {"name": "b", "batch_limit": 5}]
    cursors = {"a": {"committed": 0, "head": 10}, "b": {"committed": 0, "head": 10}}
    sch = scheduler.Scheduler(sources, budget_limit=10, cursors=cursors, now=NOW)
    batch = sch.next_batch(1)
    skipped = [d for d in batch if not d.dispatched]
    assert skipped and all(d.reason == scheduler.SKIPPED_BATCH_FULL for d in skipped)
    assert sch.round == 1


def test_source_quota_exhaustion_is_explained():
    sources = [{"name": "a", "batch_limit": 5}, {"name": "b", "batch_limit": 5}]
    cursors = {"a": {"committed": 0, "head": 50}, "b": {"committed": 0, "head": 50}}
    sch = scheduler.Scheduler(sources, ledger={"limit": 10, "allocations": {"a": 2}},
                              cursors=cursors, now=NOW)
    for _ in range(3):
        sch.next_batch(1)
    d = sch.next_batch(1)
    reasons = {(x.source, x.reason) for x in d if not x.dispatched}
    assert ("a", scheduler.SKIPPED_SOURCE_QUOTA) in reasons
    row = next(x for x in sch.deferrals(round_no=sch.round)
               if x["source"] == "a")
    assert "available 0" in row["detail"]


def test_global_budget_exhaustion_stops_dispatch():
    sources = [{"name": "a", "batch_limit": 5}, {"name": "b", "batch_limit": 5}]
    cursors = {"a": {"committed": 0, "head": 50}, "b": {"committed": 0, "head": 50}}
    sch = scheduler.Scheduler(sources, budget_limit=2, cursors=cursors, now=NOW)
    first = sch.next_batch(2)
    assert sum(d.dispatched for d in first) == 2
    second = sch.next_batch(2)
    assert not any(d.dispatched for d in second)
    assert sch.stop_reason == scheduler.SKIPPED_GLOBAL_BUDGET
    assert {d.reason for d in second} == {scheduler.SKIPPED_GLOBAL_BUDGET}


def test_reserve_protects_small_source_from_hog():
    sources = [
        {"name": "hog", "priority": 100, "batch_limit": 2},
        {"name": "small", "priority": 1, "batch_limit": 2, "reserve": 2},
    ]
    cursors = {"hog": {"committed": 0, "head": 100},
               "small": {"committed": 0, "head": 4}}
    sch = scheduler.Scheduler(sources, budget_limit=5, cursors=cursors, now=NOW,
                              config={"starvation_rounds": 3})
    used = {"hog": 0, "small": 0}
    for _ in range(6):
        d = sch.next_task()
        if d is None:
            break
        used[d.source] += 1
        sch.complete(d)
        sch.advance(1)
    assert used["small"] == 2       # 保底被兑现
    assert sch.requests_used() == 5


def test_zero_cost_cache_hit_does_not_consume_budget():
    sources = [{"name": "a", "batch_limit": 5}]
    cursors = {"a": {"committed": 0, "head": 5}}
    cache = conditional.CacheStore()
    cache.put("a", (0, 5), body={"rows": []})
    sch = scheduler.Scheduler(sources, budget_limit=1, cursors=cursors,
                              cache=cache, now=NOW)
    d = sch.next_task()
    assert d.reason == scheduler.DISPATCH_CACHE
    assert d.cost == 0
    assert sch.requests_used() == 0
    sch.complete(d)
    assert sch.coverage()["a"]["committed"] == 5


# --------------------------------------------------------------- 失败与熔断
def test_backoff_blocks_immediate_retry():
    sources = [{"name": "a", "batch_limit": 5, "backoff_base": 60}]
    cursors = {"a": {"committed": 0, "head": 20}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors, now=NOW)
    d = sch.next_task()
    sch.fail(d, "timeout")
    blocked = sch.next_batch(1)
    assert not any(x.dispatched for x in blocked)
    assert sch.stop_reason == scheduler.SKIPPED_BACKOFF
    sch.advance(61)
    retry = sch.next_task()
    assert retry.source == "a"
    assert retry.reason == scheduler.DISPATCH_RETRY


def test_single_source_long_failure_opens_circuit_and_frees_others():
    sources = [
        {"name": "bad", "priority": 100, "batch_limit": 5,
         "max_consecutive_failures": 2, "backoff_base": 5,
         "quarantine_base": 100, "quarantine_cap": 400},
        {"name": "good", "priority": 1, "batch_limit": 5},
    ]
    cursors = {"bad": {"committed": 0, "head": 50},
               "good": {"committed": 0, "head": 50}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors, now=NOW)
    # 两次失败 -> 熔断
    for _ in range(2):
        d = sch.next_task()
        assert d.source == "bad"
        sch.fail(d, "503")
        sch.advance(6)
    assert sch.runtime["bad"].circuit == "open"
    # 熔断期间另一家照常推进，不会被坏接口拖到截止
    d = sch.next_task()
    assert d.source == "good"
    skips = {x["source"]: x["reason"] for x in sch.deferrals(round_no=sch.round)}
    assert skips["bad"] == scheduler.SKIPPED_CIRCUIT
    # 冷却结束后半开探测一次
    sch.advance(400)
    probe = sch.next_task()
    assert probe.source == "bad"
    assert probe.reason == scheduler.DISPATCH_PROBE


def test_success_after_half_open_closes_circuit():
    sources = [{"name": "bad", "batch_limit": 5, "max_consecutive_failures": 1,
                "backoff_base": 1, "quarantine_base": 10}]
    cursors = {"bad": {"committed": 0, "head": 20}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors, now=NOW)
    d = sch.next_task()
    sch.fail(d, "503")
    assert sch.runtime["bad"].circuit == "open"
    sch.advance(11)
    probe = sch.next_task()
    assert probe.reason == scheduler.DISPATCH_PROBE
    sch.complete(probe)
    assert sch.runtime["bad"].circuit == "closed"
    assert sch.runtime["bad"].consecutive_failures == 0


def test_failure_keeps_interval_unconfirmed_for_later_retry():
    sources = [{"name": "a", "batch_limit": 5, "backoff_base": 1}]
    cursors = {"a": {"committed": 0, "head": 10}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors, now=NOW)
    d = sch.next_task()
    sch.fail(d, "boom")
    sch.advance(2)
    retry = sch.next_task()
    assert retry.interval == d.interval
    assert retry.reason == scheduler.DISPATCH_RETRY


# --------------------------------------------------------------- 可解释性
def test_explain_lists_rank_and_skip_reasons():
    sch = simple_scheduler()
    sch.next_batch(1)
    rows = sch.explain()
    assert {r["source"] for r in rows} == {"alpha", "beta"}
    for row in rows:
        assert len(row["rank"]) == 8
        assert row["action"] in {"dispatch", "skip"}
        assert row["reason"]


def test_deferrals_trace_why_a_source_was_skipped():
    sources = [{"name": "done", "batch_limit": 5}]
    cursors = {"done": {"committed": 5, "head": 5}}
    sch = scheduler.Scheduler(sources, budget_limit=10, cursors=cursors, now=NOW)
    sch.next_batch(1)
    rows = sch.deferrals(round_no=1)
    assert rows and rows[0]["source"] == "done"
    assert rows[0]["reason"] == scheduler.SKIPPED_COMPLETE
    assert "caught up" in rows[0]["detail"]


# --------------------------------------------------------------- 重启恢复
def test_restart_replays_identical_order_and_spend(tmp_path):
    data = load_fixture()
    live, _ = build(data)
    first = run_rounds(live, data, 6, start=1)

    path = tmp_path / "state.json"
    live.save(path)
    restored = scheduler.Scheduler.load(path)
    assert restored.round == live.round
    assert restored.ledger.to_dict() == live.ledger.to_dict()

    expected = run_rounds(live, data, 6, start=7)
    # 用一份“从未中断”的参考调度器做对照
    reference, _ = build(data)
    reference_log = run_rounds(reference, data, 12, start=1)
    actual = run_rounds(restored, data, 6, start=7)

    assert [r["source"] for r in actual] == [r["source"] for r in expected]
    assert [r["interval"] for r in actual] == [r["interval"] for r in expected]
    assert summary(restored)["spent"] == summary(reference)["spent"]
    assert summary(restored)["coverage"] == summary(reference)["coverage"]
    assert [r["source"] for r in reference_log[:6]] == [r["source"] for r in first]


def test_restart_keeps_circuit_and_backoff_state(tmp_path):
    sources = [{"name": "bad", "batch_limit": 5, "max_consecutive_failures": 2,
                "backoff_base": 10, "quarantine_base": 1000}]
    cursors = {"bad": {"committed": 0, "head": 50}}
    sch = scheduler.Scheduler(sources, budget_limit=100, cursors=cursors, now=NOW)
    d = sch.next_task()
    sch.fail(d, "503")
    sch.advance(11)
    d = sch.next_task()
    assert d is not None and d.source == "bad"
    sch.fail(d, "503")
    assert sch.runtime["bad"].circuit == "open"

    path = tmp_path / "state.json"
    sch.save(path)
    restored = scheduler.Scheduler.load(path)
    assert restored.runtime["bad"].circuit == "open"
    assert restored.next_task() is None
    assert restored.stop_reason == scheduler.SKIPPED_CIRCUIT


def test_checkpoint_is_atomic_and_json(tmp_path):
    sch = simple_scheduler()
    sch.next_batch(1)
    path = tmp_path / "nested" / "state.json"
    sch.save(path)
    assert path.exists()
    assert not (tmp_path / "nested" / "state.json.tmp").exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert payload["history"]


# --------------------------------------------------------------- 多机构夹具
def test_multi_source_fixture_covers_failure_partial_and_exhaustion():
    data = load_fixture()
    sch, _ = build(data)
    log = run_rounds(sch, data, 26, start=1)
    coverage = sch.coverage()

    # beta / gamma 在坏机构失败期间继续推进（完成覆盖）
    assert coverage["beta"]["committed"] > 0
    assert coverage["gamma"]["committed"] > 0
    # beta 的部分完成留下未确认区间，没有被伪装成“已完成”
    assert coverage["beta"]["pending"] >= 1
    # alpha 连续失败，区间保持未确认，且失败期间其他机构照常推进
    assert sch.runtime["alpha"].total_failures >= 2
    assert coverage["alpha"]["pending"] >= 1
    # 额度最终耗尽，且没有超支
    assert sch.requests_used() <= 22
    assert int(sch.ledger.remaining) >= 0
    # 每一轮的跳过都能追到原因
    for entry in log:
        for source, reason in entry["skips"]:
            assert reason in scheduler.SKIP_REASONS
    # 请求量可解释：成功/失败/缓存命中之和接近派发次数
    dispatched = [d for d in sch.history if d.dispatched]
    assert len(dispatched) == len({d.seq for d in dispatched})
    assert sch.requests_used() <= len(dispatched)


def test_fixture_restart_matches_uninterrupted_run(tmp_path):
    data = load_fixture()
    interrupted, _ = build(data)
    run_rounds(interrupted, data, 7, start=1)
    path = tmp_path / "state.json"
    interrupted.save(path)
    resumed = scheduler.Scheduler.load(path)
    resumed_log = run_rounds(resumed, data, 12, start=8)

    reference, _ = build(data)
    reference_log = run_rounds(reference, data, 19, start=1)

    assert [r["source"] for r in resumed_log] == \
        [r["source"] for r in reference_log[7:]]
    assert summary(resumed)["spent"] == summary(reference)["spent"]
    assert summary(resumed)["coverage"] == summary(reference)["coverage"]


def test_happy_path_process_unchanged():
    rec = {"id": "r-001", "src": "alpha", "value": 10}
    out = scheduler.process(rec)
    assert out["_stage_scheduler"] == "naive"
    assert out == {**rec, "_stage_scheduler": "naive"}
