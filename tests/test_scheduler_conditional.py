"""scheduler + conditional 联动：公平调度、条件命中、部分响应、
成功后超时重试、游标恢复与挂起对账。"""
from datetime import datetime, timedelta, timezone

import pytest

from src.quotaflow import budget, conditional, incremental, policy, scheduler
from src.quotaflow.budget import BudgetExceeded, BudgetLedger
from src.quotaflow.conditional import ConditionalProcessor, Response, TimeoutError, TransportError

UTC = timezone.utc


class FakeTransport:
    """可编程传输：按调用序号返回预设结果，记录所有发出的请求。"""

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[dict] = []

    def send(self, org, endpoint, params, headers):
        self.calls.append({"org": org, "endpoint": endpoint,
                           "params": params, "headers": dict(headers)})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture()
def ledger():
    return BudgetLedger(budget.connect(":memory:"), policy.DEFAULT_POLICIES)


@pytest.fixture()
def sched(ledger):
    return scheduler.Scheduler(ledger, owner="test-sched")


def test_fair_ordering_priority_then_deadline_then_cost(sched, ledger):
    now = datetime.now(UTC)
    jobs = [
        scheduler.Job("beta", "/b", "low", 2, params={"i": 1}),
        scheduler.Job("alpha", "/a", "low", 5,
                      deadline=now + timedelta(seconds=10), params={"i": 2}),
        scheduler.Job("alpha", "/a", "high", 3, params={"i": 3}),  # 高优先级应最先
        scheduler.Job("alpha", "/c", "low", 1,
                      deadline=now + timedelta(seconds=5), params={"i": 4}),  # 同 low 中更紧迫
    ]
    result = sched.schedule(jobs)
    order = [(p.job.org, p.job.endpoint, p.job.lane) for p in result.planned]
    assert order[0] == ("alpha", "/a", "high")          # weight=3 最先
    # 两个 low 有 deadline 的排在无 deadline 前；更紧迫的在前
    low_order = [o for o in order if o[2] == "low"]
    assert low_order == [("alpha", "/c", "low"),
                         ("alpha", "/a", "low"),
                         ("beta", "/b", "low")]


def test_deferred_when_budget_exhausted(sched, ledger):
    # high 车道份额 75
    jobs = [scheduler.Job("alpha", f"/e{i}", "high", 40) for i in range(2)]
    result = sched.schedule(jobs)
    assert len(result.planned) == 1
    assert len(result.deferred) == 1
    assert result.deferred[0].reason == "lane_exceeded"
    ledger.assert_healthy()


def test_304_settles_at_conditional_cost_and_keeps_cursor(sched, ledger):
    job = scheduler.Job("alpha", "/items", "high", 10)
    plan = sched.schedule([job]).planned[0]
    transport = FakeTransport([
        Response(200, {"ETag": '"v1"', "Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT",
                       "X-Next-Cursor": "page-2"}),
    ])
    proc = ConditionalProcessor(ledger, transport)
    out = proc.execute(plan)
    assert out.action == "settled"
    cur = incremental.load_cursor(ledger.con, "alpha", "/items")
    assert cur.cursor == "page-2" and cur.etag == '"v1"'

    # 第二轮：游标已推进到 page-2，调度器自动派生新轮次 request_id，
    # 并携带上一轮持久化的条件请求头
    plan2 = sched.schedule([job]).planned[0]
    assert plan2.request_id != plan.request_id
    assert plan2.headers["If-None-Match"] == '"v1"'
    transport2 = FakeTransport([Response(304)])
    out2 = ConditionalProcessor(ledger, transport2).execute(plan2)
    assert out2.state == "SETTLED" and out2.detail["actual_cost"] == 1
    # 两轮逐笔对应：10 + 1 的真实消耗，没有重复扣费
    days = {ledger.get(plan.request_id).day, ledger.get(plan2.request_id).day}
    spent_total = sum(ledger.day_summary("alpha", d)["spent"] for d in days)
    assert spent_total == 11
    cur2 = incremental.load_cursor(ledger.con, "alpha", "/items")
    assert cur2.cursor == "page-2"  # 304 不推进也不破坏游标
    ledger.assert_healthy()


def test_206_partial_response_settles_actual_units(sched, ledger):
    job = scheduler.Job("alpha", "/stream", "high", 10)
    plan = sched.schedule([job]).planned[0]
    transport = FakeTransport([
        Response(206, {"X-Next-Cursor": "chunk-4"}, partial_units=4),
    ])
    out = ConditionalProcessor(ledger, transport).execute(plan)
    assert out.action == "settled" and out.detail["actual_cost"] == 4
    res = ledger.get(plan.request_id)
    assert res.state == "SETTLED" and res.reason == "partial"
    s = ledger.day_summary("alpha", res.day)
    assert s["spent"] == 4 and s["released"] == 6  # 差额自动释放
    cur = incremental.load_cursor(ledger.con, "alpha", "/stream")
    assert cur.cursor == "chunk-4"
    ledger.assert_healthy()


def test_429_refunds_with_evidence_and_retry_can_reserve_again(sched, ledger):
    job = scheduler.Job("alpha", "/rate", "low", 5)
    plan = sched.schedule([job]).planned[0]
    rid = plan.request_id
    transport = FakeTransport([Response(429, {"Retry-After": "30"})])
    out = ConditionalProcessor(ledger, transport).execute(plan)
    assert out.action == "refunded"
    assert ledger.get(rid).state == "REFUNDED"
    day = ledger.get(rid).day
    assert ledger.day_summary("alpha", day)["available"] == 100

    # 新逻辑请求（新 request_id）可立即再预留；旧 id 不可复活
    retry_job = scheduler.Job("alpha", "/rate", "low", 5,
                              params={"page": 1})  # 参数不同 -> 新 id
    plan2 = sched.schedule([retry_job]).planned[0]
    assert plan2.request_id != rid
    transport2 = FakeTransport([
        Response(200, {"ETag": '"e2"', "X-Next-Cursor": "p2"}),
    ])
    ConditionalProcessor(ledger, transport2).execute(plan2)
    ledger.assert_healthy()


def test_timeout_holds_quota_then_idempotent_retry_settles_once(sched, ledger):
    """关键场景：请求成功但响应丢失（超时）-> 重试必须只扣一次费。"""
    job = scheduler.Job("alpha", "/flaky", "high", 8)
    plan = sched.schedule([job]).planned[0]
    rid = plan.request_id

    # 第一次：超时（远端实际可能已受理），结果未知 -> HELD
    transport1 = FakeTransport([TimeoutError("read timed out")])
    out1 = ConditionalProcessor(ledger, transport1).execute(plan)
    assert out1.action == "held" and ledger.get(rid).state == "HELD"
    day = ledger.get(rid).day
    s = ledger.day_summary("alpha", day)
    assert s["inflight"] == 8 and s["spent"] == 0  # 额度仍挂账

    # 恢复/重排：同一 request_id，reserve 幂等返回同一笔预留，不产生新扣费
    recovered = sched.recover()
    assert rid in recovered
    result2 = sched.schedule([job])
    plan2 = result2.planned[0]
    assert plan2.request_id == rid and plan2.attempt == 2
    assert ledger.day_summary("alpha", day)["reserved"] == 8  # 仍是同一笔

    # 条件重试拿到 200（远端以 Idempotency-Key 去重，只处理一次）
    transport2 = FakeTransport([
        Response(200, {"ETag": '"e"', "X-Next-Cursor": "p"}),
    ])
    out2 = ConditionalProcessor(ledger, transport2).execute(plan2)
    assert out2.action == "settled"
    # 逐笔对应：该 request_id 只有一笔预留、一笔结算，成本 8
    res = ledger.get(rid)
    assert res.state == "SETTLED" and res.actual_cost == 8
    final = ledger.day_summary("alpha", day)
    assert final["spent"] == 8 and final["inflight"] == 0
    events = [e for e in ledger.replay() if e["request_id"] == rid]
    assert [e["event"] for e in events].count("RESERVE") == 1
    ledger.assert_healthy()


def test_connection_error_and_5xx_hold_not_refund(sched, ledger):
    proc_holder = []

    def run_one(script_item, expect_reason):
        job = scheduler.Job("beta", "/x", "high", 4, params={"n": len(proc_holder)})
        plan = sched.schedule([job]).planned[0]
        transport = FakeTransport([script_item])
        out = ConditionalProcessor(ledger, transport).execute(plan)
        assert out.action == "held"
        assert ledger.get(plan.request_id).state == "HELD"
        # 未知结果下任何退款尝试都被账本拒绝
        with pytest.raises(budget.RefundNotAllowed):
            ledger.refund(plan.request_id, "timeout", {})
        proc_holder.append(plan.request_id)

    run_one(TransportError("conn reset"), "connection_error")
    run_one(Response(503), "http_5xx")
    run_one(Response(408), "http_408")
    ledger.assert_healthy()


def test_reconciliation_path_refunds_held_reservation_with_evidence(sched, ledger):
    job = scheduler.Job("alpha", "/audit", "low", 6)
    plan = sched.schedule([job]).planned[0]
    proc = ConditionalProcessor(ledger, FakeTransport([TimeoutError("t/o")]))
    proc.execute(plan)
    assert ledger.get(plan.request_id).state == "HELD"
    # 人工对账：向远端反查确认未受理
    out = proc.reconcile_refund(
        plan.request_id, "reconciled_not_served",
        {"lookup_request_id": plan.request_id, "lookup_status": "not_found"},
    )
    assert out.action == "refunded"
    ledger.assert_healthy()


def test_cursor_persists_across_ledger_reopen(tmp_path):
    db = str(tmp_path / "cursors.db")
    led1 = BudgetLedger(budget.connect(db), policy.DEFAULT_POLICIES)
    s1 = scheduler.Scheduler(led1)
    job = scheduler.Job("alpha", "/paged", "high", 5)
    plan = s1.schedule([job]).planned[0]
    ConditionalProcessor(
        led1,
        FakeTransport([Response(200, {"ETag": '"e9"', "X-Next-Cursor": "p-9"})]),
    ).execute(plan)
    led1.con.close()

    led2 = BudgetLedger(budget.connect(db), policy.DEFAULT_POLICIES)
    cur = incremental.load_cursor(led2.con, "alpha", "/paged")
    assert cur.cursor == "p-9" and cur.etag == '"e9"'
    assert cur.last_request_id == job.request_id
    # 恢复时同一请求仍可定位到同一预留行
    assert led2.get(job.request_id).state == "SETTLED"
    led2.assert_healthy()


def test_attempt_counter_derives_from_persisted_events(sched, ledger):
    job = scheduler.Job("alpha", "/retry", "high", 2)
    p1 = sched.schedule([job]).planned[0]
    assert p1.attempt == 1
    ConditionalProcessor(ledger, FakeTransport([TimeoutError("t")])).execute(p1)
    p2 = sched.schedule([job]).planned[0]
    assert p2.attempt == 2 and p2.request_id == p1.request_id  # 同轮重试复用同键
    # 再次超时：两次尝试均失败；max_attempts=2 的作业第三次进入时被拒绝
    ConditionalProcessor(ledger, FakeTransport([TimeoutError("t")])).execute(p2)
    exhausted = scheduler.Job("alpha", "/retry", "high", 2, max_attempts=2)
    result = sched.schedule([exhausted])
    assert not result.planned
    assert result.deferred[0].reason == "max_attempts"
    # 整轮只有一笔预留（重试不产生新扣费），额度仍挂账等待对账
    assert len(ledger.reservations_in_state(policy.STATE_HELD)) == 1
    ledger.assert_healthy()
