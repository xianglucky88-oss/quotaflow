"""账本核心：预留/结算/退款/挂起、恒等式、幂等与跨时区日界。"""
from datetime import datetime, timedelta, timezone

import pytest

from src.quotaflow import budget, policy
from src.quotaflow.budget import (
    BudgetExceeded,
    BudgetLedger,
    InvalidTransition,
    LaneExceeded,
    LedgerIntegrityError,
    RefundNotAllowed,
)

UTC = timezone.utc


class Clock:
    """可控时钟：账本看到的"现在"固定为某 UTC 时刻，可手动推进。"""

    def __init__(self, start: datetime):
        self.t = start

    def __call__(self) -> datetime:
        return self.t

    def advance(self, **kw) -> None:
        self.t += timedelta(**kw)


@pytest.fixture()
def clock():
    # 2026-11-18T15:30Z：上海时间 23:30（同一天），洛杉矶时间 07:30
    return Clock(datetime(2026, 11, 18, 15, 30, tzinfo=UTC))


@pytest.fixture()
def ledger(clock):
    return BudgetLedger(budget.connect(":memory:"), policy.DEFAULT_POLICIES, now=clock)


def test_lane_caps_partition_daily_limit_without_overrun():
    # alpha: limit=100, high:low = 3:1 -> 75 / 25
    caps = policy.DEFAULT_POLICIES.get("alpha").lane_caps()
    assert caps == {"high": 75, "low": 25}
    # beta: limit=60, high:low = 2:1 -> 40 / 20
    caps_b = policy.DEFAULT_POLICIES.get("beta").lane_caps()
    assert sum(caps_b.values()) == 60
    # 余数分配确定且总额严格等于限额（资源上限）
    p = policy.PolicyBook.from_mapping({
        "x": {"timezone": "UTC", "daily_limit": 10,
              "lanes": [{"name": "a", "weight": 1}, {"name": "b", "weight": 1},
                        {"name": "c", "weight": 1}]},
    })
    assert sum(p.get("x").lane_caps().values()) == 10


def test_reserve_settle_partial_refunds_remainder(ledger):
    res = ledger.reserve("alpha", "high", "req-1", 10)
    assert res.state == policy.STATE_RESERVED
    summary = ledger.day_summary("alpha", res.day)
    assert summary["reserved"] == 10 and summary["inflight"] == 10
    assert summary["available"] == 90

    settled = ledger.settle("req-1", 7, reason=policy.SETTLE_PARTIAL,
                            evidence={"status": 206, "partial_units": 7})
    assert settled.state == policy.STATE_SETTLED and settled.actual_cost == 7
    summary = ledger.day_summary("alpha", res.day)
    assert summary["spent"] == 7 and summary["released"] == 3
    assert summary["inflight"] == 0 and summary["available"] == 93
    ledger.assert_healthy()


def test_full_refund_releases_entire_reservation(ledger):
    ledger.reserve("alpha", "low", "req-2", 5)
    out = ledger.refund("req-2", "rate_limited",
                        {"status": 429, "retry_after": "30"})
    assert out.state == policy.STATE_REFUNDED
    s = ledger.day_summary("alpha", out.day)
    assert s["reserved"] == 5 and s["released"] == 5 and s["spent"] == 0
    assert s["inflight"] == 0 and s["available"] == 100
    ledger.assert_healthy()


def test_unknown_result_holds_quota_and_blocks_refund(ledger):
    ledger.reserve("alpha", "high", "req-3", 8)
    held = ledger.hold("req-3", "timeout", {"error": "read timed out"})
    assert held.state == policy.STATE_HELD and held.held_units == 8
    # 挂起期间额度仍然占用，绝不释放
    s = ledger.day_summary("alpha", held.day)
    assert s["inflight"] == 8 and s["available"] == 92

    with pytest.raises(RefundNotAllowed):
        ledger.refund("req-3", "timeout", {"error": "read timed out"})
    # 白名单理由但凭据字段缺失同样拒绝
    with pytest.raises(RefundNotAllowed):
        ledger.refund("req-3", "rate_limited", {"status": 429})
    # 非法挂起理由
    with pytest.raises(budget.LedgerError):
        ledger.hold("req-3", "because_i_say_so", {})

    # 对账确认远端未受理后，凭据齐全才能退款
    out = ledger.refund(
        "req-3", "reconciled_not_served",
        {"lookup_request_id": "req-3", "lookup_status": "not_found"},
    )
    assert out.state == policy.STATE_REFUNDED
    ledger.assert_healthy()


def test_reserve_is_idempotent_under_retry_and_restart(ledger):
    a = ledger.reserve("alpha", "high", "req-4", 6, endpoint="/v1/items")
    b = ledger.reserve("alpha", "high", "req-4", 6, endpoint="/v1/items")
    assert a.request_id == b.request_id
    s = ledger.day_summary("alpha", a.day)
    assert s["reserved"] == 6  # 没有第二笔扣费
    # 参数不一致的同名请求必须被拒绝
    with pytest.raises(InvalidTransition):
        ledger.reserve("alpha", "low", "req-4", 6)
    # 终态 request_id 不可复活
    ledger.settle("req-4", 6)
    with pytest.raises(InvalidTransition):
        ledger.reserve("alpha", "high", "req-4", 6)


def test_settle_and_refund_are_idempotent_replays(ledger):
    ledger.reserve("alpha", "high", "req-5", 4)
    ledger.settle("req-5", 4)
    again = ledger.settle("req-5", 4)  # 重放
    assert again.state == policy.STATE_SETTLED
    with pytest.raises(InvalidTransition):
        ledger.settle("req-5", 3)  # 同 id 不同成本
    with pytest.raises(InvalidTransition):
        ledger.refund("req-5", "rate_limited", {"status": 429, "retry_after": "1"})

    ledger.reserve("alpha", "low", "req-6", 2)
    ledger.refund("req-6", "rate_limited", {"status": 429, "retry_after": "1"})
    again2 = ledger.refund("req-6", "rate_limited", {"status": 429, "retry_after": "1"})
    assert again2.state == policy.STATE_REFUNDED
    ledger.assert_healthy()


def test_settle_cannot_exceed_reservation(ledger):
    ledger.reserve("alpha", "high", "req-7", 3)
    with pytest.raises(InvalidTransition):
        ledger.settle("req-7", 4)  # 不允许隐性超额


def test_org_budget_hard_cap(ledger):
    # 机构限额 100：两车道合计最多预留 100
    ledger.reserve("alpha", "high", "a", 75)
    ledger.reserve("alpha", "low", "b", 25)
    with pytest.raises(BudgetExceeded):
        ledger.reserve("alpha", "low", "c", 1)
    # 退款后额度回来，可以再预留
    ledger.refund("b", "rate_limited", {"status": 429, "retry_after": "0"})
    ledger.reserve("alpha", "low", "d", 25)
    ledger.assert_healthy()


def test_lane_fair_share_is_enforced(ledger):
    # high 车道份额 75，即使机构整体还有额度也不能越界
    ledger.reserve("alpha", "high", "h1", 75)
    with pytest.raises(LaneExceeded):
        ledger.reserve("alpha", "high", "h2", 1)
    # low 车道仍可独立使用自己的 25 份额
    ledger.reserve("alpha", "low", "l1", 25)
    ledger.assert_healthy()


def test_day_rollover_uses_org_timezone_and_late_settle_hits_old_day(ledger, clock):
    # 上海 23:30 预留（UTC 15:30）
    before = ledger.reserve("alpha", "high", "night-1", 10)
    assert before.day == "2026-11-18"
    # 推进 2 小时 -> UTC 17:30 -> 上海 2026-11-19 01:30，跨日
    clock.advance(hours=2)
    after = ledger.reserve("alpha", "high", "night-2", 10)
    assert after.day == "2026-11-19"
    # 新配额日额度完整刷新
    assert ledger.day_summary("alpha", "2026-11-19")["available"] == 90
    # 迟到结算写回旧日：按 request_id 定位，不按当前时间重算
    settled = ledger.settle("night-1", 10)
    assert settled.day == "2026-11-18"
    old = ledger.day_summary("alpha", "2026-11-18")
    new = ledger.day_summary("alpha", "2026-11-19")
    assert old["spent"] == 10 and new["spent"] == 0
    ledger.assert_healthy()


def test_dst_boundary_local_day(clock):
    # 2026 年洛杉矶夏令时于 11-01 09:00Z 结束（PDT UTC-7 -> PST UTC-8）。
    # 本地午夜 00:00 仍处 PDT，故对应 07:00Z。
    p = policy.DEFAULT_POLICIES
    # 06:59Z = 本地 10-31 23:59 PDT，仍是前一天
    assert p.local_day("beta", datetime(2026, 11, 1, 6, 59, tzinfo=UTC)) == "2026-10-31"
    # 07:00Z = 本地 11-01 00:00 PDT，跨入新配额日
    assert p.local_day("beta", datetime(2026, 11, 1, 7, 0, tzinfo=UTC)) == "2026-11-01"
    # 回退前 08:30Z = 本地 01:30 PDT
    assert p.local_day("beta", datetime(2026, 11, 1, 8, 30, tzinfo=UTC)) == "2026-11-01"
    # 回退后 09:30Z = 本地 01:30 PST（同一本地钟点出现两次，日期不变）
    assert p.local_day("beta", datetime(2026, 11, 1, 9, 30, tzinfo=UTC)) == "2026-11-01"


def test_default_timezone_is_utc_when_unconfigured():
    book = policy.PolicyBook.from_mapping({
        "ghost": {"daily_limit": 10,
                  "lanes": [{"name": "high", "weight": 1}]},
    })
    assert book.get("ghost").timezone == policy.DEFAULT_TIMEZONE
    assert book.local_day("ghost", datetime(2026, 11, 18, 23, 59, tzinfo=UTC)) == "2026-11-18"


def test_replay_events_reconstructs_counters(ledger):
    ledger.reserve("alpha", "high", "r1", 10)
    ledger.settle("r1", 6, evidence={"status": 206})
    ledger.reserve("alpha", "low", "r2", 4)
    ledger.refund("r2", "rate_limited", {"status": 429, "retry_after": "5"})

    events = ledger.replay("alpha", "2026-11-18")
    kinds = [(e["event"], e["units"]) for e in events]
    assert ("RESERVE", 10) in kinds and ("SETTLE", 6) in kinds
    assert ("REFUND", 4) in kinds and ("REFUND", 4) in kinds  # 结算差额 + 全退
    ledger.assert_healthy()


def test_verify_detects_tampered_counters(ledger):
    ledger.reserve("alpha", "high", "r1", 10)
    ledger.con.execute("UPDATE budget_account SET spent_total = 99")
    ledger.con.commit()
    with pytest.raises(LedgerIntegrityError):
        ledger.assert_healthy()
    assert any("恒等式" in p for p in ledger.verify())


def test_verify_detects_missing_event_stream(ledger):
    ledger.reserve("alpha", "high", "r1", 10)
    ledger.con.execute("DELETE FROM budget_event")
    ledger.con.commit()
    problems = ledger.verify()
    assert any("无事件流水" in p for p in problems)
