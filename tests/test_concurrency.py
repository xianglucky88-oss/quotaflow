"""并发抢占：多连接/多线程争抢最后额度，验证绝不超额、无重复扣费。"""
import sqlite3
import threading
from datetime import datetime, timezone

from src.quotaflow import budget, policy
from src.quotaflow.budget import BudgetExceeded, BudgetLedger, LaneExceeded


def _open(path):
    con = budget.connect(path)
    return BudgetLedger(con, policy.DEFAULT_POLICIES)


def test_concurrent_race_for_last_units(tmp_path):
    db = str(tmp_path / "race.db")
    # 机构限额 20，high 车道份额 15；30 个线程各抢 1 单位
    book = policy.PolicyBook.from_mapping({
        "alpha": {"timezone": "Asia/Shanghai", "daily_limit": 20,
                  "lanes": [{"name": "high", "weight": 3}, {"name": "low", "weight": 1}]},
    })
    con0 = budget.connect(db)
    BudgetLedger(con0, book)  # 建表
    con0.close()

    winners: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker(i: int):
        led = BudgetLedger(budget.connect(db), book)
        try:
            res = led.reserve("alpha", "high", f"race-{i:03d}", 1, owner=f"t{i}")
            with lock:
                winners.append(res.request_id)
        except (BudgetExceeded, LaneExceeded, sqlite3.OperationalError) as exc:
            with lock:
                errors.append(exc)
        finally:
            led.con.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    audit = BudgetLedger(budget.connect(db), book)
    # high 车道 cap = 15（3/4 * 20），精确赢家数
    assert len(winners) == 15
    assert len(set(winners)) == 15  # 无重复预留
    day = book.local_day("alpha", datetime.now(timezone.utc))
    lane = audit.lane_summary("alpha", day)["high"]
    assert lane["reserved"] == 15 and lane["inflight"] == 15
    summary = audit.day_summary("alpha", day)
    assert summary["reserved"] == 15 and summary["reserved"] <= summary["limit"]
    # 重放流水：预留事件单位之和 == 计数器
    events = audit.replay("alpha", day)
    assert sum(e["units"] for e in events if e["event"] == "RESERVE") == 15
    audit.assert_healthy()


def test_concurrent_race_against_org_cap_across_lanes(tmp_path):
    db = str(tmp_path / "race2.db")
    book = policy.PolicyBook.from_mapping({
        "beta": {"timezone": "UTC", "daily_limit": 10,
                 "lanes": [{"name": "high", "weight": 1}, {"name": "low", "weight": 1}]},
    })
    seed = BudgetLedger(budget.connect(db), book)
    seed.con.close()

    outcomes = {"high_win": 0, "low_win": 0, "err": 0}
    lock = threading.Lock()

    def worker(i: int, lane: str):
        led = BudgetLedger(budget.connect(db), book)
        try:
            led.reserve("beta", lane, f"x-{lane}-{i}", 1)
            with lock:
                outcomes[f"{lane}_win"] += 1
        except (BudgetExceeded, LaneExceeded):
            with lock:
                outcomes["err"] += 1
        finally:
            led.con.close()

    threads = []
    for i in range(10):  # 每车道 10 个请求，各抢 1，车道 cap=5 但总限额=10
        threads.append(threading.Thread(target=worker, args=(i, "high")))
        threads.append(threading.Thread(target=worker, args=(i, "low")))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    audit = BudgetLedger(budget.connect(db), book)
    day = book.local_day("beta", datetime.now(timezone.utc))
    s = audit.day_summary("beta", day)
    # 两车道合计严格不超机构限额
    assert s["reserved"] == 10
    assert outcomes["high_win"] == 5 and outcomes["low_win"] == 5
    assert outcomes["err"] == 10
    audit.assert_healthy()


def test_orphan_leases_reaped_to_held_without_releasing_quota(tmp_path):
    """模拟"预留后进程退出"：租约过期 -> 回收 HELD，额度不丢不重扣。"""
    db = str(tmp_path / "orphan.db")
    book = policy.DEFAULT_POLICIES
    led = BudgetLedger(budget.connect(db), book, lease_ttl_seconds=1)
    led.reserve("alpha", "high", "orphan-1", 10, owner="dead-proc")
    day = led.get("orphan-1").day
    led.con.close()

    # 新进程打开同一账本并恢复
    led2 = BudgetLedger(budget.connect(db), book, lease_ttl_seconds=1)
    import time
    time.sleep(1.1)
    reaped = led2.reap_orphans()
    assert reaped == ["orphan-1"]
    res = led2.get("orphan-1")
    assert res.state == "HELD" and res.reason == "lease_expired"
    s = led2.day_summary("alpha", day)
    assert s["inflight"] == 10 and s["available"] == 90  # 额度仍挂账
    # 再次回收幂等，不会产生重复状态翻转
    assert led2.reap_orphans() == []
    # 对账后退款，额度才释放
    led2.refund("orphan-1", "reconciled_not_served",
                {"lookup_request_id": "orphan-1", "lookup_status": "not_found"})
    assert led2.day_summary("alpha", day)["available"] == 100
    led2.assert_healthy()
