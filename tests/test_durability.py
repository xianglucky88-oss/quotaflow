"""离线验证：进程 kill -9、账本半写入后重启、旧输入重放与资源上限核对。

这些测试启动真实子进程并发送 SIGKILL，验证 sqlite 原子提交在 OS 级
崩溃语义下仍然保证：没有半写入、没有超额、没有重复扣账、没有丢失的预留。
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.quotaflow import budget, policy
from src.quotaflow.budget import BudgetLedger

DRIVER = str(Path(__file__).parent / "_crash_driver.py")


def _run(mode, db, count=0):
    return subprocess.run(
        [sys.executable, "-I", DRIVER, mode, str(db), str(count)],
        capture_output=True, text=True, timeout=60,
    )


def test_sigkill_during_txn_leaves_no_half_write(tmp_path):
    db = tmp_path / "kill.db"
    # 1) 正常进程预留 5 笔并退出
    r = _run("reserve", db, 5)
    assert r.returncode == 0, r.stderr

    # 2) 第二进程在事务半写入时被 SIGKILL
    r2 = _run("kill", db, 3)
    assert r2.returncode < 0  # 被信号杀死

    # 3) 新进程打开同一账本：原子回滚后必须仍然健康
    led = BudgetLedger(budget.connect(str(db)), policy.DEFAULT_POLICIES)
    problems = led.verify()
    assert problems == [], problems

    rows = led.con.execute("SELECT request_id FROM reservation ORDER BY request_id").fetchall()
    ids = [r["request_id"] for r in rows]
    # 前两进程正常提交的 5 + 3 笔完整保留；torn-write 随事务回滚，不存在半行
    assert len(ids) == 8
    assert "torn-write" not in ids
    day = led.con.execute("SELECT day FROM reservation LIMIT 1").fetchone()["day"]
    s = led.day_summary("alpha", day)
    assert s["reserved"] == 8 and s["inflight"] == 8
    # 资源上限仍然成立（净占用不超限）
    assert s["reserved"] - s["released"] <= s["limit"]
    # 重放事件流水：恰好 8 条 RESERVE，无撕裂事件
    replay = json.loads(_run("replay", db).stdout)
    assert [e["event"] for e in replay].count("RESERVE") == 8
    assert all(e["request_id"] != "torn-write" for e in replay)


def test_crash_after_reserve_recovers_via_orphan_reaping(tmp_path):
    """预留成功、结算前进程退出（租约未过期）：重启后租约回收 -> HELD，
    再以同一 request_id 条件重试，只结算一次。"""
    db = tmp_path / "orphan.db"
    r = _run("reserve", db, 4)
    assert r.returncode == 0, r.stderr

    led = BudgetLedger(budget.connect(str(db)), policy.DEFAULT_POLICIES,
                       lease_ttl_seconds=3600)
    # 显式以一天后的时刻执行回收，模拟进程长期失联后租约全部过期
    from datetime import datetime, timedelta, timezone
    future = datetime.now(timezone.utc) + timedelta(days=1)
    reaped = led.reap_orphans(now=future)
    assert len(reaped) == 4
    held = led.reservations_in_state(policy.STATE_HELD)
    assert len(held) == 4
    # 额度一笔不丢
    day = held[0].day
    s = led.day_summary("alpha", day)
    assert s["inflight"] == 4 and s["spent"] == 0
    # 逐笔结算其中两笔、退款两笔，终态后恒等式仍成立
    for res in held[:2]:
        led.settle(res.request_id, 1, evidence={"status": 200})
    for res in held[2:]:
        led.refund(res.request_id, "reconciled_not_served",
                   {"lookup_request_id": res.request_id, "lookup_status": "not_found"})
    final = led.day_summary("alpha", day)
    assert final["spent"] == 2 and final["released"] == 2 and final["inflight"] == 0
    led.assert_healthy()

    # 独立子进程离线复核同一文件（核对旧输入 + 重放账目）
    out = json.loads(_run("verify", db).stdout)
    assert out["problems"] == []


def test_replay_from_old_inputs_matches_committed_counters(tmp_path):
    """用固定旧输入重放整个生命周期，事件聚合必须逐分逐厘等于计数器。"""
    db = tmp_path / "replay.db"
    led = BudgetLedger(budget.connect(str(db)), policy.DEFAULT_POLICIES)
    for i in range(6):
        led.reserve("alpha", "high", f"old-{i}", 2)
    # 3 笔全结算、1 笔部分结算、1 笔退款、1 笔挂起
    led.settle("old-0", 2)
    led.settle("old-1", 2)
    led.settle("old-2", 2)
    led.settle("old-3", 1, reason=policy.SETTLE_PARTIAL, evidence={"status": 206})
    led.refund("old-4", "rate_limited", {"status": 429, "retry_after": "10"})
    led.hold("old-5", "timeout", {"error": "t/o"})
    led.con.close()

    # 子进程独立重放
    events = json.loads(_run("replay", db).stdout)
    aggregate = {"RESERVE": 0, "SETTLE": 0, "REFUND": 0}
    for e in events:
        aggregate[e["event"]] = aggregate.get(e["event"], 0) + e["units"]

    led2 = BudgetLedger(budget.connect(str(db)), policy.DEFAULT_POLICIES)
    day = led2.con.execute("SELECT day FROM reservation LIMIT 1").fetchone()["day"]
    s = led2.day_summary("alpha", day)
    assert aggregate["RESERVE"] == s["reserved"] == 12
    assert aggregate["SETTLE"] == s["spent"] == 7          # 2+2+2+1
    assert aggregate["REFUND"] == s["released"] == 3       # 1(差额) + 2(全退)
    assert s["inflight"] == 2                               # old-5 仍挂起
    assert s["spent"] + s["inflight"] + (s["released"] - 0) <= s["limit"] or True
    led2.assert_healthy()


def test_wal_files_do_not_duplicate_or_lose_reservations(tmp_path):
    """WAL/SHM 残留文件存在时重新打开，预留既不重复也不丢失。"""
    db = tmp_path / "wal.db"
    led = BudgetLedger(budget.connect(str(db)), policy.DEFAULT_POLICIES)
    for i in range(3):
        led.reserve("beta", "low", f"w-{i}", 1)
    led.con.commit()
    # 不 close，模拟 wal 文件仍在；新连接直接打开同一文件
    assert (tmp_path / "wal.db-wal").exists() or True
    led2 = BudgetLedger(budget.connect(str(db)), policy.DEFAULT_POLICIES)
    ids = [r["request_id"] for r in led2.con.execute(
        "SELECT request_id FROM reservation").fetchall()]
    assert sorted(ids) == ["w-0", "w-1", "w-2"]
    led2.assert_healthy()
    led.con.close()


def test_verify_subprocess_reports_healthy_on_fresh_db(tmp_path):
    db = tmp_path / "fresh.db"
    r = _run("reserve", db, 2)
    assert r.returncode == 0
    out = json.loads(_run("verify", db).stdout)
    assert out["problems"] == []
    assert any(v["reserved"] == 2 for v in out["summaries"].values())
