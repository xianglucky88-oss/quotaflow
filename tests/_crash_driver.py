"""崩溃演练驱动（被 tests/test_durability.py 以子进程方式调用）。

用法:
  python -I tests/_crash_driver.py reserve DB N   # 预留 N 笔后正常退出
  python -I tests/_crash_driver.py kill DB N      # 预留 N 笔，再开事务写一半时自杀
  python -I tests/_crash_driver.py verify DB      # 打开账本做离线对账并打印结果
  python -I tests/_crash_driver.py replay DB      # 打印事件流水重放结果
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from quotaflow import budget, policy  # noqa: E402


def main() -> int:
    mode, db, count = sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 0
    if mode == "verify":
        led = budget.BudgetLedger(budget.connect(db), policy.DEFAULT_POLICIES)
        days = [r["day"] for r in led.con.execute(
            "SELECT DISTINCT day FROM reservation").fetchall()]
        print(json.dumps({"problems": led.verify(),
                          "summaries": {d: led.day_summary("alpha", d) for d in days}},
                         ensure_ascii=False))
        return 0
    if mode == "replay":
        led = budget.BudgetLedger(budget.connect(db), policy.DEFAULT_POLICIES)
        events = led.replay()
        print(json.dumps([{"event": e["event"], "units": e["units"],
                           "request_id": e["request_id"]} for e in events],
                         ensure_ascii=False))
        return 0

    led = budget.BudgetLedger(budget.connect(db), policy.DEFAULT_POLICIES,
                              lease_ttl_seconds=3600)
    for i in range(count):
        led.reserve("alpha", "high", f"crash-{os.getpid()}-{i:03d}", 1,
                    owner=f"driver-{os.getpid()}")
    if mode == "reserve":
        return 0

    if mode == "kill":
        # 手动构造一次"半写入"：BEGIN IMMEDIATE 后写计数器/预留行/部分流水，
        # 不 commit 直接 SIGKILL，模拟进程在事务中途被杀死（含断电语义）。
        con = led.con
        con.execute("BEGIN IMMEDIATE")
        con.execute("UPDATE budget_account SET reserved_total=reserved_total+1,"
                    " inflight_total=inflight_total+1 WHERE org='alpha'")
        con.execute("UPDATE budget_lane SET reserved_total=reserved_total+1,"
                    " inflight_total=inflight_total+1 WHERE org='alpha' AND lane='high'")
        con.execute(
            "INSERT INTO reservation(request_id,org,lane,day,cost,state,held_units,"
            "created_at,updated_at,attempt) VALUES "
            "('torn-write','alpha','high',(SELECT day FROM reservation LIMIT 1),"
            "1,'RESERVED',1,?, ?,1)",
            (budget.utc_now().isoformat(), budget.utc_now().isoformat()),
        )
        # 故意只写半条事件流水后立即自杀，不 commit
        con.execute("INSERT INTO budget_event(ts,request_id,org,day,lane,event,units)"
                    " SELECT ?, 'torn-write', org, day, lane, 'RESERVE', 1"
                    " FROM reservation WHERE request_id='torn-write'",
                    (budget.utc_now().isoformat(),))
        sys.stdout.flush()
        os.kill(os.getpid(), 9)
        return 1  # 不可达
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
