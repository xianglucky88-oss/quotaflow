import json, sys
from . import pipeline, budget, policy
from .config import DB_PATH


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        # 离线对账：打开持久化账本，重放事件流水并核对资源上限
        ledger = budget.BudgetLedger(budget.connect(DB_PATH), policy.DEFAULT_POLICIES)
        problems = ledger.verify()
        print(json.dumps(
            {"healthy": not problems, "problems": problems,
             "accounts": [
                 dict(dict(row), **{})
                 for row in ledger.con.execute(
                     "SELECT org,day,limit_units,reserved_total,released_total,"
                     "spent_total,inflight_total FROM budget_account ORDER BY org,day"
                 ).fetchall()
             ]},
            ensure_ascii=False, indent=2,
        ))
        return 0 if not problems else 1
    print(json.dumps(pipeline.run(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
