"""测试共用夹具：从 fixtures/schedule_multi.json 构造可复现的调度场景。

注入点按“该机构第 N 次派发”给出，而派发计数保存在调度器运行时状态里，
因此检查点恢复后注入点不会漂移。
"""
import json
from pathlib import Path

from src.quotaflow import scheduler

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "schedule_multi.json"


def load_fixture():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def build(fixture=None, *, now_override=None):
    data = fixture or load_fixture()
    now = data["now"] if now_override is None else now_override
    sch = scheduler.Scheduler(
        data["sources"],
        ledger=data["budget"],
        cursors=data["cursors"],
        config=data["config"],
        now=now,
        day=data.get("day"),
    )
    return sch, data


def timeline_for(data, source):
    entries = (data.get("timeline") or {}).get(source) or []
    return {int(item["attempt"]): item for item in entries}


def play_round(sch, data, round_no, *, step_seconds=10.0):
    """跑一轮；按“机构第 N 次派发”注入失败/部分完成。"""
    batch = sch.next_batch(1)
    decision = next((d for d in batch if d.dispatched), None)
    entry = None
    if decision is not None:
        attempt_no = sch.runtime[decision.source].dispatched
        plan = timeline_for(data, decision.source).get(attempt_no)
        entry = {
            "round": round_no,
            "source": decision.source,
            "interval": decision.interval,
            "reason": decision.reason,
            "attempt": attempt_no,
            "outcome": (plan or {}).get("outcome", "complete"),
        }
        if plan and plan.get("outcome") == "fail":
            sch.fail(decision, plan.get("detail", "injected failure"))
        elif plan and plan.get("outcome") == "partial":
            start, end = decision.interval
            cut = min(end, start + int(plan.get("advance", 1)))
            sch.record_partial(decision, cut, reason="partial")
        else:
            sch.complete(decision)
    sch.advance(step_seconds)
    return batch, decision, entry


def run_rounds(sch, data, rounds, *, start=1, step_seconds=10.0):
    out = []
    for r in range(start, start + rounds):
        batch, decision, entry = play_round(sch, data, r,
                                            step_seconds=step_seconds)
        out.append({
            "round": r,
            "source": None if decision is None else decision.source,
            "interval": None if decision is None else decision.interval,
            "reason": None if decision is None else decision.reason,
            "skips": [(d.source, d.reason) for d in batch if not d.dispatched],
            "stop": sch.stop_reason,
            "entry": entry,
        })
    return out


def summary(sch):
    return {
        "spent": {k: int(v) for k, v in sch.ledger.spent.items()},
        "used": sch.requests_used(),
        "coverage": {k: (v["committed"], v["pending"], v["dispatched"])
                     for k, v in sch.coverage().items()},
    }
