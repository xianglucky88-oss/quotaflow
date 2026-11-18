"""四个业务模块在流水线外协同时的集成行为。

旧 happy-path 的 ``process(record)`` 必须保持可用（流水线仍按原顺序调用）；
新的调度 API 在流水线之外组合使用，不改动既有摄取路径。
"""
import json

from src.quotaflow import budget, conditional, incremental, pipeline, scheduler


def test_stage_process_functions_stay_pure_and_compatible():
    rec = {"id": "r-001", "src": " Alpha ", "name": " x ", "value": "10"}
    for module in (budget, incremental, conditional, scheduler):
        out = module.process(dict(rec))
        assert out is not rec
        assert out["id"] == "r-001"

    assert budget.process(rec)["_stage_budget"] == "ok"
    assert incremental.process(rec)["_stage_incremental"] == "ok"
    assert conditional.process(rec)["_stage_conditional"] == "ok"
    assert scheduler.process(rec)["_stage_scheduler"] == "naive"


def test_pipeline_still_runs_with_all_modules_importable():
    result = pipeline.run()
    assert result["fed"] == 4
    assert result["rows"] >= 1


def test_end_to_end_schedule_then_persist(tmp_path):
    """账本 → 游标 → 缓存 → 调度 → 检查点，全链路可序列化。"""
    now = 1_000_000.0
    ledger = budget.Ledger(limit=4, allocations={"alpha": 2})
    cursors = incremental.CursorStore({
        "alpha": incremental.Cursor(source="alpha", committed=0, head=10),
        "beta": incremental.Cursor(source="beta", committed=0, head=10),
    })
    cache = conditional.CacheStore()
    cache.put("beta", (0, 5), etag='"etag"')

    sch = scheduler.Scheduler(
        [{"name": "alpha", "priority": 5, "batch_limit": 5},
         {"name": "beta", "priority": 1, "batch_limit": 5}],
        ledger=ledger, cursors=cursors, cache=cache, now=now)

    for _ in range(6):
        d = sch.next_task()
        if d is None:
            break
        sch.complete(d)
        sch.advance(1)

    path = tmp_path / "checkpoint.json"
    sch.save(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ledger"]["limit"] == 4
    assert payload["cursors"]["alpha"]["committed"] >= 0
    assert payload["cache"]

    restored = scheduler.Scheduler.load(path)
    assert restored.ledger.to_dict() == sch.ledger.to_dict()
    assert restored.requests_used() == sch.requests_used()
    assert restored.coverage() == sch.coverage()
    # beta 的 [0,5) 有 ETag 但不等于完整命中，仍会发请求
    beta_rows = [d for d in sch.history if d.source == "beta" and d.dispatched]
    assert beta_rows and beta_rows[0].reason in {
        scheduler.DISPATCH_ADVANCE, scheduler.DISPATCH_CACHE}
