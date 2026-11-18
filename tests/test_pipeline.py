from quotaflow import pipeline, store


def test_pipeline_runs_and_idempotent(temp_db):
    a = pipeline.run()
    b = pipeline.run()   # 重跑不增行（幂等 upsert）
    assert a["fed"] == 4
    assert b["rows"] == a["rows"] == 3   # r-003 重复被去重


def test_store_roundtrip(temp_db):
    con = store.connect()
    try:
        assert store.count(con) == 0  # 临时库初始为空
    finally:
        con.close()
