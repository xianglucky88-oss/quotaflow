"""incremental 游标语义测试：错误不推进、缺口拒绝、末页完成、中断恢复与重复同步。"""
import pytest

from quotaflow import incremental, normalize, store
from quotaflow.pipeline import InterruptedSync, sync_responses


def _page(source, index, *, next_cursor="tok", has_more=True, records=None,
          status_override=None, con=None):
    payload = {
        "schema": "v2",
        "data": {"records": records if records is not None else [
            {"id": f"{source}-{index}", "name": "n", "value": index,
             "status": "active", "ts": "2026-11-01T00:00:00Z"}]},
        "page": {"index": index, "next_cursor": next_cursor,
                 "has_more": has_more, "size": 1},
    }
    resp = normalize.normalize_response(payload, source=source, con=con)
    if status_override:
        resp.status = status_override
    return resp


def _error_page(source, con):
    return normalize.normalize_response(
        {"error": {"code": "rate_limited"}}, source=source, con=con)


def test_initial_state_starts_before_first_page(temp_db):
    con = store.connect()
    try:
        st = incremental.get_state(con, "alpha")
        assert st["last_page_index"] == -1
        assert st["last_cursor"] is None
        assert st["finished"] is False
        assert incremental.next_cursor(con, "alpha") is None
    finally:
        con.close()


def test_error_response_does_not_advance_cursor(temp_db):
    con = store.connect()
    try:
        move = incremental.advance(con, _error_page("alpha", con))
        assert move["advanced"] is False
        assert incremental.get_state(con, "alpha")["last_page_index"] == -1
    finally:
        con.close()


def test_pages_advance_contiguously_and_finish(temp_db):
    con = store.connect()
    try:
        m0 = incremental.advance(con, _page("alpha", 0, next_cursor="c1", con=con))
        m1 = incremental.advance(
            con, _page("alpha", 1, next_cursor=None, has_more=False, con=con))
        assert m0["advanced"] and m1["advanced"]
        st = incremental.get_state(con, "alpha")
        assert st["last_page_index"] == 1
        assert st["last_cursor"] is None
        assert st["finished"] is True
    finally:
        con.close()


def test_out_of_order_page_refused_until_gap_filled(temp_db):
    con = store.connect()
    try:
        incremental.advance(con, _page("alpha", 0, next_cursor="c1", con=con))
        jump = incremental.advance(con, _page("alpha", 2, con=con))
        assert jump["advanced"] is False
        assert "page-gap" in jump["reason"]
        # 位置仍停在 0，游标不被乱序页污染
        assert incremental.get_state(con, "alpha")["last_page_index"] == 0
        # 补齐缺口页 1 后才能继续
        filled = incremental.advance(con, _page("alpha", 1, next_cursor="c2", con=con))
        assert filled["advanced"] is True
        assert incremental.get_state(con, "alpha")["last_page_index"] == 1
    finally:
        con.close()


def test_already_applied_page_is_idempotent(temp_db):
    con = store.connect()
    try:
        incremental.advance(con, _page("alpha", 0, next_cursor="c1", con=con))
        again = incremental.advance(con, _page("alpha", 0, next_cursor="c1", con=con))
        assert again["advanced"] is False
        assert again["reason"] == "already-applied-page"
        assert incremental.get_state(con, "alpha")["updated_count"] == 1
    finally:
        con.close()


def test_reset_returns_cursor_to_origin(temp_db):
    con = store.connect()
    try:
        incremental.advance(con, _page("alpha", 0, con=con))
        incremental.reset(con, "alpha")
        assert incremental.get_state(con, "alpha")["last_page_index"] == -1
    finally:
        con.close()


# --------------------------------------------------------------------------
# 端到端：重复同步稳定、中断恢复、错误包不落库
# --------------------------------------------------------------------------
def test_full_sync_then_resync_is_stable(temp_db):
    first = sync_responses()
    second = sync_responses()
    # 第二轮：所有已存在记录走条件比较（fixture 原文不变 → ETag/updated_at 不变）
    assert first["synced"] >= second["synced"]
    assert second["rows"] == first["rows"]
    assert second["quarantined"] == first["quarantined"]
    assert second["error_pages"] == 3 and second["empty_pages"] == 2

    con = store.connect()
    try:
        # 错误包机构（beta 第 0 页）没有任何游标状态，epsilon/zeta 同样无状态
        assert incremental.get_state(con, "epsilon")["last_page_index"] == -1
        # delta 只有一页且 has_more=true，游标保留给下轮
        assert incremental.get_state(con, "delta")["last_cursor"] == "delta-page1"
        # gamma 空结果也是有效页：游标推进并结束
        assert incremental.get_state(con, "gamma")["finished"] is True
        n_rows = store.count(con)
    finally:
        con.close()
    assert n_rows == first["rows"]


def test_interrupted_sync_resumes_without_duplicates(temp_db):
    responses = [
        ("svc", {"schema": "v2", "data": {"records": [
            {"id": "r0", "name": "zero", "value": 0, "status": "active",
             "ts": "2026-11-01T00:00:00Z"}]},
            "page": {"index": 0, "next_cursor": "c1", "has_more": True, "size": 1}}),
        ("svc", {"schema": "v2", "data": {"records": [
            {"id": "r1", "name": "one", "value": 1, "status": "active",
             "ts": "2026-11-02T00:00:00Z"}]},
            "page": {"index": 1, "next_cursor": "c2", "has_more": True, "size": 1}}),
        ("svc", {"schema": "v2", "data": {"records": [
            {"id": "r2", "name": "two", "value": 2, "status": "active",
             "ts": "2026-11-03T00:00:00Z"}]},
            "page": {"index": 2, "next_cursor": None, "has_more": False, "size": 1}}),
    ]
    con = store.connect()
    try:
        with pytest.raises(InterruptedSync) as exc:
            sync_responses(responses, con=con, stop_after_pages=1)
        partial = exc.value.partial
        assert partial["pages_processed"] == 1
        rows_after_interrupt = store.count(con)
        state = incremental.get_state(con, "svc")
        assert state["last_page_index"] == 0  # 第 0 页已确认
        assert state["last_cursor"] == "c1"

        # 恢复：重放整批，已应用页被识别为 already-applied，不产生重复行
        recovered = sync_responses(responses, con=con)
        assert store.count(con) == rows_after_interrupt + 2
        assert recovered["rows"] == 3
        final_state = incremental.get_state(con, "svc")
        assert final_state["last_page_index"] == 2
        assert final_state["finished"] is True
    finally:
        con.close()


def test_error_packets_never_become_records(temp_db):
    responses = [
        ("beta", {"error": {"code": "rate_limited", "message": "x"}}),
        ("beta", {"ver": "v2", "payload": {"items": [
            {"rid": "b-ok", "title": "OK", "amount": 1, "state": "active",
             "changed_at": "2026-11-06T11:30:00Z"}]},
            "pagination": {"page_no": 0, "next": None, "more": False, "per_page": 1}}),
    ]
    report = sync_responses(responses)
    assert report["error_pages"] == 1
    assert report["synced"] == 1
    con = store.connect()
    try:
        keys = [k for k, _ in store.all_records(con)]
        assert keys == ["beta:b-ok"]  # 错误包没有对应键
    finally:
        con.close()
