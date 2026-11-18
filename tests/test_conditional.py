"""conditional 缓存比较测试：ETag（强/弱）、updated_at、错误响应隔离。"""
from quotaflow import conditional, normalize


def _rec(etag=None, updated_at=None, ts=None):
    return {"id": "r-1", "src": "alpha", "etag": etag,
            "updated_at": updated_at, "ts": ts}


def test_first_fetch_when_no_cache():
    v = conditional.compare_record(_rec(etag="e1"), None)
    assert v["decision"] == conditional.DECISION_FETCH
    assert v["reason"] == "no-cache"


def test_strong_etag_equal_means_unchanged():
    v = conditional.compare_record(_rec(etag="e1"), _rec(etag="e1"))
    assert v["decision"] == conditional.DECISION_UNCHANGED


def test_weak_etag_same_body_is_unchanged():
    v = conditional.compare_record(_rec(etag="W/e1"), _rec(etag="e1"))
    assert v["decision"] == conditional.DECISION_UNCHANGED
    assert v["reason"] == "etag-equal-weak"


def test_etag_difference_is_changed():
    v = conditional.compare_record(_rec(etag="e2"), _rec(etag="e1"))
    assert v["decision"] == conditional.DECISION_CHANGED


def test_updated_at_comparison_when_no_etag():
    old = _rec(updated_at="2026-11-01T00:00:00Z")
    same = _rec(updated_at="2026-11-01T00:00:00Z")
    new = _rec(updated_at="2026-11-02T00:00:00Z")
    assert conditional.compare_record(same, old)["decision"] == conditional.DECISION_UNCHANGED
    assert conditional.compare_record(new, old)["decision"] == conditional.DECISION_CHANGED


def test_fallback_to_ts_when_updated_at_missing():
    old = _rec(ts="2026-11-01T00:00:00Z")
    new = _rec(ts="2026-11-01T00:00:00Z")
    assert conditional.compare_record(new, old)["decision"] == conditional.DECISION_UNCHANGED


def test_no_validator_conservatively_fetches():
    v = conditional.compare_record(_rec(), _rec())
    assert v["decision"] == conditional.DECISION_FETCH
    assert v["reason"] == "no-comparable-validator"


def test_error_response_is_quarantine_decision(temp_db):
    resp = normalize.normalize_response(
        {"error": {"code": "x"}}, source="beta")
    v = conditional.evaluate_response(resp)
    assert v["decision"] == conditional.DECISION_QUARANTINE
    assert "error_packet" in v["quarantined"]


def test_response_etag_equal_is_unchanged(temp_db):
    resp = normalize.normalize_response(
        {"schema": "v2", "data": {"records": []},
         "paging": {"page": 0, "cursor": None, "has_next": False, "limit": 1}},
        source="gamma")
    # gamma 信封无 etag：构造一个带 etag 的空响应验证响应级比较
    resp.etag = "page-etag"
    assert conditional.evaluate_response(resp, "page-etag")["decision"] \
        == conditional.DECISION_UNCHANGED
    assert conditional.evaluate_response(resp, "other")["decision"] \
        == conditional.DECISION_FETCH


def test_process_attaches_verdict_without_mutating_fields():
    rec = _rec(etag="e1")
    out = conditional.process(rec, _rec(etag="e1"))
    assert out["_conditional"]["decision"] == conditional.DECISION_UNCHANGED
    assert rec.get("_conditional") is None  # 原记录不被修改
