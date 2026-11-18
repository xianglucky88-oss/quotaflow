"""条件请求缓存：命中预期与失效。"""
from src.quotaflow import conditional


def test_no_cache_is_a_miss():
    cache = conditional.CacheStore()
    exp = conditional.expectation(cache, "a", (0, 5))
    assert exp.kind == "miss"
    assert exp.expected_hit is False
    assert exp.hit_probability == 0.0
    assert conditional.should_fetch(cache, "a", (0, 5)) is True


def test_full_content_cache_hit_saves_request():
    cache = conditional.CacheStore()
    cache.put("a", (0, 5), body={"rows": [1, 2, 3]})
    exp = conditional.expectation(cache, "a", (0, 5))
    assert exp.kind == "full"
    assert exp.saves_request is True
    assert exp.cost_factor == 0.0
    assert conditional.should_fetch(cache, "a", (0, 5)) is False
    # 换一个区间就不再命中
    assert conditional.expectation(cache, "a", (5, 10)).kind == "miss"


def test_validator_enables_revalidation():
    cache = conditional.CacheStore()
    entry = cache.put("a", (0, 5), etag='"abc"', last_modified="Wed, 01 Jan 2026")
    assert entry.to_headers() == {
        "If-None-Match": '"abc"',
        "If-Modified-Since": "Wed, 01 Jan 2026",
    }
    exp = conditional.expectation(cache, "a", (0, 5))
    assert exp.kind == "revalidate"
    assert exp.has_validator is True
    assert 0 < exp.cost_factor < 1
    # revalidate 仍需发请求，只是响应体可省
    assert conditional.should_fetch(cache, "a", (0, 5)) is True


def test_force_bypasses_cache():
    cache = conditional.CacheStore()
    cache.put("a", (0, 5), body={"x": 1})
    assert conditional.should_fetch(cache, "a", (0, 5), force=True) is True


def test_invalidate_and_evict():
    cache = conditional.CacheStore()
    cache.put("a", (0, 5), body={"x": 1})
    cache.put("a", (5, 10), body={"x": 2})
    cache.put("b", (0, 5), body={"x": 3})
    assert cache.evict_source("a") == 2
    assert cache.sources() == ["b"]
    assert cache.invalidate("b", (0, 5)) is True
    assert cache.invalidate("b", (0, 5)) is False


def test_request_key_is_stable_and_source_scoped():
    assert conditional.request_key("a", (0, 5)) == conditional.request_key("a", (0, 5))
    assert conditional.request_key("a", (0, 5)) != conditional.request_key("b", (0, 5))
    assert conditional.request_key("a", (0, 5), {"q": 1}) != \
        conditional.request_key("a", (0, 5), {"q": 2})


def test_cache_roundtrip():
    cache = conditional.CacheStore()
    cache.put("a", (0, 5), etag='"e"', body={"x": 1}, stored_at=3.0)
    data = cache.to_dict()
    restored = conditional.CacheStore.from_dict(data)
    assert restored.to_dict() == data
    assert restored.get("a", (0, 5)).etag == '"e"'


def test_record_hit_counts():
    cache = conditional.CacheStore()
    cache.put("a", (0, 5), etag='"e"')
    cache.record_hit("a", (0, 5), revalidated=True)
    entry = cache.get("a", (0, 5))
    assert entry.hits == 1
    assert entry.validators_used == 1


def test_happy_path_process_unchanged():
    rec = {"id": "r-1"}
    out = conditional.process(rec)
    assert out["_stage_conditional"] == "ok"
    assert rec == {"id": "r-1"}
