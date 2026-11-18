"""normalize 语义测试：多版本响应、时区、乱序页、缺失字段、错误包、空值、删除标记。"""
import json

import pytest

from quotaflow import keyer, loader, normalize, rawarchive
from quotaflow.models import NormalizedResponse


# --------------------------------------------------------------------------
# 时间戳 / ETag 归一
# --------------------------------------------------------------------------
def test_timestamp_normalizes_timezones_to_utc():
    assert normalize.normalize_timestamp("2026-11-01T08:00:00+08:00") == "2026-11-01T00:00:00Z"
    assert normalize.normalize_timestamp("2026-11-04T09:15:00+05:30") == "2026-11-04T03:45:00Z"
    assert normalize.normalize_timestamp("2026-11-02T00:30:00Z") == "2026-11-02T00:30:00Z"
    # naive 时间戳按方言默认偏移解释
    assert normalize.normalize_timestamp(
        "2026-11-01T08:00:00", default_tz_offset="+08:00") == "2026-11-01T00:00:00Z"
    # 无偏移信息按 UTC
    assert normalize.normalize_timestamp("2026-11-01T00:00:00") == "2026-11-01T00:00:00Z"
    # epoch 秒 / 毫秒
    assert normalize.normalize_timestamp(1893456000) == "2030-01-01T00:00:00Z"
    assert normalize.normalize_timestamp(1893456000000) == "2030-01-01T00:00:00Z"
    # 空值与不可解析
    assert normalize.normalize_timestamp(None) is None
    assert normalize.normalize_timestamp("") is None
    assert normalize.normalize_timestamp("not-a-date") is None


def test_etag_normalization_keeps_weak_marker():
    assert normalize.normalize_etag('"a-etag-1"') == "a-etag-1"
    assert normalize.normalize_etag('W/"x-2"') == "W/x-2"
    assert normalize.normalize_etag("  plain  ") == "plain"
    assert normalize.normalize_etag("") is None
    assert normalize.normalize_etag(None) is None


# --------------------------------------------------------------------------
# 错误包 / 空结果 / 不可解析 / schema 阻断：永远不是资源记录
# --------------------------------------------------------------------------
def test_error_packet_is_quarantined_not_a_resource(temp_db):
    payload = {"error": {"code": "rate_limited", "message": "too many requests"}}
    out = normalize.normalize_response(payload, source="beta")
    assert isinstance(out, NormalizedResponse)
    assert out.status == normalize.RESP_ERROR
    assert out.records == []
    assert out.quarantined[0].kind == normalize.QUARANTINE_ERROR_PACKET
    assert out.cursor is None  # 错误响应无游标，增量不得推进


def test_error_packet_variants(temp_db):
    for payload in [
        {"error_code": "E500", "msg": "boom"},
        {"success": False, "items": []},
        {"status": 503},
        {"status": "failed"},
    ]:
        out = normalize.normalize_response(payload, source="beta")
        assert out.status == normalize.RESP_ERROR
        assert out.quarantined[0].kind == normalize.QUARANTINE_ERROR_PACKET


def test_unparseable_payload(temp_db):
    out = normalize.normalize_response("not-a-json-object-at-all", source="zeta")
    assert out.status == normalize.RESP_ERROR
    assert out.quarantined[0].kind == normalize.QUARANTINE_UNPARSEABLE


def test_unknown_envelope_is_blocked(temp_db):
    out = normalize.normalize_response({"unexpected": {"nested": 1}}, source="omega")
    assert out.status == normalize.RESP_ERROR
    assert out.quarantined[0].kind == normalize.QUARANTINE_UNKNOWN_ENVELOPE


def test_unknown_schema_major_is_blocked_but_known_minor_passes(temp_db):
    blocked = normalize.normalize_response(
        {"schema": "v9.1", "data": {"records": [{"id": "e-1"}]}}, source="epsilon")
    assert blocked.status == normalize.RESP_ERROR
    assert blocked.quarantined[0].kind == normalize.QUARANTINE_SCHEMA_BLOCKED
    assert blocked.schema_version == "v9.1"

    legacy = normalize.normalize_response(
        {"schema": "v1.7", "data": {"records": [
            {"id": "a-legacy-1", "name": "Legacy", "value": 1,
             "status": "active", "ts": "2026-10-30T18:30:00+05:30"}]}},
        source="alpha")
    assert legacy.status == normalize.RESP_OK
    assert legacy.schema_version == "v1.7"
    assert legacy.records[0]["ts"] == "2026-10-30T13:00:00Z"


def test_empty_result_is_empty_status_with_advancing_cursor(temp_db):
    out = normalize.normalize_response(
        {"schema_ver": "v2", "result": {"data": []},
         "paging": {"page": 0, "cursor": None, "has_next": False, "limit": 50}},
        source="gamma")
    assert out.status == normalize.RESP_EMPTY
    assert out.records == []
    assert out.quarantined == []
    assert out.cursor.has_more is False and out.cursor.next_cursor is None


# --------------------------------------------------------------------------
# 字段缺失 / 未知字段 / 未知状态 / 合成标识
# --------------------------------------------------------------------------
def test_missing_optional_fields_are_lenient_with_notes(temp_db):
    out = normalize.normalize_response(
        {"schema": "v2", "data": {"records": [
            {"id": "a-101", "name": "Gadget", "value": None,
             "status": "enabled", "ts": "2026-11-02T00:30:00Z"}]}},
        source="alpha")
    rec = out.records[0]
    assert rec["value"] == 0
    assert rec["status"] == normalize.ST_ACTIVE  # enabled -> active
    assert any("宽松策略补 0" in n for n in rec["_provenance"]["notes"])


def test_unknown_fields_are_preserved_not_dropped(temp_db):
    out = normalize.normalize_response(
        {"schema": "v2", "data": {"records": [
            {"id": "a-100", "name": "Widget", "value": 1, "status": "active",
             "ts": "2026-11-01T00:00:00Z", "color": "azure", "region": "cn-east"}]}},
        source="alpha")
    extra = out.records[0]["_provenance"]["extra_fields"]
    assert extra == {"color": "azure", "region": "cn-east"}


def test_missing_identity_gets_stable_synthetic_key(temp_db):
    payload = {"schema": "v2", "data": {"records": [
        {"name": "Nameless Part", "value": 7, "status": "active",
         "ts": "2026-11-03T12:00:00Z", "lot": "L-77"}]}}
    first = normalize.normalize_response(payload, source="alpha")
    second = normalize.normalize_response(payload, source="alpha")
    r1, r2 = first.records[0], second.records[0]
    assert r1["id"].startswith("synth:")
    assert r1["id"] == r2["id"]  # 重复同步合成标识稳定
    assert r1["_provenance"]["synthetic_id"] is True
    assert keyer.key_of(r1) == keyer.key_of(r2)


def test_unknown_status_quarantines_single_record_only(temp_db):
    out = normalize.normalize_response(
        {"schema": "v2", "data": {"records": [
            {"id": "ok-1", "name": "OK", "value": 1, "status": "active",
             "ts": "2026-11-01T00:00:00Z"},
            {"id": "bad-1", "name": "Bad", "value": 2, "status": "weird-state",
             "ts": "2026-11-02T00:00:00Z"}]}}, source="alpha")
    assert [r["id"] for r in out.records] == ["ok-1"]
    assert out.quarantined[0].kind == normalize.QUARANTINE_MALFORMED_RECORD
    assert out.quarantined[0].raw_ref["raw_id"] == "bad-1"


def test_tombstone_is_syncable_resource_state(temp_db):
    out = normalize.normalize_response(
        {"version": "v2", "entries": [
            {"resource_id": "d-301", "display_name": "Gone", "metric": 0,
             "state": "inactive", "observed_at": "2026-11-08T07:00:00Z",
             "tombstoned": True}]}, source="delta")
    rec = out.records[0]
    assert rec["deleted"] is True
    assert rec["status"] == normalize.ST_INACTIVE
    assert out.status == normalize.RESP_OK  # 删除标记不是错误包


def test_invalid_value_becomes_null_with_note(temp_db):
    rec = normalize.normalize_record(
        {"id": "d-302", "display_name": "X", "metric": "oops",
         "state": "disabled", "observed_at": "2026-11-08T08:00:00Z"},
        source="delta")
    assert rec["value"] is None
    assert rec["status"] == normalize.ST_INACTIVE
    assert any("不可解析" in n for n in rec["_provenance"]["notes"])


# --------------------------------------------------------------------------
# 来源字段映射 / 原文归档 / 稳定键
# --------------------------------------------------------------------------
def test_provenance_field_map_and_raw_digest(temp_db):
    out = normalize.normalize_response(
        {"ver": "v2", "payload": {"items": [
            {"rid": "b-200", "title": "Beta", "amount": "42", "state": "closed",
             "changed_at": "2026-11-06 11:30:00", "record_etag": "W/b-rec-200-v3"}]}},
        source="beta")
    rec = out.records[0]
    fmap = rec["_provenance"]["field_map"]
    assert fmap["id"] == "rid" and fmap["name"] == "title" and fmap["value"] == "amount"
    assert fmap["etag"] == "record_etag"
    assert rec["etag"] == "W/b-rec-200-v3"
    # 原文按摘要可回查
    fetched = rawarchive.fetch(rec["_provenance"]["raw_digest"])
    assert fetched["payload"]["items"][0]["rid"] == "b-200"


def test_stable_keys_are_source_namespaced(temp_db):
    a = normalize.normalize_record({"id": "x-1", "src": "alpha"})
    b = normalize.normalize_record(
        {"rid": "x-1", "title": "Beta", "amount": 1, "state": "active",
         "changed_at": "2026-11-01T00:00:00Z"}, source="beta")
    assert keyer.key_of(a) == "alpha:x-1"
    assert keyer.key_of(b) == "beta:x-1"
    assert keyer.key_of(a) != keyer.key_of(b)


def test_legacy_process_preserves_old_record_shape(temp_db):
    raw = {"id": "r-001", "src": " Alpha ", "name": " Record One ",
           "value": 10, "status": "active", "ts": "2026-11-01T00:00:00Z"}
    rec = normalize.process(raw)
    assert rec["id"] == "r-001"
    assert rec["src"] == "alpha"
    assert rec["name"] == "Record One"
    assert rec["value"] == 10 and rec["status"] == "active"
    assert rec["ts"] == "2026-11-01T00:00:00Z"


# --------------------------------------------------------------------------
# 批量：乱序页重排、缺口检测、重复同步幂等
# --------------------------------------------------------------------------
def test_batch_reorders_out_of_order_pages(temp_db):
    pages = [
        ("svc", {"schema": "v2", "data": {"records": [
            {"id": "p2", "name": "two", "value": 2, "status": "active",
             "ts": "2026-11-02T00:00:00Z"}]},
            "page": {"index": 1, "next_cursor": None, "has_more": False, "size": 1}}),
        ("svc", {"schema": "v2", "data": {"records": [
            {"id": "p0", "name": "zero", "value": 0, "status": "active",
             "ts": "2026-11-01T00:00:00Z"}]},
            "page": {"index": 0, "next_cursor": "tok1", "has_more": True, "size": 1}}),
    ]
    batch = normalize.normalize_batch(pages)
    indexes = [r.cursor.page_index for r in batch.responses]
    assert indexes == sorted(indexes) == [0, 1]
    assert [r.records[0]["id"] for r in batch.responses] == ["p0", "p2"]


def test_batch_detects_page_gaps(temp_db):
    pages = [
        ("svc", {"schema": "v2", "data": {"records": []},
                 "page": {"index": 0, "next_cursor": "tok1", "has_more": True, "size": 0}}),
        ("svc", {"schema": "v2", "data": {"records": []},
                 "page": {"index": 2, "next_cursor": None, "has_more": False, "size": 0}}),
    ]
    batch = normalize.normalize_batch(pages)
    assert batch.order_gaps == [1]


def test_batch_is_idempotent_on_repeat_sync(temp_db):
    raw = loader.load_responses()
    first = normalize.normalize_batch(raw)
    second = normalize.normalize_batch(raw)
    assert first.as_dict() == second.as_dict()
    # 错误包计数与可同步记录数稳定
    total_q = sum(len(r.quarantined) for r in first.responses)
    assert total_q == sum(len(r.quarantined) for r in second.responses)
    assert first.syncable_records == second.syncable_records


def test_generic_dialect_auto_detects_envelope(temp_db):
    # 未登记机构：自动探测常见容器键与默认字段名
    out = normalize.normalize_response(
        {"items": [{"id": "g-1", "name": "Generic", "value": 5,
                    "status": "active", "ts": "2026-11-01T00:00:00Z"}],
         "etag": "g-etag"}, source="unknown-co")
    assert out.status == normalize.RESP_OK
    assert out.records[0]["src"] == "unknown-co"
    assert out.records[0]["id"] == "g-1"
    assert out.etag == "g-etag"


def test_bare_list_payload_is_supported(temp_db):
    out = normalize.normalize_response(
        [{"id": "l-1", "name": "List", "value": 1, "status": "active",
          "ts": "2026-11-01T00:00:00Z"}], source="alpha")
    assert out.status == normalize.RESP_OK
    assert out.cursor.page_index == 0 and out.cursor.page_size == 1
    assert out.records[0]["id"] == "l-1"


def test_non_dict_record_in_container_is_quarantined(temp_db):
    out = normalize.normalize_response(
        {"schema": "v2", "data": {"records": ["not-an-object"]}}, source="alpha")
    assert out.status == normalize.RESP_ERROR
    assert out.quarantined[0].kind == normalize.QUARANTINE_MALFORMED_RECORD
    assert out.quarantined[0].raw_ref["position"] == 0


def test_full_fixture_classification_counts(temp_db):
    batch = normalize.normalize_batch(loader.load_responses())
    statuses = sorted(r.status for r in batch.responses)
    assert statuses.count("error") == 3   # beta 错误包 / epsilon schema 阻断 / zeta 不可解析
    assert statuses.count("empty") == 2   # beta 空页 / gamma 空结果
    # 11 条可同步记录（含 1 条 tombstone、1 条合成标识，重复 a-102 不去重也不丢）
    assert batch.syncable_records == 11
    kinds = sorted(q.kind for q in batch.quarantined)
    assert normalize.QUARANTINE_ERROR_PACKET in kinds
    assert normalize.QUARANTINE_SCHEMA_BLOCKED in kinds
    assert normalize.QUARANTINE_UNPARSEABLE in kinds
    assert normalize.QUARANTINE_MALFORMED_RECORD in kinds
