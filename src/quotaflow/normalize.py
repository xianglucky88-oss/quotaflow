"""单一职责：把多机构、多版本 API 响应规范化为稳定语义。

规范输出（见 ``models.NormalizedResponse``）包含三部分，分别供不同下游消费：
  1. 规范资源记录 ``records`` —— 供现有 keyer/store 稳定落库，以及 conditional/incremental；
  2. 来源字段映射 ``_provenance``（记录内）与隔离项 ``quarantined`` —— 可回查、可审计；
  3. 响应状态 ``status`` / 规范游标 ``cursor`` / ``etag`` / ``updated_at`` ——
     供 conditional.py 做缓存比较、incremental.py 解释游标语义。

兼容边界（对未知字段与新 schema 版本的明确策略）
--------------------------------------------------
* 未知字段：**保留**。规范记录收纳已知字段，未知字段原样收入
  ``_provenance.extra_fields``，不阻断同步。
* 已知 major 版本（当前 v1/v2）：正常处理，字段缺失按宽松策略补默认并记 notes。
* 更新的 minor/patch：默认沿用同 major 解析器处理（前向兼容，未知字段保留）。
* 更新的 major（如 v9）：**阻断**该响应并隔离（QUARANTINE_SCHEMA_BLOCKED），
  绝不猜测语义后落库。
* 错误包/不可解析信封/无法识别的信封：整页隔离，绝不当成资源记录。
* 记录缺少资源标识：可从规范字段合成稳定指纹（synth: 前缀）；
  连指纹都无法构造时隔离（QUARANTINE_MISSING_IDENTITY）。
* 空值：机构级“无资源”统一为响应状态 ``empty``；字段级 null 宽松归一并记 notes。
* 删除标记（tombstone）：是可同步的资源状态（``deleted=true``），不是错误包。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Iterable

from . import rawarchive
from .models import NormalizedBatch, NormalizedResponse, PageCursor, QuarantineItem

# ---------------------------------------------------------------------------
# 稳定语义常量（下游模块只依赖这些常量，不依赖机构原始字符串）
# ---------------------------------------------------------------------------
RESP_OK = "ok"
RESP_EMPTY = "empty"
RESP_ERROR = "error"

# 规范资源状态
ST_ACTIVE = "active"
ST_SOLD = "sold"
ST_INACTIVE = "inactive"
ST_DELETED = "deleted"
SYNCABLE_STATUSES = (ST_ACTIVE, ST_SOLD, ST_INACTIVE, ST_DELETED)

# 隔离原因：明确“哪些结果只能隔离、不可同步”
QUARANTINE_ERROR_PACKET = "error_packet"
QUARANTINE_UNPARSEABLE = "unparseable"
QUARANTINE_UNKNOWN_ENVELOPE = "unknown_envelope"
QUARANTINE_SCHEMA_BLOCKED = "schema_blocked"
QUARANTINE_MISSING_IDENTITY = "missing_identity"
QUARANTINE_MALFORMED_RECORD = "malformed_record"
QUARANTINE_REASONS = (
    QUARANTINE_ERROR_PACKET,
    QUARANTINE_UNPARSEABLE,
    QUARANTINE_UNKNOWN_ENVELOPE,
    QUARANTINE_SCHEMA_BLOCKED,
    QUARANTINE_MISSING_IDENTITY,
    QUARANTINE_MALFORMED_RECORD,
)

# 已知 schema major：超出此范围的 major 版本按兼容边界阻断
SUPPORTED_SCHEMA_MAJORS = (1, 2)
DEFAULT_SCHEMA_VERSION = "v2"

# 机构状态词 -> 规范状态
_STATUS_MAP = {
    "active": ST_ACTIVE, "enabled": ST_ACTIVE, "1": ST_ACTIVE,
    "sold": ST_SOLD, "closed": ST_SOLD,
    "inactive": ST_INACTIVE, "disabled": ST_INACTIVE, "0": ST_INACTIVE,
    "deleted": ST_DELETED, "tombstone": ST_DELETED, "removed": ST_DELETED,
}
_DELETED_MARKERS = ("true", "1", "yes", "deleted", "tombstone", "removed")


# ---------------------------------------------------------------------------
# 通用工具：时间戳 / ETag / 摘要 / 路径取值
# ---------------------------------------------------------------------------
def canonical_digest(payload: Any) -> str:
    """内容寻址摘要：同原文重复同步结果一致。"""
    return rawarchive.digest_of(payload)


def normalize_timestamp(value: Any, *, default_tz_offset: str | None = None) -> str | None:
    """把各类机构时间戳统一为 UTC ISO-8601（秒/微秒按需保留，结尾 ``Z``）。

    覆盖：带 Z、带偏移（+08:00）、naive（按 default_tz_offset，否则按 UTC）、
    仅日期、数值 epoch（秒/毫秒）、``None``/空串（返回 None）。
    无法解析时返回 None，由调用方记 note，绝不抛异常中断整页。
    """
    if value is None or value == "":
        return None
    # 数值 epoch：10 位秒、13 位毫秒、浮点秒
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value) / 1000.0 if abs(value) >= 1e12 else float(value)
        try:
            return _fmt_dt(datetime.fromtimestamp(seconds, tz=timezone.utc))
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    iso = text
    if iso.endswith(("Z", "z")):
        iso = iso[:-1] + "+00:00"
    # 空格分隔的日期时间 -> ISO 的 T 分隔
    if " " in iso and "T" not in iso:
        iso = iso.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        # 仅日期
        try:
            dt = datetime.fromisoformat(text + "T00:00:00")
        except ValueError:
            return None
    if dt.tzinfo is None:
        offset = _parse_offset(default_tz_offset)
        dt = dt.replace(tzinfo=offset)
    return _fmt_dt(dt.astimezone(timezone.utc))


def _parse_offset(offset: str | None) -> timezone:
    if not offset:
        return timezone.utc
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", offset.strip())
    if not m:
        return timezone.utc
    sign, hh, mm = m.groups()
    from datetime import timedelta
    delta = timedelta(hours=int(hh), minutes=int(mm))
    return timezone(-delta if sign == "-" else delta)


def _fmt_dt(dt: datetime) -> str:
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        base += f".{dt.microsecond:06d}"
    return base + "Z"


def normalize_etag(value: Any) -> str | None:
    """统一 ETag：空值归 None；弱验证标记 W/ 保留并 trim 空白与大小写。

    conditional 比较时再按 HTTP 语义决定弱/强，本函数只做形态归一，不丢信息。
    """
    if value is None:
        return None
    tag = str(value).strip()
    if not tag:
        return None
    weak = tag.upper().startswith("W/")
    body = tag[2:].strip() if weak else tag
    body = body.strip('"').strip()
    if not body:
        return None
    return ("W/" + body) if weak else body


def _get_path(obj: Any, path: str | tuple | None) -> Any:
    """按点路径取值（支持 ``a.b.0.c``）；任何一级缺失返回 None。"""
    if not path or not isinstance(obj, dict):
        return None
    parts = path.split(".") if isinstance(path, str) else tuple(path)
    cur = obj
    for part in parts:
        if isinstance(cur, dict):
            if part not in cur:
                return None
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return None
    return cur


def _coerce_int(raw: Any) -> tuple[int | None, str | None]:
    """值字段宽松归一：bool 不当数字；非法值返回 (None, note)。"""
    if raw is None:
        return 0, "value 缺失，按宽松策略补 0"
    if isinstance(raw, bool):
        return None, f"value 非法布尔值 {raw!r}，置 null"
    try:
        return int(raw), None
    except (TypeError, ValueError):
        return None, f"value 不可解析 {raw!r}，置 null"


# ---------------------------------------------------------------------------
# 机构方言（dialect）：每个机构一套字段映射与信封路径
# ---------------------------------------------------------------------------
def _dialect(source: str) -> dict[str, Any]:
    s = (source or "").strip().lower()
    table = {
        "alpha": {
            "record_path": "data.records",
            "id": "id", "name": "name", "value": "value", "status": "status",
            "ts": "ts", "etag": "meta.etag", "updated": "meta.updated_at",
            "page_index": "page.index", "next_cursor": "page.next_cursor",
            "has_more": "page.has_more", "page_size": "page.size",
            "schema": "schema", "default_tz": None,
        },
        "beta": {
            "record_path": "payload.items",
            "id": "rid", "name": "title", "value": "amount", "status": "state",
            "ts": "changed_at", "etag": "record_etag", "updated": "updated_at",
            "page_index": "pagination.page_no", "next_cursor": "pagination.next",
            "has_more": "pagination.more", "page_size": "pagination.per_page",
            "schema": "ver", "default_tz": None,
        },
        "gamma": {
            "record_path": "result.data",
            "id": "uuid", "name": "label", "value": "qty", "status": "lifecycle",
            "ts": "event_time", "etag": "headers.etag", "updated": "headers.last_modified",
            "page_index": "paging.page", "next_cursor": "paging.cursor",
            "has_more": "paging.has_next", "page_size": "paging.limit",
            "schema": "schema_ver", "default_tz": "+08:00",
        },
        "delta": {
            "record_path": "entries",
            "id": "resource_id", "name": "display_name", "value": "metric",
            "status": "state", "ts": "observed_at", "etag": "etag",
            "updated": "revision_time", "deleted_flag": "tombstoned",
            "page_index": "page_no", "next_cursor": "next_page_token",
            "has_more": "has_next", "page_size": "page_size",
            "schema": "version", "default_tz": None,
        },
    }
    if s in table:
        cfg = dict(table[s])
        cfg["source"] = s
        return cfg
    # 未知机构：走通用自动探测，来源名仍保留原样（小写归一）
    return _generic_dialect(s)


def _generic_dialect(source: str) -> dict[str, Any]:
    return {
        "source": source,
        "record_path": None,  # 由 _find_records 自动探测
        "id": "id", "name": "name", "value": "value", "status": "status",
        "ts": "ts", "etag": "etag", "updated": "updated_at",
        "page_index": "page.index", "next_cursor": "page.next_cursor",
        "has_more": "page.has_more", "page_size": "page.size",
        "schema": "schema", "default_tz": None,
    }


def _find_records(payload: Any) -> tuple[list | None, str]:
    """通用信封探测：裸列表 / 常见容器键，返回 (记录列表或 None, 命中路径说明)。"""
    if isinstance(payload, list):
        return payload, "<root list>"
    if isinstance(payload, dict):
        for key in ("records", "items", "entries", "data", "result"):
            cand = payload.get(key)
            if isinstance(cand, list):
                return cand, key
            if isinstance(cand, dict):
                for sub in ("records", "items", "entries", "data"):
                    if isinstance(cand.get(sub), list):
                        return cand[sub], f"{key}.{sub}"
    return None, ""


# ---------------------------------------------------------------------------
# schema 版本
# ---------------------------------------------------------------------------
def _schema_major(version: Any) -> int | None:
    if version is None:
        return None
    m = re.fullmatch(r"v?(\d+)(?:\.\d+.*)?", str(version).strip().lower())
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# 单条记录规范化（保持与旧流水线 process() 同形：dict -> dict）
# ---------------------------------------------------------------------------
def normalize_record(raw: dict[str, Any], *, source: str = "",
                     dialect: dict[str, Any] | None = None) -> dict[str, Any]:
    """把一条机构原始记录规范化。旧正常结果字段保持不变（id/src/name/value/status/ts）。"""
    dialect = dialect or _dialect(source)
    if not source and isinstance(raw, dict) and raw.get("src"):
        dialect = _dialect(str(raw["src"]))
    src = (source or dialect.get("source", "")).strip().lower()
    notes: list[str] = []
    mapping: dict[str, str] = {}

    def take(canonical: str) -> Any:
        path = dialect.get(canonical)
        val = _get_path(raw, path) if path else None
        if val is not None:
            mapping[canonical] = path
        return val

    rid = take("id")
    name = take("name")
    raw_value = take("value")
    raw_status = take("status")
    raw_ts = take("ts")
    raw_etag = take("etag")
    raw_updated = take("updated")

    name = "" if name is None else str(name).strip()
    value, value_note = _coerce_int(raw_value)
    if value_note:
        notes.append(value_note)

    deleted = False
    flag_path = dialect.get("deleted_flag")
    if flag_path:
        flag = _get_path(raw, flag_path)
        if flag is not None:
            mapping["deleted"] = flag_path
            deleted = str(flag).strip().lower() in _DELETED_MARKERS

    status = None
    if raw_status is not None:
        status = _STATUS_MAP.get(str(raw_status).strip().lower())
        mapping["status"] = dialect["status"]
        if status is None:
            notes.append(f"未知状态值 {raw_status!r}，保留原值于 provenance 并隔离该记录")
    elif deleted:
        status = ST_DELETED
        notes.append("无状态字段但带删除标记，规范为 deleted")
    else:
        notes.append("status 缺失，置 null（宽松保留，不阻断）")

    ts = normalize_timestamp(raw_ts, default_tz_offset=dialect.get("default_tz"))
    if raw_ts is not None:
        mapping["ts"] = dialect["ts"]
        if ts is None:
            notes.append(f"时间戳不可解析 {raw_ts!r}，置 null")
    updated_at = normalize_timestamp(raw_updated, default_tz_offset=dialect.get("default_tz"))
    if raw_updated is not None:
        mapping["updated_at"] = dialect["updated"]
    etag = normalize_etag(raw_etag)
    if raw_etag is not None:
        mapping["etag"] = dialect["etag"]

    known_paths = {p for p in (
        dialect.get("id"), dialect.get("name"), dialect.get("value"),
        dialect.get("status"), dialect.get("ts"), dialect.get("etag"),
        dialect.get("updated"), dialect.get("deleted_flag"),
    ) if p}
    extra = _collect_extra(raw, known_paths)

    identity = str(rid).strip() if rid is not None and str(rid).strip() else ""
    synthetic = False
    if not identity:
        fingerprint = _fingerprint(src, name, value, status, ts)
        if fingerprint is None:
            raise _QuarantineSignal(QUARANTINE_MISSING_IDENTITY,
                                    "记录缺少资源标识且无法合成稳定指纹")
        identity = "synth:" + fingerprint
        synthetic = True
        notes.append("缺少机构资源标识，已用规范字段合成稳定标识 synth:")

    rec = {
        "id": identity,
        "src": src,
        "name": name,
        "value": value,
        "status": status,
        "ts": ts,
        "etag": etag,
        "updated_at": updated_at,
        "deleted": bool(deleted or status == ST_DELETED),
        "schema_version": None,  # 由响应级填充
        "_provenance": {
            "source": src,
            "field_map": dict(sorted(mapping.items())),
            "extra_fields": extra,           # 未知字段：保留不阻断
            "raw_status": raw_status,
            "raw_id": rid if rid is not None else None,
            "synthetic_id": synthetic,
            "notes": notes,
        },
    }
    if status is not None and status not in SYNCABLE_STATUSES:  # 防御性，正常不会触发
        raise _QuarantineSignal(QUARANTINE_MALFORMED_RECORD, f"规范状态不可同步 {status!r}")
    if status is None and raw_status is not None:
        # 未知状态：隔离该记录（同页其余记录不受影响）
        raise _QuarantineSignal(
            QUARANTINE_MALFORMED_RECORD,
            f"未知状态值 {raw_status!r} 无规范映射",
        )
    return rec


class _QuarantineSignal(Exception):
    """记录级隔离信号，携带隔离原因。"""
    def __init__(self, kind: str, detail: str):
        super().__init__(detail)
        self.kind = kind
        self.detail = detail


def _collect_extra(raw: dict[str, Any], known_paths: set[str]) -> dict[str, Any]:
    """未知字段保留：只收集顶层未被方言映射引用的键（嵌套路径的根键视为已消费）。"""
    consumed_roots = {p.split(".")[0] for p in known_paths}
    return {k: v for k, v in raw.items() if k not in consumed_roots}


def _fingerprint(src: str, name: Any, value: Any, status: Any, ts: Any) -> str | None:
    basis = [src, name, value, status, ts]
    if all(b in (None, "") for b in basis[1:]):
        return None
    blob = json.dumps(basis, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


# ---------------------------------------------------------------------------
# 整页响应规范化
# ---------------------------------------------------------------------------
def normalize_response(payload: Any, *, source: str = "",
                       con=None) -> NormalizedResponse:
    """把一次机构响应（可能是 dict 信封、裸列表或错误包）规范化。

    永远不抛异常：错误形态收敛为 ``status=error`` + 隔离项，保证中断恢复时
    重复处理同一响应得到完全一致的结果。
    """
    dialect = _dialect(source)
    src = dialect["source"]
    digest = rawarchive.archive(payload, source=src, con=con)

    # 1) 不可解析 / 非对象信封
    if not isinstance(payload, (dict, list)):
        return _error_response(src, QUARANTINE_UNPARSEABLE,
                               f"响应类型 {type(payload).__name__} 无法解析",
                               digest, payload)

    # 2) 错误包识别（多机构不同形态统一在此判定，绝不进入资源路径）
    err = _detect_error_packet(payload)
    if err is not None:
        return _error_response(src, QUARANTINE_ERROR_PACKET, err, digest, payload)

    # 3) 记录容器定位
    if isinstance(payload, list):
        records_raw, container_path = payload, "<root list>"
        schema_raw = None
        cursor = _build_cursor({}, dialect=dialect,
                               fallback_index=0, fallback_size=len(payload))
        etag = updated = None
    else:
        records_raw, container_path = _locate_records(payload, dialect)
        if records_raw is None:
            return _error_response(src, QUARANTINE_UNKNOWN_ENVELOPE,
                                   "无法识别的信封结构，未找到记录容器",
                                   digest, payload)
        schema_raw = _get_path(payload, dialect.get("schema"))
        etag = normalize_etag(_get_path(payload, dialect.get("etag")))
        updated = normalize_timestamp(
            _get_path(payload, dialect.get("updated")),
            default_tz_offset=dialect.get("default_tz"))
        cursor = _build_cursor(payload, dialect=dialect,
                               fallback_index=0, fallback_size=len(records_raw))

    # 4) schema 版本兼容边界
    version = str(schema_raw).lower() if schema_raw is not None else DEFAULT_SCHEMA_VERSION
    major = _schema_major(version)
    if major is not None and major not in SUPPORTED_SCHEMA_MAJORS:
        return _error_response(
            src, QUARANTINE_SCHEMA_BLOCKED,
            f"不支持的 schema 主版本 {version}（支持 {SUPPORTED_SCHEMA_MAJORS}），按策略阻断",
            digest, payload, schema_version=version)

    # 5) 空结果：机构明确表达“无资源”
    if not records_raw:
        return NormalizedResponse(
            source=src, status=RESP_EMPTY, records=[], cursor=cursor,
            etag=etag, updated_at=updated, schema_version=version,
            quarantined=[], raw_digest=digest,
            notes=[f"空结果容器 {container_path}，游标仍可推进"])

    # 6) 逐条规范化，单条坏记录隔离而不拖垮整页
    records: list[dict[str, Any]] = []
    quarantined: list[QuarantineItem] = []
    notes: list[str] = []
    for pos, item in enumerate(records_raw):
        if not isinstance(item, dict):
            quarantined.append(QuarantineItem(
                kind=QUARANTINE_MALFORMED_RECORD,
                detail=f"容器 {container_path} 第 {pos} 项不是对象",
                source=src, raw_digest=digest, schema_version=version,
                raw_ref={"container": container_path, "position": pos}))
            continue
        try:
            rec = normalize_record(item, source=src, dialect=dialect)
        except _QuarantineSignal as sig:
            quarantined.append(QuarantineItem(
                kind=sig.kind, detail=sig.detail, source=src,
                raw_digest=digest, schema_version=version,
                raw_ref={"container": container_path, "position": pos,
                         "raw_id": item.get(dialect.get("id"))}))
            continue
        rec["schema_version"] = version
        rec["_provenance"]["container"] = container_path
        rec["_provenance"]["position"] = pos
        rec["_provenance"]["raw_digest"] = digest
        records.append(rec)

    if not records and quarantined:
        # 整页没有任何可同步记录：响应状态记 error（资源视角为空，但区别于机构空结果）
        status = RESP_ERROR
        notes.append("全部记录被隔离，无资源可同步")
    else:
        status = RESP_OK
    return NormalizedResponse(
        source=src, status=status, records=records, cursor=cursor,
        etag=etag, updated_at=updated, schema_version=version,
        quarantined=quarantined, raw_digest=digest, notes=notes)


def _detect_error_packet(payload: Any) -> str | None:
    """统一识别多机构错误包形态；返回错误描述或 None。"""
    if isinstance(payload, list):
        return None
    if not isinstance(payload, dict):
        return "响应不是 JSON 对象/数组"
    # 显式错误信封
    err_obj = payload.get("error")
    if isinstance(err_obj, dict):
        code = err_obj.get("code") or err_obj.get("type") or "error"
        msg = err_obj.get("message") or err_obj.get("msg") or ""
        return f"错误包 code={code} {msg}".strip()
    if "error_code" in payload or "err" in payload:
        return f"错误包 {payload.get('error_code') or payload.get('err')}"
    status = payload.get("status")
    if isinstance(status, str) and status.strip().lower() in ("error", "failed", "fail"):
        return f"错误包 status={status}"
    if isinstance(status, int) and status >= 400:
        return f"错误包 HTTP 风格 status={status}"
    if "success" in payload and payload["success"] is False:
        return "错误包 success=false"
    return None


def _locate_records(payload: dict, dialect: dict) -> tuple[list | None, str]:
    path = dialect.get("record_path")
    if path:
        cand = _get_path(payload, path)
        if isinstance(cand, list):
            return cand, path
        # 配置路径命中空容器也算（区别于未知信封）
        if cand is None and _path_exists(payload, path):
            return [], path
    cand, hit = _find_records(payload)
    if cand is not None:
        return cand, hit or "auto"
    return None, ""


def _path_exists(obj: dict, path: str) -> bool:
    cur: Any = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return False
    return True


def _build_cursor(envelope: dict, dialect: dict, *, fallback_index: int,
                  fallback_size: int) -> PageCursor:
    idx = _get_path(envelope, dialect.get("page_index"))
    page_index = int(idx) if isinstance(idx, int) or (isinstance(idx, str) and idx.isdigit()) \
        else fallback_index
    raw_cursor = _get_path(envelope, dialect.get("next_cursor"))
    has_more_raw = _get_path(envelope, dialect.get("has_more"))
    size = _get_path(envelope, dialect.get("page_size"))
    page_size = int(size) if isinstance(size, int) or (isinstance(size, str) and size.isdigit()) \
        else fallback_size

    if has_more_raw is None:
        has_more = raw_cursor not in (None, "", "")
    else:
        has_more = bool(has_more_raw)
    # 机构自相矛盾（has_more=false 却给 token）时以 has_more 为准，token 留档于 raw_cursor
    next_cursor = str(raw_cursor) if has_more and raw_cursor not in (None, "") else None
    return PageCursor(
        page_index=page_index, next_cursor=next_cursor, has_more=has_more,
        page_size=page_size,
        raw_cursor=str(raw_cursor) if raw_cursor not in (None, "") else None)


def _error_response(source: str, kind: str, detail: str, digest: str,
                    payload: Any, *, schema_version: str | None = None) -> NormalizedResponse:
    return NormalizedResponse(
        source=source, status=RESP_ERROR, records=[], cursor=None,
        etag=None, updated_at=None, schema_version=schema_version,
        quarantined=[QuarantineItem(
            kind=kind, detail=detail, source=source, raw_digest=digest,
            schema_version=schema_version, raw_ref={"top_level_keys": sorted(
                payload.keys()) if isinstance(payload, dict) else []})],
        raw_digest=digest, notes=[detail])


# ---------------------------------------------------------------------------
# 批量：乱序页重排、缺口检测、重复同步幂等
# ---------------------------------------------------------------------------
def normalize_batch(raw_responses: Iterable[Any] | Iterable[tuple[str, Any]], *,
                    con=None) -> NormalizedBatch:
    """规范化一批响应。

    入参元素可以是 ``(source, payload)`` 元组，或自带 ``src``/``source`` 字段的 dict。
    乱序页按 (source, page_index) 稳定重排；页序号缺口记录到 ``order_gaps``，
    中断恢复后重跑同一批，输出顺序与摘要完全一致。
    """
    normalized: list[NormalizedResponse] = []
    notes: list[str] = []
    for item in raw_responses:
        if isinstance(item, tuple) and len(item) == 2:
            source, payload = item
        elif isinstance(item, dict):
            source = item.get("src") or item.get("source") or ""
            payload = item.get("payload", item)
        else:
            source, payload = "", item
        normalized.append(normalize_response(payload, source=source, con=con))

    # 稳定排序：先机构，再页序号（error/empty 无 cursor 的排到该机构最后）
    def sort_key(r: NormalizedResponse):
        idx = r.cursor.page_index if r.cursor else 10**9
        return (r.source, idx, r.raw_digest or "")
    normalized.sort(key=sort_key)

    gaps = _detect_gaps(normalized)
    if gaps:
        notes.append(f"检测到分页缺口 {gaps}，游标推进需等待缺口页补齐")

    syncable = sum(len(r.records) for r in normalized)
    quarantined = [q for r in normalized for q in r.quarantined]
    return NormalizedBatch(
        responses=normalized, pages=len(normalized),
        syncable_records=syncable, quarantined=quarantined,
        order_gaps=gaps, notes=notes)


def _detect_gaps(responses: list[NormalizedResponse]) -> list[int]:
    by_source: dict[str, list[int]] = {}
    for r in responses:
        if r.cursor is not None:
            by_source.setdefault(r.source, []).append(r.cursor.page_index)
    gaps: list[int] = []
    for src, indexes in by_source.items():
        ordered = sorted(set(indexes))
        if not ordered:
            continue
        expected = set(range(ordered[0], ordered[-1] + 1))
        for missing in sorted(expected - set(ordered)):
            gaps.append(missing)
    return gaps


# ---------------------------------------------------------------------------
# 旧流水线入口：保持单记录 dict -> dict 形态与既有结果不变
# ---------------------------------------------------------------------------
def process(record: dict[str, Any]) -> dict[str, Any]:
    """兼容旧流水线（loader 逐记录喂入）的薄封装。

    旧 fixtures 是裸记录而非机构响应信封，这里走记录级规范化并补齐旧字段形态；
    不产生隔离项（旧路径没有响应状态概念），新批量入口请用 normalize_response/batch。
    """
    if not isinstance(record, dict):
        raise TypeError(f"process() 期望 dict 记录，收到 {type(record).__name__}")
    rec = normalize_record(record, source=str(record.get("src", "")))
    rec["schema_version"] = rec.get("schema_version") or DEFAULT_SCHEMA_VERSION
    # 旧流水线只认 id/src/name/value/status/ts；新增字段附加在后，不改变旧值
    return rec
