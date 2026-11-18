"""单一职责：条件请求（ETag / Last-Modified）缓存与命中预期。

调度器在选任务时只关心一件事：**这次请求大概率能不能省下来**。
本模块因此提供：

* ``CacheEntry``：某来源某区间的 ETag / Last-Modified 与内容指纹；
* ``CacheStore``：按来源维护缓存，回答 ``hit`` / ``miss`` / ``revalidate``；
* ``CacheExpectation``：对一次潜在请求的命中概率估计，供调度打分；
* ``should_fetch``：给调度器用的最小判定入口。

设计要点：

1. 缓存命中不能只凭“有 ETag”，还要比对请求指纹（区间/游标/参数）；
2. 命中 ``304 Not Modified`` 与“命中本地缓存”都能省一次完整抓取；
3. 没有缓存信息的来源必须按 miss 处理（保守策略，不会漏数）。

旧 happy-path 的 ``process(record)`` 行为保持不变。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping


class CacheError(ValueError):
    """缓存数据非法。"""


def fingerprint(payload: Any) -> str:
    """稳定内容指纹（用于判断“同一份内容”）。"""
    if isinstance(payload, str):
        raw = payload.encode("utf-8")
    else:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                         default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _canonical_interval(interval: Any) -> Any:
    """把区间统一成 ``[start, end]``，保证 tuple/list/dict/Interval 共享缓存。"""
    if interval is None:
        return None
    if hasattr(interval, "to_dict"):
        interval = interval.to_dict()
    if isinstance(interval, Mapping):
        return [int(interval.get("start", 0)), int(interval.get("end", 0))]
    if isinstance(interval, (list, tuple)) and len(interval) == 2:
        return [int(interval[0]), int(interval[1])]
    return interval


def request_key(source: str, interval: Any = None, params: Any = None) -> str:
    """请求指纹：同一来源 + 同一窗口/参数才共享缓存。"""
    return fingerprint({"source": source,
                        "interval": _canonical_interval(interval),
                        "params": params or {}})


@dataclass
class CacheEntry:
    """一条缓存记录。"""

    source: str
    key: str
    etag: str | None = None
    last_modified: str | None = None
    content_hash: str | None = None
    interval: Any = None
    params: dict[str, Any] = field(default_factory=dict)
    stored_at: float | None = None
    hits: int = 0
    validators_used: int = 0

    @property
    def has_validator(self) -> bool:
        return bool(self.etag or self.last_modified)

    def can_revalidate(self, source: str, interval: Any = None,
                       params: Any = None) -> bool:
        return (self.has_validator
                and self.source == source
                and self.key == request_key(source, interval, params))

    def to_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        return headers

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source, "key": self.key, "etag": self.etag,
            "last_modified": self.last_modified,
            "content_hash": self.content_hash,
            "interval": self.interval.to_dict()
            if hasattr(self.interval, "to_dict") else self.interval,
            "params": dict(self.params), "stored_at": self.stored_at,
            "hits": self.hits, "validators_used": self.validators_used,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CacheEntry":
        return cls(
            source=str(data.get("source", "")),
            key=str(data.get("key", "")),
            etag=data.get("etag"),
            last_modified=data.get("last_modified"),
            content_hash=data.get("content_hash"),
            interval=data.get("interval"),
            params=dict(data.get("params") or {}),
            stored_at=data.get("stored_at"),
            hits=int(data.get("hits", 0)),
            validators_used=int(data.get("validators_used", 0)),
        )


@dataclass
class CacheStore:
    """按请求指纹索引的缓存。"""

    entries: dict[str, CacheEntry] = field(default_factory=dict)

    def get(self, source: str, interval: Any = None,
            params: Any = None) -> CacheEntry | None:
        return self.entries.get(request_key(source, interval, params))

    def lookup(self, source: str, interval: Any = None,
               params: Any = None) -> CacheEntry | None:
        return self.get(source, interval, params)

    def put(self, source: str, interval: Any = None, *, etag: str | None = None,
            last_modified: str | None = None, body: Any = None,
            params: Any = None, stored_at: float | None = None) -> CacheEntry:
        """写入/更新缓存。``body`` 用于计算内容指纹。"""
        key = request_key(source, interval, params)
        entry = CacheEntry(
            source=source, key=key, etag=etag, last_modified=last_modified,
            content_hash=fingerprint(body) if body is not None else None,
            interval=interval, params=dict(params or {}), stored_at=stored_at,
        )
        self.entries[key] = entry
        return entry

    def record_hit(self, source: str, interval: Any = None,
                   params: Any = None, revalidated: bool = False) -> None:
        entry = self.get(source, interval, params)
        if entry is None:
            return
        entry.hits += 1
        if revalidated:
            entry.validators_used += 1

    def invalidate(self, source: str, interval: Any = None,
                   params: Any = None) -> bool:
        key = request_key(source, interval, params)
        return self.entries.pop(key, None) is not None

    def evict_source(self, source: str) -> int:
        doomed = [k for k, e in self.entries.items() if e.source == source]
        for key in doomed:
            self.entries.pop(key, None)
        return len(doomed)

    def sources(self) -> list[str]:
        return sorted({e.source for e in self.entries.values()})

    def to_dict(self) -> dict[str, Any]:
        return {k: e.to_dict() for k, e in sorted(self.entries.items())}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "CacheStore":
        return cls({str(k): CacheEntry.from_dict(v)
                    for k, v in (data or {}).items()})


@dataclass
class CacheExpectation:
    """对一次请求“能省下多少”的估计。"""

    #: 0.0 ~ 1.0，越大越可能命中
    hit_probability: float = 0.0
    #: ``full`` / ``revalidate`` / ``miss``
    kind: str = "miss"
    has_validator: bool = False
    reason: str = "no-cache"

    @property
    def expected_hit(self) -> bool:
        return self.kind in {"full", "revalidate"}

    @property
    def is_hit(self) -> bool:
        return self.expected_hit

    @property
    def saves_request(self) -> bool:
        """完整命中可完全省下请求；revalidate 省下的是响应体。"""
        return self.kind == "full"

    @property
    def cost_factor(self) -> float:
        """调度打分用的成本系数：完整命中近乎免费。"""
        return {"full": 0.0, "revalidate": 0.35}.get(self.kind, 1.0)

    def to_dict(self) -> dict[str, Any]:
        return {"hit_probability": round(self.hit_probability, 4),
                "kind": self.kind, "has_validator": self.has_validator,
                "reason": self.reason}


def expectation(cache: CacheStore | Mapping[str, Any] | None, source: str,
                interval: Any = None, params: Any = None) -> CacheExpectation:
    """估计一次请求的缓存命中情况。"""
    store = cache if isinstance(cache, CacheStore) else CacheStore.from_dict(cache)
    entry = store.get(source, interval, params)
    if entry is None:
        return CacheExpectation(kind="miss", reason="no-cache")
    if entry.content_hash is not None and not entry.has_validator:
        return CacheExpectation(1.0, "full", False, "content-addressed")
    if entry.has_validator:
        return CacheExpectation(0.85, "revalidate", True, "validator-present")
    return CacheExpectation(0.0, "miss", False, "stale-entry")


#: 简写别名
estimate = expectation
expectation_of = expectation


def is_hit(cache: CacheStore | Mapping[str, Any] | None, source: str,
           interval: Any = None, params: Any = None) -> bool:
    return expectation(cache, source, interval, params).expected_hit


def should_fetch(cache: CacheStore | Mapping[str, Any] | None, source: str,
                 interval: Any = None, params: Any = None,
                 force: bool = False) -> bool:
    """是否需要真的发起请求（考虑缓存与强制刷新）。"""
    if force:
        return True
    return not expectation(cache, source, interval, params).saves_request


def hit_probability(cache: CacheStore | Mapping[str, Any] | None, source: str,
                    interval: Any = None, params: Any = None) -> float:
    return expectation(cache, source, interval, params).hit_probability


# ------------------------------------------------------------------ 流水线阶段
def process(record):
    """旧 happy-path 阶段：只做记录变换。"""
    rec = dict(record)
    rec.setdefault("_stage_conditional", "ok")
    return rec
