"""单一职责：条件请求 / 缓存比较。

消费 normalize 的规范产物（记录的 ``etag`` / ``updated_at``，响应级 etag），
对照本地已知缓存状态，给出确定性的同步决策，不做任何落库或游标推进。

决策语义：
  - ``fetch``        ：无缓存或缓存失效，必须同步；
  - ``unchanged``    ：强 ETag 全等，或 updated_at 未变，可跳过资源处理；
  - ``changed``      ：ETag 变化（含弱标记差异）或 updated_at 更新，需要重新落库；
  - ``quarantine``   ：error 响应/隔离项，不参与缓存比较。

弱 ETag（W/）只用于“可能不变”的判断：弱 tag 不同但 updated_at 相同，保守判 changed；
弱 tag 相同则可判 unchanged（与 HTTP 条件请求语义一致）。
"""
from __future__ import annotations

from typing import Any

from .models import NormalizedResponse

DECISION_FETCH = "fetch"
DECISION_UNCHANGED = "unchanged"
DECISION_CHANGED = "changed"
DECISION_QUARANTINE = "quarantine"


def _split_etag(tag: str | None) -> tuple[bool, str | None]:
    if not tag:
        return False, None
    weak = tag.startswith("W/")
    return weak, tag[2:] if weak else tag


def compare_record(record: dict[str, Any], cached: dict[str, Any] | None) -> dict[str, Any]:
    """比较单条规范记录与缓存状态，返回 {decision, reason, key fields}。"""
    if cached is None:
        return {"decision": DECISION_FETCH, "reason": "no-cache",
                "id": record.get("id"), "src": record.get("src")}

    new_etag = record.get("etag")
    old_etag = cached.get("etag")
    if new_etag or old_etag:
        new_weak, new_body = _split_etag(new_etag)
        old_weak, old_body = _split_etag(old_etag)
        if new_body == old_body and new_body is not None:
            # body 相同：强弱差异不影响“内容指纹一致”的判断
            reason = "etag-equal" + ("-weak" if (new_weak or old_weak) else "")
            return {"decision": DECISION_UNCHANGED, "reason": reason,
                    "id": record.get("id"), "src": record.get("src")}
        return {"decision": DECISION_CHANGED,
                "reason": "etag-differ",
                "id": record.get("id"), "src": record.get("src")}

    new_updated = record.get("updated_at") or record.get("ts")
    old_updated = cached.get("updated_at") or cached.get("ts")
    if new_updated and old_updated:
        if new_updated == old_updated:
            return {"decision": DECISION_UNCHANGED, "reason": "updated_at-equal",
                    "id": record.get("id"), "src": record.get("src")}
        return {"decision": DECISION_CHANGED, "reason": "updated_at-advances",
                "id": record.get("id"), "src": record.get("src")}

    # 没有任何缓存比较依据：保守要求重新处理（幂等 upsert 保证不增行）
    return {"decision": DECISION_FETCH, "reason": "no-comparable-validator",
            "id": record.get("id"), "src": record.get("src")}


def evaluate_response(response: NormalizedResponse,
                      cached_etag: str | None = None) -> dict[str, Any]:
    """响应级条件判断：错误包直接隔离；空结果比较响应 ETag 决定是否要同步处理。"""
    if response.status == "error":
        return {"decision": DECISION_QUARANTINE,
                "reason": "error-response",
                "source": response.source,
                "quarantined": [q.kind for q in response.quarantined]}
    _, new_body = _split_etag(response.etag)
    _, old_body = _split_etag(cached_etag)
    if new_body and old_body and new_body == old_body:
        return {"decision": DECISION_UNCHANGED, "reason": "response-etag-equal",
                "source": response.source}
    if response.status == "empty":
        return {"decision": DECISION_FETCH, "reason": "empty-page-advance-cursor",
                "source": response.source}
    return {"decision": DECISION_FETCH, "reason": "stale-or-no-etag",
            "source": response.source}


def process(record: dict[str, Any], cached: dict[str, Any] | None = None) -> dict[str, Any]:
    """流水线形接口：给记录附加 ``_conditional`` 决策并原样返回，供链式阶段消费。

    无缓存参数时按首见处理（fetch）；不修改规范字段本身。
    """
    verdict = compare_record(record, cached)
    out = dict(record)
    out["_conditional"] = verdict
    return out
