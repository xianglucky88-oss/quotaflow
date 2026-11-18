"""单一职责：派生稳定主键。"""
import hashlib, json

def key_of(record):
    rid = record.get("id")
    if rid:
        return str(rid)
    return hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def request_key(org, endpoint, params=None, *, round_tag="", prefix="req"):
    """逻辑请求的幂等键。

    同一 (机构, 接口, 参数, 轮次标签) 无论重试多少次、进程是否重启，都派生
    出同一个 request_id，使 budget.reserve 的幂等分支与条件重试始终对应同一笔
    预留，从根本上杜绝重复扣费。参数排序后哈希，消除字典顺序差异。

    round_tag 区分"同一页的失败重试"与"游标推进后的新一轮抓取"：
    前者必须复用同键（超时重试不双扣），后者必须换新键
    （上一轮已终结的 request_id 不可复活）。通常取当前游标值。
    """
    payload = json.dumps(
        {"org": org, "endpoint": endpoint, "params": params or {},
         "round_tag": round_tag},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}:{org}:{digest}"
