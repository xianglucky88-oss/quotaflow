"""单一职责：派生稳定主键。

稳定性语义（重复同步 / 中断恢复后必须得到同一个键）：
  1. 优先使用 normalize 产出的规范资源标识 ``id``（机构原生 ID 或 ``synth:`` 指纹）；
  2. 跨机构用 ``src`` 命名空间隔离，避免不同机构同一原生 ID 撞键；
  3. 既无 ``id`` 又无 ``src`` 时退化为全字段 sha256（与旧实现一致），保证旧路径不变。

keyer 只消费规范记录，不感知错误包/隔离项——那些数据在 normalize 已被拦下，
因此错误包永远不会被派生为资源键。
"""
import hashlib
import json


def key_of(record):
    rid = record.get("id")
    src = str(record.get("src", "")).strip().lower()
    if rid:
        rid = str(rid)
        return f"{src}:{rid}" if src else rid
    if src:
        # 理论上 normalize 已保证 id 存在；双保险：命名空间 + 内容指纹
        digest = hashlib.sha256(
            json.dumps(record, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:16]
        return f"{src}:{digest}"
    return hashlib.sha256(
        json.dumps(record, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()[:16]
