"""薄编排。

两条入口，均不含业务判断，只按顺序调用单一职责阶段：

* ``run()``（旧路径，保持不变）：loader 裸记录 → normalize→budget→scheduler → keyer →
  幂等 upsert sqlite。
* ``sync_responses()``（新路径）：loader 响应批次 → normalize 整批（重排/隔离/游标）→
  conditional 缓存比较 → keyer 稳定键 → store 幂等落库 → incremental 游标推进。
  错误包/隔离项不进 keyer/store；游标推进与落库共用同一连接、同一事务。
"""
import importlib

from . import incremental, loader, normalize, rawarchive, store as _store
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalize', 'budget', 'scheduler']


def run(con=None):
    own = con is None
    con = con or _store.connect()
    try:
        chain = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            chain.append(mod.process)
        fed = 0
        for raw in loader.load():
            rec = raw
            for fn in chain:
                rec = fn(rec)
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()


def sync_responses(responses=None, *, con=None, stop_after_pages: int | None = None):
    """规范化并同步一批机构响应，返回稳定的同步报告。

    ``stop_after_pages`` 仅用于测试中断恢复：处理指定页数后抛
    ``InterruptedSync``，已落库记录随连接保留，游标状态同样已持久化，
    再次调用即可从断点继续。
    """
    own = con is None
    con = con or _store.connect()
    rawarchive.connect(con)
    try:
        raw_responses = responses if responses is not None else loader.load_responses()
        batch = normalize.normalize_batch(raw_responses, con=con)

        synced = unchanged = quarantined = empty_pages = error_pages = 0
        cursor_moves = 0
        processed_pages = 0
        for response in batch.responses:
            if stop_after_pages is not None and processed_pages >= stop_after_pages:
                _store.commit(con)
                raise InterruptedSync(_report(
                    con, batch, synced, unchanged, quarantined, empty_pages,
                    error_pages, cursor_moves, processed_pages))
            processed_pages += 1

            if response.status == normalize.RESP_ERROR:
                error_pages += 1
                quarantined += len(response.quarantined)
                # 错误响应不推进游标
                incremental.advance(con, response)
                continue
            if response.status == normalize.RESP_EMPTY:
                empty_pages += 1

            for rec in response.records:
                key = key_of(rec)
                existing = _store.get(con, key)
                verdict = _conditional_module().process(rec, existing)
                rec["_conditional"] = verdict["_conditional"]
                if rec["_conditional"]["decision"] == "unchanged":
                    unchanged += 1
                    continue
                _store.upsert(con, Record(key=key, data=rec))
                synced += 1

            move = incremental.advance(con, response)
            if move["advanced"]:
                cursor_moves += 1

        _store.commit(con)
        return _report(con, batch, synced, unchanged, quarantined, empty_pages,
                       error_pages, cursor_moves, processed_pages)
    finally:
        if own:
            con.close()


def _conditional_module():
    return importlib.import_module(".conditional", __package__)


def _report(con, batch, synced, unchanged, quarantined, empty_pages,
            error_pages, cursor_moves, processed_pages):
    return {
        "pages": batch.pages,
        "pages_processed": processed_pages,
        "syncable_records": batch.syncable_records,
        "synced": synced,
        "unchanged": unchanged,
        "quarantined": quarantined,
        "empty_pages": empty_pages,
        "error_pages": error_pages,
        "cursor_moves": cursor_moves,
        "order_gaps": list(batch.order_gaps),
        "rows": _store.count(con),
    }


class InterruptedSync(Exception):
    """中断信号：携带中断点之前的部分报告。"""
    def __init__(self, partial: dict):
        super().__init__("sync interrupted for recovery test")
        self.partial = partial
