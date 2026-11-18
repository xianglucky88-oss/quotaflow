"""单一职责：增量同步的游标语义与断点状态。

消费 normalize 的规范游标（``PageCursor``）与响应状态，维护每机构一份持久化同步状态：

* ``last_cursor``   ：已确认落库的最后一页的 *下一页* token（即下次请求应带的游标）；
* ``last_page_index``：已确认的连续页序号，乱序/缺口页不会推进它；
* ``last_etag``     ：最近一次成功响应的 ETag，供 conditional 比较；
* ``updated_at``    ：最近一次成功响应的规范更新时间（UTC）。

关键语义
--------
1. 只有 ``ok`` / ``empty`` 响应推进游标；``error`` 响应（错误包/阻断）不推进，
   下次同步仍从原游标重试——错误包不会污染增量位置。
2. 页序号必须连续：页 1 已确认而页 2 缺失时，页 3 即使先到也不推进
   （配合 normalize 检测的 order_gaps），中断恢复后从缺口页重放。
3. 末页 ``next_cursor is None``：游标置空但 ``finished`` 置位，下轮从头/全量校验，
   与“还有下一页”明确区分。
4. 所有状态写入与业务落库共用同一 sqlite 连接、同一事务边界，中断不会出现
   “游标前进了但记录没落库”的错位。
"""
from __future__ import annotations

import sqlite3
from typing import Any

from .models import NormalizedResponse

SYNC_TABLE = "sync_state"


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {SYNC_TABLE}("
        "source TEXT PRIMARY KEY, last_cursor TEXT, last_page_index INTEGER NOT NULL, "
        "finished INTEGER NOT NULL, last_etag TEXT, updated_at TEXT, "
        "last_raw_digest TEXT, updated_count INTEGER NOT NULL DEFAULT 0)")


def get_state(con: sqlite3.Connection, source: str) -> dict[str, Any]:
    ensure_table(con)
    row = con.execute(
        f"SELECT last_cursor,last_page_index,finished,last_etag,updated_at,"
        f"last_raw_digest,updated_count FROM {SYNC_TABLE} WHERE source=?",
        (source,)).fetchone()
    if row is None:
        return {"source": source, "last_cursor": None, "last_page_index": -1,
                "finished": False, "last_etag": None, "updated_at": None,
                "last_raw_digest": None, "updated_count": 0}
    return {"source": source, "last_cursor": row[0], "last_page_index": row[1],
            "finished": bool(row[2]), "last_etag": row[3], "updated_at": row[4],
            "last_raw_digest": row[5], "updated_count": row[6]}


def advance(con: sqlite3.Connection, response: NormalizedResponse) -> dict[str, Any]:
    """根据一页规范响应推进（或拒绝推进）同步状态，返回推进结果说明。"""
    ensure_table(con)
    state = get_state(con, response.source)

    if response.status == "error" or response.cursor is None:
        # 错误包/无游标的阻断响应：状态原地不动
        return {"advanced": False, "reason": "error-or-no-cursor",
                "source": response.source, "state": state}

    cursor = response.cursor
    expected_index = state["last_page_index"] + 1
    if cursor.page_index != expected_index:
        # 乱序页 / 缺口页 / 重复页：不推进连续位置
        if cursor.page_index <= state["last_page_index"]:
            return {"advanced": False, "reason": "already-applied-page",
                    "source": response.source, "state": state}
        return {"advanced": False,
                "reason": f"page-gap: want {expected_index} got {cursor.page_index}",
                "source": response.source, "state": state}

    finished = not cursor.has_more and cursor.next_cursor is None
    con.execute(
        f"INSERT INTO {SYNC_TABLE}(source,last_cursor,last_page_index,finished,"
        f"last_etag,updated_at,last_raw_digest,updated_count) VALUES(?,?,?,?,?,?,?,1) "
        f"ON CONFLICT(source) DO UPDATE SET last_cursor=excluded.last_cursor,"
        f"last_page_index=excluded.last_page_index,finished=excluded.finished,"
        f"last_etag=excluded.last_etag,updated_at=excluded.updated_at,"
        f"last_raw_digest=excluded.last_raw_digest,"
        f"updated_count={SYNC_TABLE}.updated_count+1",
        (response.source, cursor.next_cursor, cursor.page_index, int(finished),
         response.etag, response.updated_at, response.raw_digest))
    new_state = get_state(con, response.source)
    return {"advanced": True, "reason": "page-applied", "finished": finished,
            "source": response.source, "state": new_state}


def reset(con: sqlite3.Connection, source: str) -> None:
    """显式重置某机构游标（下轮从首页重新开始）。"""
    ensure_table(con)
    con.execute(f"DELETE FROM {SYNC_TABLE} WHERE source=?", (source,))


def next_cursor(con: sqlite3.Connection, source: str) -> str | None:
    """下一次请求该机构时应携带的游标；None 表示从头开始。"""
    return get_state(con, source)["last_cursor"]


def process(record: dict[str, Any], con: sqlite3.Connection | None = None) -> dict[str, Any]:
    """流水线形接口：为单条记录附加增量定位信息（不改规范字段、不推进页游标）。

    页级推进属于 ``advance`` 的职责；这里只标注记录所属机构当前已知位置，
    使链式阶段在无连接环境下也可安全调用。
    """
    out = dict(record)
    if con is None:
        out["_incremental"] = {"state": "unbound", "src": record.get("src")}
        return out
    state = get_state(con, str(record.get("src", "")))
    out["_incremental"] = {
        "state": "bound", "last_cursor": state["last_cursor"],
        "last_page_index": state["last_page_index"], "finished": state["finished"]}
    return out
