"""增量同步凭据：游标 / ETag / Last-Modified 的持久化与恢复。

与账本的分工：budget.py 记录"额度事实"，本模块记录"抓取位置事实"。
两者共用同一个 sqlite 连接但表独立，任一更新随各自事务原子落盘。

恢复语义（进程重启后）：
  - load_cursor() 取回上次成功推进的位置与条件头，供 scheduler 续跑；
  - pending_request_id 指向最后一次已预留但未确认终结的请求：
    先交给 budget.reap_orphans() 回收租约，再按该 request_id 对账，
    保证"同一页"永远复用同一幂等键，不会因为重启重复扣费。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_DDL = """
CREATE TABLE IF NOT EXISTS sync_cursor(
    org                 TEXT NOT NULL,
    endpoint            TEXT NOT NULL,
    cursor              TEXT,
    etag                TEXT,
    last_modified       TEXT,
    last_request_id     TEXT,
    last_status         TEXT,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (org, endpoint)
);
"""


@dataclass(frozen=True)
class Cursor:
    org: str
    endpoint: str
    cursor: str | None
    etag: str | None
    last_modified: str | None
    last_request_id: str | None
    last_status: str | None
    updated_at: str

    def conditional_headers(self) -> dict[str, str]:
        """RFC 9110 13.1：If-None-Match / If-Modified-Since 条件请求头。"""
        headers: dict[str, str] = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        return headers


def init(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)
    conn.commit()


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_cursor(
    conn: sqlite3.Connection,
    org: str,
    endpoint: str,
    *,
    cursor: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    last_request_id: str | None = None,
    last_status: str | None = None,
) -> None:
    """UPSERT 一个 (org, endpoint) 的抓取位置。None 字段保留旧值。"""
    exists = conn.execute(
        "SELECT 1 FROM sync_cursor WHERE org=? AND endpoint=?", (org, endpoint)
    ).fetchone()
    if exists is None:
        conn.execute(
            "INSERT INTO sync_cursor(org,endpoint,cursor,etag,last_modified,"
            "last_request_id,last_status,updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (org, endpoint, cursor, etag, last_modified,
             last_request_id, last_status, _iso()),
        )
    else:
        conn.execute(
            "UPDATE sync_cursor SET cursor=COALESCE(?,cursor),"
            " etag=COALESCE(?,etag), last_modified=COALESCE(?,last_modified),"
            " last_request_id=COALESCE(?,last_request_id),"
            " last_status=COALESCE(?,last_status), updated_at=?"
            " WHERE org=? AND endpoint=?",
            (cursor, etag, last_modified, last_request_id,
             last_status, _iso(), org, endpoint),
        )
    conn.commit()


def load_cursor(conn: sqlite3.Connection, org: str, endpoint: str) -> Cursor | None:
    row = conn.execute(
        "SELECT * FROM sync_cursor WHERE org=? AND endpoint=?", (org, endpoint)
    ).fetchone()
    return Cursor(**dict(row)) if row else None


def all_cursors(conn: sqlite3.Connection) -> list[Cursor]:
    rows = conn.execute(
        "SELECT * FROM sync_cursor ORDER BY org, endpoint"
    ).fetchall()
    return [Cursor(**dict(r)) for r in rows]


def process(record):
    """流水线阶段兼容接口（真正的游标操作由 scheduler/conditional 显式调用）。"""
    rec = dict(record)
    rec.setdefault("_stage_incremental", "cursor")
    return rec
