"""单一职责：来源原文归档。

normalize 只在规范记录/隔离项里保留内容寻址摘要（``raw_digest``），
原文统一存放在本模块，避免把大段机构原文塞进每条资源记录。

- 存储为 SQLite 表 ``raw_archive(digest, content, content_type, source, created_at)``；
- digest 用 sha256 over 规范化 JSON（sort_keys），同一机构重复同步得到同一摘要，天然幂等；
- 与业务 ``records`` 表分离，隔离项/错误包也能回查，但不会被当成资源记录。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from . import config


def digest_of(payload: Any) -> str:
    """对任意可 JSON 化原文计算稳定摘要（与键序无关、与重复同步无关）。"""
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


_DDL = (
    "CREATE TABLE IF NOT EXISTS raw_archive("
    "digest TEXT PRIMARY KEY, content TEXT NOT NULL, "
    "content_type TEXT NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL)"
)


def ensure_table(con: sqlite3.Connection) -> None:
    con.execute(_DDL)


def connect(con: sqlite3.Connection | None = None) -> sqlite3.Connection:
    own = con is None
    con = con or sqlite3.connect(config.DB_PATH)
    ensure_table(con)
    if own:
        con.commit()
    return con


def archive(payload: Any, *, source: str = "", content_type: str = "application/json",
            con: sqlite3.Connection | None = None) -> str:
    """归档一份来源原文，返回其摘要。重复归档为幂等 no-op。"""
    own = con is None
    con = con or connect()
    try:
        if not own:
            ensure_table(con)
        digest = digest_of(payload)
        con.execute(
            "INSERT OR IGNORE INTO raw_archive(digest,content,content_type,source,created_at) "
            "VALUES(?,?,?,?,?)",
            (digest,
             json.dumps(payload, ensure_ascii=False, default=str),
             content_type, str(source),
             datetime.now(timezone.utc).isoformat()),
        )
        if own:
            con.commit()
        return digest
    finally:
        if own:
            con.close()


def fetch(digest: str, con: sqlite3.Connection | None = None) -> Any | None:
    """按摘要回查原文；不存在返回 None。"""
    own = con is None
    con = con or connect()
    try:
        row = con.execute("SELECT content FROM raw_archive WHERE digest=?", (digest,)).fetchone()
        return json.loads(row[0]) if row else None
    finally:
        if own:
            con.close()
