"""稳定数据契约：跨阶段传递的规范结构。

- ``Record``：单条规范资源记录，沿用既有流水线（keyer/store）。
- ``PageCursor``：规范分页游标，来源机构的分页语义在此被统一。
- ``QuarantineItem``：隔离项——不可同步的响应/记录（错误包、缺标识、schema 阻断等）。
- ``NormalizedResponse`` / ``NormalizedBatch``：normalize 对整页/整批响应的输出。

所有结构均可通过 ``as_dict`` 转为纯 JSON 类型，便于落库与回查。
"""
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class Record:
    key: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class PageCursor:
    """规范分页游标。

    - ``next_cursor`` 为 ``None`` 表示本页已是末页（游标终止，语义稳定，不依赖机构原始 token）。
    - ``has_more`` 与 ``next_cursor is not None`` 保持一致。
    - 游标只表达“下一页在哪里”，不夹带资源内容；乱序页在 normalize 阶段按页序号重排。
    """
    page_index: int
    next_cursor: str | None
    has_more: bool
    page_size: int
    raw_cursor: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class QuarantineItem:
    """隔离项：永远不会进入 keyer/store 的资源落库路径。

    ``kind`` 取值见 ``normalize.QUARANTINE_REASONS``；
    ``raw_digest`` 指向 rawarchive 中保留的来源原文，可回查。
    """
    kind: str
    detail: str
    source: str = ""
    raw_digest: str | None = None
    schema_version: str | None = None
    raw_ref: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class NormalizedResponse:
    """单页（单次机构响应）规范化结果。

    响应状态 ``status``：
      - ``ok``     ：至少产出可同步记录（或带删除标记的资源）；
      - ``empty``  ：机构明确表达“本页无资源”（空列表/空信封），游标仍可推进；
      - ``error``  ：错误包/不可解析/未知信封——整页隔离，无任何资源记录。

    同步语义：只有 ``records`` 中的条目可进入下游；``quarantined`` 仅可隔离与回查。
    """
    source: str
    status: str
    records: list[dict[str, Any]] = field(default_factory=list)
    cursor: PageCursor | None = None
    etag: str | None = None
    updated_at: str | None = None
    schema_version: str | None = None
    quarantined: list[QuarantineItem] = field(default_factory=list)
    raw_digest: str | None = None
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


@dataclass
class NormalizedBatch:
    """一批响应（多机构、多页、可能乱序）的聚合结果。"""
    responses: list[NormalizedResponse] = field(default_factory=list)
    pages: int = 0
    syncable_records: int = 0
    quarantined: list[QuarantineItem] = field(default_factory=list)
    order_gaps: list[int] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "responses": [r.as_dict() for r in self.responses],
            "pages": self.pages,
            "syncable_records": self.syncable_records,
            "quarantined": [q.as_dict() for q in self.quarantined],
            "order_gaps": list(self.order_gaps),
            "notes": list(self.notes),
        }

    def iter_records(self):
        """按重排后的稳定顺序产出全部可同步规范记录。"""
        for resp in self.responses:
            for rec in resp.records:
                yield rec
