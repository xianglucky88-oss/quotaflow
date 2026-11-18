"""单一职责：增量同步游标与“未确认区间”。

本模块只回答一件事：**某机构下一次应该从哪里继续抓**。它不扣预算、不做排期。

核心概念：

* ``Cursor``：机构级进度。``committed`` 是已确认（已落库）的水位，
  ``attempts`` 是各次尝试的返回区间；只有被确认的尝试才会推进 ``committed``。
* ``Interval``：闭开区间 ``[start, end)``，用于表达待抓窗口。
* 失败/部分成功时，尝试区间保持 **未确认**，下次调度必须重新覆盖，
  不能因为“发起过请求”就跳过 —— 这是增量同步最容易出错的地方。

兼容：同时支持 dict 记录与 :class:`Cursor` 对象；旧 happy-path 的
``process(record)`` 行为保持不变。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


class CursorError(ValueError):
    """游标数据非法。"""


@dataclass(frozen=True, order=True)
class Interval:
    """闭开区间 ``[start, end)``。"""

    start: int
    end: int

    def __post_init__(self):
        if int(self.end) < int(self.start):
            raise CursorError(f"invalid interval [{self.start}, {self.end})")

    def __len__(self) -> int:
        return int(self.end) - int(self.start)

    def __bool__(self) -> bool:
        return int(self.end) > int(self.start)

    def overlaps(self, other: "Interval") -> bool:
        return int(self.start) < int(other.end) and int(other.start) < int(self.end)

    def contains(self, point: int) -> bool:
        return int(self.start) <= int(point) < int(self.end)

    def to_dict(self) -> dict[str, int]:
        return {"start": int(self.start), "end": int(self.end)}

    @classmethod
    def from_dict(cls, data: Any) -> "Interval":
        if isinstance(data, Interval):
            return data
        if isinstance(data, (list, tuple)) and len(data) == 2:
            return cls(int(data[0]), int(data[1]))
        if isinstance(data, Mapping):
            return cls(int(data.get("start", 0)), int(data.get("end", 0)))
        raise CursorError(f"cannot build interval from {data!r}")


def _coerce(intervals: Iterable[Any]) -> list["Interval"]:
    return [Interval.from_dict(i) for i in intervals
            if not isinstance(i, (list, tuple)) or len(i) == 2]


def merge(intervals: Iterable[Interval]) -> list[Interval]:
    """合并重叠/相邻区间，按起点排序。接受 Interval/元组/dict。"""
    ordered = sorted((i for i in _coerce(intervals) if i),
                     key=lambda i: (i.start, i.end))
    out: list[Interval] = []
    for cur in ordered:
        if out and cur.start <= out[-1].end:
            out[-1] = Interval(out[-1].start, max(out[-1].end, cur.end))
        else:
            out.append(Interval(cur.start, cur.end))
    return out


def subtract(base: Iterable[Interval],
             holes: Iterable[Interval]) -> list[Interval]:
    """从 ``base`` 中挖掉 ``holes``，返回剩余区间。"""
    result = merge(base)
    for hole in merge(holes):
        nxt: list[Interval] = []
        for piece in result:
            if not piece.overlaps(hole):
                nxt.append(piece)
                continue
            if piece.start < hole.start:
                nxt.append(Interval(piece.start, min(piece.end, hole.start)))
            if piece.end > hole.end:
                nxt.append(Interval(max(piece.start, hole.end), piece.end))
        result = [i for i in nxt if i]
    return result


@dataclass
class Attempt:
    """一次抓取尝试。``confirmed=False`` 表示结果未落库/未确认。

    ``resolved=True`` 表示该尝试已被后续结果取代（例如重试成功、部分完成后
    重新切分），只保留在历史里，不再计入“未确认区间”。
    """

    interval: Interval
    confirmed: bool = False
    ok: bool = True
    reason: str = ""
    at: float | None = None
    resolved: bool = False

    @property
    def unconfirmed(self) -> bool:
        return bool(self.interval) and not self.confirmed and not self.resolved

    def to_dict(self) -> dict[str, Any]:
        return {"interval": self.interval.to_dict(), "confirmed": self.confirmed,
                "ok": self.ok, "reason": self.reason, "at": self.at,
                "resolved": self.resolved}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Attempt":
        return cls(
            interval=Interval.from_dict(data.get("interval") or (0, 0)),
            confirmed=bool(data.get("confirmed", False)),
            ok=bool(data.get("ok", True)),
            reason=str(data.get("reason", "")),
            at=data.get("at"),
            resolved=bool(data.get("resolved", False)),
        )


@dataclass
class Cursor:
    """机构同步游标。"""

    source: str
    committed: int = 0
    attempts: list[Attempt] = field(default_factory=list)
    #: 真实数据上界（例如上游 last-modified 序号）；``None`` 表示未知
    head: int | None = None
    version: int = 0

    # ------------------------------------------------------------- 进度查询
    def confirmed_bounds(self) -> list[Interval]:
        return merge(a.interval for a in self.attempts if a.confirmed)

    def unconfirmed_bounds(self) -> list[Interval]:
        """尚未确认的区间，按起点排序合并。"""
        return merge(a.interval for a in self.attempts if a.unconfirmed)

    #: 常用别名
    pending = unconfirmed_bounds

    def unconfirmed(self) -> list[Interval]:
        return self.unconfirmed_bounds()

    def unconfirmed_size(self) -> int:
        return sum(len(i) for i in self.unconfirmed_bounds())

    def is_caught_up(self) -> bool:
        return self.next_interval() is None

    caught_up = is_caught_up

    def next_interval(self, limit: int | None = None) -> Interval | None:
        """下一段应抓区间。

        优先级：从 ``committed`` 起的第一段“未确认内容”（既包括失败尝试
        留下的区间，也包括从未尝试过的缺口），其次是向 ``head`` 推进的新区间。
        ``limit`` 给出本次最多抓多少条。
        """
        start = int(self.committed)
        pending = [i for i in self.unconfirmed_bounds() if int(i.end) > start]

        if pending:
            first = pending[0]
            piece_start = max(start, int(first.start))
            piece_end = int(first.end)
        else:
            piece_start, piece_end = start, start

        if piece_end > piece_start:
            if limit is not None:
                piece_end = min(piece_end, piece_start + max(1, int(limit)))
            return Interval(piece_start, piece_end)

        if self.head is None or start >= int(self.head):
            return None
        end = int(self.head) if limit is None else min(int(self.head),
                                                       start + max(1, int(limit)))
        if end <= start:
            return None
        return Interval(start, end)

    def backlog(self) -> int:
        """尚未确认/未抓取的条数。"""
        if self.head is not None:
            return max(0, int(self.head) - int(self.committed))
        return sum(len(i) for i in self.unconfirmed_bounds()
                   if int(i.end) > int(self.committed))

    # ------------------------------------------------------------- 状态迁移
    def _resolve(self, target: Interval, keep: Attempt | None = None) -> None:
        """把 ``target`` 覆盖到的旧未确认尝试标记为已解决（必要时切分残段）。"""
        rewritten: list[Attempt] = []
        for attempt in self.attempts:
            if attempt is keep or attempt.confirmed or attempt.resolved \
                    or not attempt.interval.overlaps(target):
                rewritten.append(attempt)
                continue
            attempt.resolved = True
            rewritten.append(attempt)
            for piece in subtract([attempt.interval], [target]):
                rewritten.append(Attempt(piece, confirmed=False, ok=attempt.ok,
                                         reason=attempt.reason, at=attempt.at))
        self.attempts = rewritten

    def start_attempt(self, interval: Interval | tuple[int, int],
                      at: float | None = None) -> Attempt:
        attempt = Attempt(interval=Interval.from_dict(interval), at=at)
        self.attempts.append(attempt)
        return attempt

    def _advance_committed(self) -> None:
        """沿连续已确认区间推进水位，缺口处停下（不跳跃、不掩盖空洞）。"""
        progressed = True
        while progressed:
            progressed = False
            for attempt in self.attempts:
                if (attempt.confirmed
                        and int(attempt.interval.start) <= int(self.committed)
                        < int(attempt.interval.end)):
                    self.committed = int(attempt.interval.end)
                    progressed = True

    def confirm(self, interval: Interval | tuple[int, int] | Attempt,
                at: float | None = None) -> None:
        """确认某区间已成功落库，推进 ``committed`` 水位。

        只有确认区间从当前水位起连续覆盖时才推进，避免跳跃式更新掩盖缺口。
        """
        if isinstance(interval, Attempt):
            interval.confirmed = True
            interval.ok = True
            interval.resolved = False
            target = interval.interval
            self._resolve(target, keep=interval)
        else:
            target = Interval.from_dict(interval)
            self._resolve(target)
            self.attempts.append(Attempt(target, confirmed=True, ok=True, at=at))
        self._advance_committed()
        self.version += 1

    def fail(self, interval: Interval | tuple[int, int] | Attempt,
             reason: str = "failed", at: float | None = None) -> Attempt:
        """记录一次失败/部分成功：区间保持未确认，后续必须重试。"""
        if isinstance(interval, Attempt):
            attempt = interval
            attempt.confirmed = False
            attempt.resolved = False
            attempt.ok = False
            attempt.reason = reason
            attempt.at = at if at is not None else attempt.at
            self._resolve(attempt.interval, keep=attempt)
        else:
            target = Interval.from_dict(interval)
            attempt = Attempt(target, confirmed=False, ok=False, reason=reason,
                              at=at)
            self._resolve(target)
            self.attempts.append(attempt)
        self.version += 1
        return attempt

    def complete(self, interval: Interval | tuple[int, int] | Attempt,
                 partial_until: int | None = None,
                 reason: str = "partial", at: float | None = None) -> None:
        """部分成功：``[start, partial_until)`` 确认，其余保持未确认。"""
        target = interval.interval if isinstance(interval, Attempt) else \
            Interval.from_dict(interval)
        if isinstance(interval, Attempt):
            # 原尝试被切片结果取代，标记已解决，避免残留整段未确认区间。
            interval.resolved = True
        cut = int(partial_until) if partial_until is not None else int(target.start)
        cut = max(int(target.start), min(int(target.end), cut))
        if cut > int(target.start):
            self.confirm(Interval(target.start, cut), at=at)
        if cut < int(target.end):
            self.fail(Interval(cut, target.end),
                      reason=reason or "partial", at=at)

    def rewind(self, to: int) -> None:
        """回拨已确认水位（例如上游数据被回滚）。"""
        self.committed = max(0, int(to))
        self.version += 1

    # -------------------------------------------------------------- 序列化
    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "committed": int(self.committed),
            "head": None if self.head is None else int(self.head),
            "version": int(self.version),
            "attempts": [a.to_dict() for a in self.attempts],
            "unconfirmed": [i.to_dict() for i in self.unconfirmed_bounds()],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None,
                  source: str | None = None) -> "Cursor":
        if not data:
            return cls(source=source or "")
        return cls(
            source=str(data.get("source", source or "")),
            committed=int(data.get("committed", 0)),
            head=None if data.get("head") is None else int(data["head"]),
            version=int(data.get("version", 0)),
            attempts=[Attempt.from_dict(a) for a in (data.get("attempts") or [])],
        )


# ---------------------------------------------------------------- 游标仓库
class CursorStore:
    """按机构保存游标的轻量容器（可序列化，便于重启恢复）。"""

    def __init__(self, cursors: Mapping[str, Cursor] | None = None):
        self._cursors: dict[str, Cursor] = dict(cursors or {})

    def get(self, source: str) -> Cursor:
        cur = self._cursors.get(source)
        if cur is None:
            cur = Cursor(source=source)
            self._cursors[source] = cur
        return cur

    def __contains__(self, source: object) -> bool:
        return source in self._cursors

    def sources(self) -> list[str]:
        return sorted(self._cursors)

    def items(self):
        return self._cursors.items()

    def to_dict(self) -> dict[str, Any]:
        return {name: cur.to_dict() for name, cur in sorted(self._cursors.items())}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "CursorStore":
        return cls({str(k): Cursor.from_dict(v, source=str(k))
                    for k, v in (data or {}).items()})


# ------------------------------------------------------------------ 便捷函数
def cursor_from(progress: Any, source: str | None = None) -> Cursor:
    """把 dict / Cursor 统一成 Cursor。"""
    if isinstance(progress, Cursor):
        return progress
    return Cursor.from_dict(progress, source=source)


def unconfirmed_intervals(progress: Any) -> list[Interval]:
    return cursor_from(progress).unconfirmed_bounds()


def next_interval(progress: Any, limit: int | None = None) -> Interval | None:
    return cursor_from(progress).next_interval(limit)


def backlog(progress: Any) -> int:
    return cursor_from(progress).backlog()


def is_caught_up(progress: Any) -> bool:
    return cursor_from(progress).is_caught_up()


# ------------------------------------------------------------------ 流水线阶段
def process(record):
    """旧 happy-path 阶段：只做记录变换。"""
    rec = dict(record)
    rec.setdefault("_stage_incremental", "ok")
    return rec
