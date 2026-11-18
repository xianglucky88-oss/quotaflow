"""单一职责：在每日预算内选择并安排“下一批”抓取任务。

本模块只做决策与记账，不亲自发请求：

* 可用额度读 :mod:`quotaflow.budget` 的账本；
* 未确认区间读 :mod:`quotaflow.incremental` 的游标；
* 缓存命中预期读 :mod:`quotaflow.conditional` 的缓存。

所有决定都会写入可序列化的历史，并随 :meth:`Scheduler.save` 落盘，
重启后用 :meth:`Scheduler.load` 恢复：账本花费、游标尝试、失败计数、
等待轮次、批次序号都原样带回，因此重启前后继续派发的顺序一致。

确定性的冲突规则（优先级/截止时间/防饥饿/重试/预算）
-------------------------------------------------------

候选机构先过硬门槛（任一命中即本轮跳过，并记录可审计的 ``reason``）：

1. ``disabled`` —— 机构被禁用；
2. ``deadline_passed`` —— 已过该机构（或全局）截止时间；
3. ``circuit_open`` —— 连续失败达到阈值且仍在熔断冷却期；
4. ``backoff`` —— 单次失败后的指数退避尚未到期；
5. ``complete`` —— 没有未确认区间、且游标已追平（无新增工作）；
6. ``global_budget_exhausted`` / ``source_quota_exhausted`` —— 无可用额度；
7. ``reserved_budget`` —— 额度被其他机构的“保底预留”挡住。

通过门槛后按下列**固定优先级元组**升序排序（越小越先）：

``(截止紧迫度, 防饥饿位, 重试位, -机构优先级, 预期成本, -已等待轮次,
截止剩余秒, 名称)``

其中：

* **截止紧迫度**：
  * ``0`` = 剩余工作量仍可能赶上、且已进入临界窗口 —— 截止**高于**防饥饿与优先级；
  * ``1`` = 仍可能赶上、处于预警窗口 —— 只作为提示，防饥饿优先；
  * ``2`` = 无截止，或剩余工作量已不可能在天内完成（按 ``backlog / batch_limit
    × round_seconds`` 估算）—— 退回普通优先级竞争，避免“永远做不完的截止”
    无限抢占、饿死别家。
* **防饥饿位** 0=已连续等待 ``starvation_rounds`` 轮（或超过等待秒数），
  1=正常。防饥饿**高于**机构优先级，但**不高于**紧迫截止：
  长期被高优先级挤掉的机构一定会被捞起，同时又不会把本可赶上的截止挤掉。
* **重试位** 0=存在未确认区间（上次失败/部分成功留下的债务），1=没有。
  未确认的旧区间先补，避免“发过请求就算完成”的假进度。
* **机构优先级** 数值越大越先。
* **预期成本** 预期能命中的条件请求更便宜（完整缓存命中成本为 0，
  且不扣预算），同优先级时先做便宜的。
* **已等待轮次** 等待越久越先，保证同级内的公平。
* **截止剩余秒** 同一紧迫度下截止更近者先做，无截止者排最后。
* 名称升序作为最后的稳定仲裁，保证完全可复现。

**保底预留**：机构可配置 ``reserve``（当天至少可用的请求数）。当总预算
吃紧时，调度器不会把预算花到“其他机构未满足的保底额度”以下；唯一例外
是截止紧迫度为 0 的机构（截止优先于保底）。若所有机构都因保底互相卡住，
本轮会放行排序最靠前的机构并记录 ``reservation_overridden``，避免死锁。

失败与熔断
----------

* 失败：游标区间保持 **未确认**，下次必须重试；失败计数 +1 并按指数退避
  设置 ``retry_at``；
* 连续失败达到 ``max_consecutive_failures``：进入熔断，冷却期内彻底跳过
  （``circuit_open``），避免单个坏接口把当天预算拖到截止；
* 冷却结束后放行一次半开探测（``half_open_probe``），成功即恢复，失败则
  以更长的冷却再次熔断；
* 部分成功：``[start, partial_until)`` 确认，剩余部分保持未确认；因为确有
  进展，不计入连续失败，但 ``reason`` 会保留 ``partial``。

旧 happy-path 的 ``process(record)`` 行为保持不变。
"""
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import budget as _budget
from . import conditional as _conditional
from . import incremental as _incremental


# ---------------------------------------------------------------- 原因常量
SKIPPED_DISABLED = "disabled"
SKIPPED_DEADLINE = "deadline_passed"
SKIPPED_CIRCUIT = "circuit_open"
SKIPPED_BACKOFF = "backoff"
SKIPPED_COMPLETE = "complete"
SKIPPED_GLOBAL_BUDGET = "global_budget_exhausted"
SKIPPED_SOURCE_QUOTA = "source_quota_exhausted"
SKIPPED_RESERVED = "reserved_budget"
SKIPPED_ROUND_CAP = "round_cap"
SKIPPED_BATCH_FULL = "batch_full"

DISPATCH_ADVANCE = "advance"
DISPATCH_RETRY = "retry"
DISPATCH_CACHE = "cache_hit"
DISPATCH_PROBE = "half_open_probe"

SKIP_REASONS = frozenset({
    SKIPPED_DISABLED, SKIPPED_DEADLINE, SKIPPED_CIRCUIT, SKIPPED_BACKOFF,
    SKIPPED_COMPLETE, SKIPPED_GLOBAL_BUDGET, SKIPPED_SOURCE_QUOTA,
    SKIPPED_RESERVED, SKIPPED_ROUND_CAP, SKIPPED_BATCH_FULL,
})


# ---------------------------------------------------------------- 配置与模型
@dataclass
class SchedulerConfig:
    """调度器全局参数。"""

    #: 距截止多久内算“预警”（秒）
    warn_window: float = 7200.0
    #: 距截止多久内算“紧迫/会错过”（秒）
    critical_window: float = 1800.0
    #: 估算每次派发所需时间（秒），用于判断截止是否来得及
    round_seconds: float = 5.0
    #: 连续多少轮未被派发即触发防饥饿
    starvation_rounds: int = 3
    #: 可选的按秒防饥饿阈值（None 表示只看轮次）
    starvation_seconds: float | None = None
    #: 单轮内每个机构最多派发几次
    max_per_source: int = 1
    #: 游标未给出时，默认每批抓取量
    default_batch_limit: int = 50

    def to_dict(self) -> dict[str, Any]:
        return {
            "warn_window": self.warn_window,
            "critical_window": self.critical_window,
            "round_seconds": self.round_seconds,
            "starvation_rounds": self.starvation_rounds,
            "starvation_seconds": self.starvation_seconds,
            "max_per_source": self.max_per_source,
            "default_batch_limit": self.default_batch_limit,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "SchedulerConfig":
        data = dict(data or {})
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in allowed})


@dataclass
class Source:
    """一个抓取来源（机构）的静态配置。"""

    name: str
    priority: float = 0.0
    #: 当天截止时间（epoch 秒）；None 表示没有单独截止
    deadline: float | None = None
    batch_limit: int = 50
    #: 当天保底请求数，用来对抗“高优先级长期霸占预算”
    reserve: int = 0
    #: 每次派发消耗多少预算单位
    cost: int = 1
    enabled: bool = True
    max_consecutive_failures: int = 3
    backoff_base: float = 30.0
    backoff_cap: float = 600.0
    quarantine_base: float = 600.0
    quarantine_cap: float = 3600.0

    def __post_init__(self):
        self.name = str(self.name)
        self.priority = float(self.priority)
        self.deadline = None if self.deadline is None else float(self.deadline)
        self.batch_limit = max(1, int(self.batch_limit))
        self.reserve = max(0, int(self.reserve))
        self.cost = max(1, int(self.cost))
        self.max_consecutive_failures = max(1, int(self.max_consecutive_failures))
        self.backoff_base = max(0.0, float(self.backoff_base))
        self.backoff_cap = max(self.backoff_base, float(self.backoff_cap))
        self.quarantine_base = max(0.0, float(self.quarantine_base))
        self.quarantine_cap = max(self.quarantine_base,
                                  float(self.quarantine_cap))

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "priority": self.priority,
            "deadline": self.deadline, "batch_limit": self.batch_limit,
            "reserve": self.reserve, "cost": self.cost,
            "enabled": self.enabled,
            "max_consecutive_failures": self.max_consecutive_failures,
            "backoff_base": self.backoff_base, "backoff_cap": self.backoff_cap,
            "quarantine_base": self.quarantine_base,
            "quarantine_cap": self.quarantine_cap,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | str) -> "Source":
        if isinstance(data, str):
            return cls(name=data)
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in dict(data).items() if k in allowed})


@dataclass
class SourceRuntime:
    """机构的运行时状态（可序列化）。"""

    dispatched: int = 0
    wait_rounds: int = 0
    consecutive_failures: int = 0
    total_failures: int = 0
    total_successes: int = 0
    cache_hits: int = 0
    cache_revalidations: int = 0
    retry_at: float | None = None
    circuit: str = "closed"           # closed | open | half-open
    circuit_open_until: float | None = None
    last_error: str = ""
    last_dispatched_at: float | None = None
    last_attempt_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "SourceRuntime":
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in dict(data or {}).items() if k in allowed})


@dataclass
class Decision:
    """一条调度决定（派发或跳过），可直接序列化审计。"""

    seq: int
    round: int
    at: float
    source: str
    action: str                    # dispatch | skip
    reason: str
    detail: str = ""
    interval: list[int] | None = None
    cost: int = 0
    expected_cost: float = 0.0
    cache: dict[str, Any] = field(default_factory=dict)
    priority: float = 0.0
    deadline_tier: int = 2
    starved: bool = False
    retry: bool = False
    wait_rounds: int = 0
    available_before: int = 0
    reserved_floor: int = 0

    @property
    def dispatched(self) -> bool:
        return self.action == "dispatch"

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Decision":
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in dict(data).items() if k in allowed})


@dataclass
class _Candidate:
    """内部评估结果。"""

    source: Source
    runtime: SourceRuntime
    interval: _incremental.Interval | None
    expectation: _conditional.CacheExpectation
    tier: int
    starved: bool
    retry: bool
    cost: int
    expected_cost: float
    floor: int
    available: int
    half_open: bool
    seconds_left: float | None = None
    skip_reason: str | None = None
    skip_detail: str = ""

    @property
    def eligible(self) -> bool:
        return self.skip_reason is None

    @property
    def rank(self) -> tuple:
        return (
            self.tier,
            0 if self.starved else 1,
            0 if self.retry else 1,
            -self.source.priority,
            round(self.expected_cost, 6),
            -self.runtime.wait_rounds,
            # 同一紧迫度下，截止更近的先做；无截止的排在最后
            float("inf") if self.seconds_left is None else round(self.seconds_left, 3),
            self.source.name,
        )


# ---------------------------------------------------------------- 归一化
def _normalize_sources(sources: Any) -> dict[str, Source]:
    if isinstance(sources, Mapping):
        items: Iterable[Any] = []
        for name, cfg in sources.items():
            if isinstance(cfg, Source):
                items.append(cfg)
            else:
                data = dict(cfg or {})
                data.setdefault("name", name)
                items.append(Source.from_dict(data))
    else:
        items = [Source.from_dict(s) for s in (sources or [])]
    out: dict[str, Source] = {}
    for src in items:
        out[src.name] = src
    return out


def _as_ledger(ledger: Any, budget_limit: int | None) -> _budget.Ledger:
    if isinstance(ledger, _budget.Ledger):
        return ledger
    if ledger is not None and not isinstance(ledger, Mapping):
        if budget_limit is None:
            return _budget.Ledger(limit=int(ledger))
    result = _budget.Ledger.from_dict(ledger if isinstance(ledger, Mapping) else None)
    if budget_limit is not None:
        result.limit = int(budget_limit)
    return result


def _as_cursors(cursors: Any) -> _incremental.CursorStore:
    if isinstance(cursors, _incremental.CursorStore):
        return cursors
    if isinstance(cursors, Mapping):
        return _incremental.CursorStore.from_dict(cursors)
    return _incremental.CursorStore()


def _as_cache(cache: Any) -> _conditional.CacheStore:
    if isinstance(cache, _conditional.CacheStore):
        return cache
    if isinstance(cache, Mapping):
        return _conditional.CacheStore.from_dict(cache)
    return _conditional.CacheStore()


# ---------------------------------------------------------------- 调度器
class Scheduler:
    """把预算、游标与缓存组合成可恢复、可解释的下一批任务。"""

    def __init__(self, sources: Any, ledger: Any = None,
                 cursors: Any = None, cache: Any = None, *,
                 config: SchedulerConfig | Mapping[str, Any] | None = None,
                 now: float | None = None, budget_limit: int | None = None,
                 day: str | None = None):
        self.sources = _normalize_sources(sources)
        self.ledger = _as_ledger(ledger, budget_limit)
        self.cursors = _as_cursors(cursors)
        self.cache = _as_cache(cache)
        self.config = (config if isinstance(config, SchedulerConfig)
                       else SchedulerConfig.from_dict(config))
        self.now = float(now) if now is not None else time.time()
        self.day = day
        self.round = 0
        self.seq = 0
        self.history: list[Decision] = []
        self.stop_reason: str | None = None
        self.runtime: dict[str, SourceRuntime] = {
            name: SourceRuntime() for name in self.sources
        }

    # ------------------------------------------------------------ 时间/日期
    def set_now(self, now: float) -> None:
        self.now = float(now)

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)

    def new_day(self, day: str | None = None, now: float | None = None) -> None:
        """翻日：清空花费与当日计数，重置熔断给机构一次新机会。"""
        if now is not None:
            self.now = float(now)
        self.day = day
        self.ledger.reset()
        self.round = 0
        self.stop_reason = None
        for rt in self.runtime.values():
            rt.dispatched = 0
            rt.wait_rounds = 0
            rt.consecutive_failures = 0
            rt.retry_at = None
            rt.circuit = "closed"
            rt.circuit_open_until = None

    def _effective_deadline(self, source: Source) -> float | None:
        return source.deadline

    # ------------------------------------------------------------- 评估
    def _deadline_projection(self, source: Source,
                             cursor: _incremental.Cursor) -> tuple[int, float]:
        """返回 (剩余条数, 预计所需秒数)。"""
        backlog = cursor.backlog() if cursor is not None else 0
        rounds_needed = math.ceil(backlog / max(1, source.batch_limit)) \
            if backlog else 0
        return backlog, rounds_needed * max(0.0, self.config.round_seconds)

    def _deadline_tier(self, source: Source, cursor: _incremental.Cursor,
                       now: float) -> int | None:
        """截止紧迫度。

        * ``0``：仍可能赶上，且已进入临界窗口 —— 截止压过防饥饿；
        * ``1``：仍可能赶上，处于预警窗口 —— 截止只是提示，防饥饿优先；
        * ``2``：无截止，或剩余工作量已不可能在天内完成（此时再以截止为由
          抢占只会饿死其他机构，因此退回普通优先级竞争）；
        * ``None``：截止已过，本轮直接跳过。
        """
        deadline = self._effective_deadline(source)
        if deadline is None:
            return 2
        left = deadline - now
        if left <= 0:
            return None
        backlog, projected = self._deadline_projection(source, cursor)
        if projected > left:
            return 2
        if left <= max(projected, self.config.critical_window):
            return 0
        if left <= self.config.warn_window:
            return 1
        return 2

    def _deadline_state(self, source: Source, cursor: _incremental.Cursor,
                        now: float) -> dict[str, Any]:
        deadline = self._effective_deadline(source)
        if deadline is None:
            return {"deadline": None, "seconds_left": None, "backlog": 0,
                    "projected_seconds": 0.0, "reachable": None}
        _, projected = self._deadline_projection(source, cursor)
        left = deadline - now
        return {
            "deadline": deadline,
            "seconds_left": left,
            "backlog": cursor.backlog() if cursor is not None else 0,
            "projected_seconds": projected,
            "reachable": projected <= left if left > 0 else False,
        }

    def _is_starved(self, rt: SourceRuntime, now: float) -> bool:
        if rt.wait_rounds >= max(1, int(self.config.starvation_rounds)):
            return True
        window = self.config.starvation_seconds
        if window is not None and rt.last_dispatched_at is not None:
            return (now - rt.last_dispatched_at) >= float(window)
        return False

    def _has_live_work(self, source: Source, rt: SourceRuntime, now: float) -> bool:
        if not source.enabled:
            return False
        cursor = self.cursors.get(source.name)
        if cursor.next_interval(source.batch_limit) is None:
            return False
        if self._deadline_tier(source, cursor, now) is None:
            return False
        if rt.circuit == "open" and rt.circuit_open_until is not None \
                and now < rt.circuit_open_until:
            return False
        return True

    def _reserve_floor(self, exclude: str, now: float) -> int:
        floor = 0
        for name, src in self.sources.items():
            if name == exclude:
                continue
            rt = self.runtime[name]
            if not self._has_live_work(src, rt, now):
                continue
            floor += max(0, int(src.reserve) - int(rt.dispatched))
        return floor

    def _evaluate(self, name: str, now: float) -> _Candidate:
        source = self.sources[name]
        rt = self.runtime[name]
        cursor = self.cursors.get(name)
        interval = cursor.next_interval(source.batch_limit)
        expectation = _conditional.expectation(
            self.cache, name, interval.to_dict() if interval else None)
        tier = self._deadline_tier(source, cursor, now)
        expected_cost = round(float(source.cost) * expectation.cost_factor, 6)
        retry = bool(cursor.unconfirmed_bounds())
        starved = self._is_starved(rt, now)
        half_open = (rt.circuit == "open" and rt.circuit_open_until is not None
                     and now >= rt.circuit_open_until)
        floor = self._reserve_floor(name, now)
        available = int(self.ledger.available(name))

        cand = _Candidate(
            source=source, runtime=rt, interval=interval,
            expectation=expectation, tier=tier if tier is not None else 2,
            starved=starved, retry=retry,
            cost=0 if expectation.saves_request else int(source.cost),
            expected_cost=expected_cost, floor=floor, available=available,
            half_open=half_open,
            seconds_left=(None if source.deadline is None
                          else float(source.deadline) - now),
        )

        if not source.enabled:
            return self._skip(cand, SKIPPED_DISABLED, "source disabled")
        if tier is None:
            return self._skip(cand, SKIPPED_DEADLINE,
                              f"deadline {self._effective_deadline(source)} passed")
        if rt.circuit == "open" and not half_open:
            left = (rt.circuit_open_until or now) - now
            return self._skip(cand, SKIPPED_CIRCUIT,
                              f"circuit open for {max(0.0, left):.0f}s "
                              f"after {rt.consecutive_failures} failures")
        if rt.retry_at is not None and now < rt.retry_at and not half_open:
            return self._skip(cand, SKIPPED_BACKOFF,
                              f"retry in {rt.retry_at - now:.0f}s "
                              f"({rt.last_error or 'failed'})")
        if interval is None:
            return self._skip(cand, SKIPPED_COMPLETE,
                              "cursor caught up, no unconfirmed interval")
        if cand.cost > 0:
            if int(self.ledger.remaining) < cand.cost:
                return self._skip(cand, SKIPPED_GLOBAL_BUDGET,
                                  f"global remaining {int(self.ledger.remaining)} "
                                  f"< cost {cand.cost}")
            if available < cand.cost:
                return self._skip(cand, SKIPPED_SOURCE_QUOTA,
                                  f"source available {available} "
                                  f"< cost {cand.cost}")
            capacity = min(available, max(0, int(self.ledger.remaining) - floor))
            if capacity < cand.cost and cand.tier > 0:
                return self._skip(cand, SKIPPED_RESERVED,
                                  f"reserved {floor} for other sources; "
                                  f"capacity {capacity} < cost {cand.cost}")
        return cand

    @staticmethod
    def _skip(cand: _Candidate, reason: str, detail: str) -> _Candidate:
        cand.skip_reason = reason
        cand.skip_detail = detail
        return cand

    def _candidates(self, now: float) -> list[_Candidate]:
        return [self._evaluate(name, now) for name in sorted(self.sources)]

    # ------------------------------------------------------------- 选择
    def next_batch(self, n: int | None = None, *,
                   per_source: int | None = None) -> list[Decision]:
        """选择并扣账下一批任务；返回本轮全部决定（含跳过）。"""
        now = self.now
        per_source = max(1, int(per_source if per_source is not None
                                else self.config.max_per_source))
        if n is None:
            n = max(1, len(self.sources))
        self.round += 1
        for rt in self.runtime.values():
            rt.wait_rounds += 1

        candidates = self._candidates(now)
        decisions: list[Decision] = []
        emitted: set[int] = set()
        dispatched = 0
        counts: dict[str, int] = {}
        blocked: dict[int, tuple[str, str]] = {}

        eligible = sorted((c for c in candidates if c.eligible),
                          key=lambda c: c.rank)

        # 一次批次里每个机构最多派发一次：下一段区间要等本次结果落库后
        # 才能确定，否则会凭空跳过一段数据。``per_source`` 只是把上限压得更低。
        for cand in eligible:
            if dispatched >= n:
                break
            name = cand.source.name
            if counts.get(name, 0) >= per_source:
                blocked[id(cand)] = (
                    SKIPPED_ROUND_CAP,
                    f"per-source cap {per_source} reached this round")
                continue
            available = int(self.ledger.available(name))
            capacity = min(available,
                           max(0, int(self.ledger.remaining) - cand.floor))
            cost = cand.cost
            if cost > 0:
                if available < cost:
                    reason = (SKIPPED_GLOBAL_BUDGET
                              if int(self.ledger.remaining) <= 0
                              else SKIPPED_SOURCE_QUOTA)
                    blocked[id(cand)] = (reason,
                                         f"available {available} < cost {cost}")
                    continue
                if capacity < cost and cand.tier > 0:
                    blocked[id(cand)] = (
                        SKIPPED_RESERVED,
                        f"reserved {cand.floor} for other sources; "
                        f"capacity {capacity} < cost {cost}")
                    continue
                self.ledger.charge(name, cost, reason="dispatch", at=now)
            decisions.append(self._record_dispatch(cand, now, cost))
            emitted.add(id(cand))
            counts[name] = counts.get(name, 0) + 1
            dispatched += 1

        # 保底互锁时（所有机构都在等别人让出额度）放行最靠前者，避免死锁。
        if dispatched == 0 and n > 0:
            reserved = [c for c in candidates
                        if blocked.get(id(c), (c.skip_reason,))[0]
                        == SKIPPED_RESERVED]
            if reserved:
                cand = min(reserved, key=lambda c: c.rank)
                available = int(self.ledger.available(cand.source.name))
                cost = cand.cost
                if cost == 0 or available >= cost:
                    if cost > 0:
                        self.ledger.charge(cand.source.name, cost,
                                           reason="dispatch", at=now)
                    decisions.append(self._record_dispatch(
                        cand, now, cost, overridden=cand.tier > 0))
                    emitted.add(id(cand))
                    blocked.pop(id(cand), None)
                    counts[cand.source.name] = \
                        counts.get(cand.source.name, 0) + 1
                    dispatched += 1

        # 未派发的候选统一补一条可审计的跳过原因（用派发后的最新额度解释）。
        for cand in candidates:
            key = id(cand)
            if key in emitted:
                continue
            if key in blocked:
                reason, detail = blocked[key]
            elif cand.skip_reason is not None:
                reason, detail = cand.skip_reason, cand.skip_detail
            elif dispatched >= n:
                reason, detail = (SKIPPED_BATCH_FULL,
                                  f"batch size {n} already filled this round")
            elif counts.get(cand.source.name, 0) >= per_source:
                reason, detail = (SKIPPED_ROUND_CAP,
                                  f"per-source cap {per_source} reached "
                                  f"this round")
            else:
                reason, detail = (SKIPPED_ROUND_CAP,
                                  "not selected this round")
            decisions.append(self._record_skip(cand, now, reason, detail))

        self.history.extend(decisions)
        if dispatched:
            self.stop_reason = None
        else:
            self.stop_reason = self._stop_reason(decisions)
        return decisions

    def next_task(self) -> Decision | None:
        """只取一个任务；跳过项仍会进入历史。"""
        batch = self.next_batch(1)
        return next((d for d in batch if d.dispatched), None)

    def _stop_reason(self, decisions: list[Decision]) -> str:
        reasons = {d.reason for d in decisions if d.action == "skip"}
        for priority_reason in (SKIPPED_CIRCUIT, SKIPPED_DEADLINE,
                                SKIPPED_GLOBAL_BUDGET,
                                SKIPPED_SOURCE_QUOTA, SKIPPED_COMPLETE,
                                SKIPPED_DISABLED, SKIPPED_BACKOFF,
                                SKIPPED_RESERVED):
            if priority_reason in reasons:
                return priority_reason
        return "no_task"

    def _base_decision(self, cand: _Candidate, now: float, action: str,
                       reason: str, detail: str, **extra: Any) -> Decision:
        self.seq += 1
        data = dict(
            seq=self.seq, round=self.round, at=now,
            source=cand.source.name, action=action, reason=reason,
            detail=detail,
            interval=None if cand.interval is None else
            [cand.interval.start, cand.interval.end],
            cost=0, expected_cost=cand.expected_cost,
            cache=cand.expectation.to_dict(),
            priority=cand.source.priority, deadline_tier=cand.tier,
            starved=cand.starved, retry=cand.retry,
            wait_rounds=cand.runtime.wait_rounds,
            available_before=cand.available, reserved_floor=cand.floor,
        )
        data.update(extra)
        return Decision(**data)

    def _record_skip(self, cand: _Candidate, now: float, reason: str,
                     detail: str) -> Decision:
        return self._base_decision(cand, now, "skip", reason, detail)

    def _record_dispatch(self, cand: _Candidate, now: float, cost: int,
                         overridden: bool = False) -> Decision:
        if cand.expectation.saves_request:
            reason = DISPATCH_CACHE
            detail = "served from cache, no request charged"
        elif cand.half_open:
            reason = DISPATCH_PROBE
            detail = "half-open probe after circuit cooldown"
        elif cand.retry:
            reason = DISPATCH_RETRY
            detail = "retrying unconfirmed interval"
        else:
            reason = DISPATCH_ADVANCE
            detail = "advancing cursor"
        if overridden:
            detail += "; reservation overridden to avoid deadlock"
        rt = cand.runtime
        rt.dispatched += 1
        rt.wait_rounds = 0
        rt.last_dispatched_at = now
        if cand.half_open:
            rt.circuit = "half-open"
        if cand.expectation.saves_request:
            rt.cache_hits += 1
        elif cand.expectation.expected_hit:
            rt.cache_revalidations += 1
        return self._base_decision(
            cand, now, "dispatch", reason, detail, cost=cost,
            available_before=cand.available)

    # ------------------------------------------------------------- 结果回填
    def complete(self, decision: Decision | Mapping[str, Any], *,
                 fetched: int | None = None,
                 partial_until: int | None = None,
                 at: float | None = None) -> _incremental.Interval | None:
        """标记派发成功（或部分成功），推进游标。

        ``partial_until`` 给出时只确认 ``[start, partial_until)``，
        剩余部分保持未确认以便下次重试。
        """
        d = decision if isinstance(decision, Decision) else Decision.from_dict(decision)
        if d.interval is None:
            return None
        interval = _incremental.Interval(int(d.interval[0]), int(d.interval[1]))
        rt = self.runtime[d.source]
        cursor = self.cursors.get(d.source)
        when = at if at is not None else self.now

        if d.reason == DISPATCH_CACHE:
            cursor.confirm(interval, at=when)
            confirmed = interval
        elif partial_until is not None and int(partial_until) < interval.end:
            cursor.complete(interval, int(partial_until),
                            reason="partial", at=when)
            confirmed = _incremental.Interval(interval.start,
                                              max(interval.start,
                                                  int(partial_until)))
        else:
            cursor.confirm(interval, at=when)
            confirmed = interval

        if d.reason == DISPATCH_CACHE:
            self.cache.record_hit(d.source, interval.to_dict(),
                                  revalidated=False)
        elif confirmed:
            # 只缓存真正确认过的前缀，别把未确认部分伪装成命中。
            self.cache.put(d.source, confirmed.to_dict(),
                           body={"fetched": fetched}, stored_at=when)

        rt.consecutive_failures = 0
        rt.total_successes += 1
        rt.retry_at = None
        rt.circuit = "closed"
        rt.circuit_open_until = None
        rt.last_attempt_at = when
        self._annotate(d, "completed" if partial_until is None else "partial",
                       at=when)
        return interval

    #: 别名
    succeed = complete
    record_success = complete

    def fail(self, decision: Decision | Mapping[str, Any], reason: str = "failed",
             *, at: float | None = None) -> None:
        """标记派发失败：区间保持未确认，累计失败并设置退避/熔断。"""
        d = decision if isinstance(decision, Decision) else Decision.from_dict(decision)
        when = at if at is not None else self.now
        rt = self.runtime[d.source]
        source = self.sources[d.source]
        cursor = self.cursors.get(d.source)
        if d.interval is not None:
            cursor.fail(_incremental.Interval(int(d.interval[0]),
                                              int(d.interval[1])),
                        reason=reason, at=when)
        if d.reason == DISPATCH_CACHE:
            self.cache.invalidate(d.source,
                                  None if d.interval is None
                                  else {"start": d.interval[0],
                                        "end": d.interval[1]})
        rt.consecutive_failures += 1
        rt.total_failures += 1
        rt.last_error = reason
        rt.last_attempt_at = when
        backoff = min(source.backoff_base * (2 ** (rt.consecutive_failures - 1)),
                      source.backoff_cap)
        rt.retry_at = when + backoff
        if rt.consecutive_failures >= source.max_consecutive_failures:
            level = rt.consecutive_failures - source.max_consecutive_failures
            cooldown = min(source.quarantine_base * (2 ** level),
                           source.quarantine_cap)
            rt.circuit = "open"
            rt.circuit_open_until = when + cooldown
        self._annotate(d, f"failed:{reason}", at=when)

    def record_partial(self, decision: Decision | Mapping[str, Any],
                       until: int, reason: str = "partial",
                       *, at: float | None = None) -> _incremental.Interval | None:
        """部分成功：确认前缀，其余留作未确认。"""
        return self.complete(decision, partial_until=int(until), at=at)

    #: 别名
    partial = record_partial

    def _annotate(self, d: Decision, outcome: str, at: float) -> None:
        """把结果写回历史记录，便于重启后追查。"""
        for item in self.history:
            if item.seq == d.seq:
                item.detail = f"{item.detail}; outcome={outcome} @ {at:.0f}".strip("; ")
                return

    # ------------------------------------------------------------- 视图/审计
    def explain(self) -> list[dict[str, Any]]:
        """解释当前排序与每个机构会被派发/跳过的原因（只读）。"""
        now = self.now
        rows = []
        for cand in self._candidates(now):
            if cand.eligible and cand.half_open:
                dispatch_reason = DISPATCH_PROBE
            elif cand.eligible and cand.expectation.saves_request:
                dispatch_reason = DISPATCH_CACHE
            elif cand.eligible and cand.retry:
                dispatch_reason = DISPATCH_RETRY
            elif cand.eligible:
                dispatch_reason = DISPATCH_ADVANCE
            else:
                dispatch_reason = cand.skip_reason
            rows.append({
                "source": cand.source.name,
                "action": "dispatch" if cand.eligible else "skip",
                "reason": dispatch_reason,
                "detail": cand.skip_detail if not cand.eligible else "",
                "rank": list(cand.rank),
                "priority": cand.source.priority,
                "deadline_tier": cand.tier,
                "starved": cand.starved,
                "retry": cand.retry,
                "half_open": cand.half_open,
                "wait_rounds": cand.runtime.wait_rounds,
                "interval": None if cand.interval is None else
                [cand.interval.start, cand.interval.end],
                "expected_cost": cand.expected_cost,
                "cache": cand.expectation.to_dict(),
                "available": cand.available,
                "reserved_floor": cand.floor,
            })
        rows.sort(key=lambda r: (r["action"] != "dispatch", r["rank"]))
        return rows

    def deferrals(self, round_no: int | None = None) -> list[dict[str, Any]]:
        """跳过记录（可序列化），用于追查“为什么跳过某机构”。"""
        return [d.to_dict() for d in self.deferral_decisions(round_no)]

    def deferral_decisions(self, round_no: int | None = None) -> list[Decision]:
        """``deferrals`` 的 ``Decision`` 版本，便于继续断言/回填。"""
        return [d for d in self.history
                if d.action == "skip" and (round_no is None or d.round == round_no)]

    def last_round(self) -> list[dict[str, Any]]:
        """最近一轮的全部决定（可序列化）。"""
        return [d.to_dict() for d in self.last_round_decisions()]

    def last_round_decisions(self) -> list[Decision]:
        return [d for d in self.history if d.round == self.round]

    def coverage(self) -> dict[str, dict[str, Any]]:
        """各机构的完成覆盖情况。"""
        out = {}
        for name in sorted(self.sources):
            cur = self.cursors.get(name)
            head = cur.head
            ratio = None
            if head:
                ratio = round(min(1.0, int(cur.committed) / int(head)), 4)
            out[name] = {
                "committed": int(cur.committed),
                "head": head,
                "pending": cur.unconfirmed_size(),
                "unconfirmed": [i.to_dict() for i in cur.unconfirmed_bounds()],
                "backlog": cur.backlog(),
                "coverage": ratio,
                "dispatched": self.runtime[name].dispatched,
                "failures": self.runtime[name].total_failures,
                "circuit": self.runtime[name].circuit,
            }
        return out

    def requests_used(self) -> int:
        """已花预算（请求数）。"""
        return int(self.ledger.total_spent)

    def deadline_state(self, source: str) -> dict[str, Any]:
        """某机构的截止可达性（为何紧迫/为何放弃截止）。"""
        src = self.sources[source]
        return self._deadline_state(src, self.cursors.get(source), self.now)

    def deadline_report(self) -> dict[str, dict[str, Any]]:
        return {name: self._deadline_state(src, self.cursors.get(name), self.now)
                for name, src in sorted(self.sources.items())}

    # ------------------------------------------------------------- 序列化
    def to_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "now": self.now,
            "day": self.day,
            "round": self.round,
            "seq": self.seq,
            "stop_reason": self.stop_reason,
            "config": self.config.to_dict(),
            "sources": [s.to_dict() for _, s in sorted(self.sources.items())],
            "runtime": {k: v.to_dict() for k, v in sorted(self.runtime.items())},
            "ledger": self.ledger.to_dict(),
            "cursors": self.cursors.to_dict(),
            "cache": self.cache.to_dict(),
            "history": [d.to_dict() for d in self.history],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], *,
                  sources: Any = None, now: float | None = None
                  ) -> "Scheduler":
        """从检查点恢复；``sources`` 可覆盖配置，``now`` 可注入时钟。"""
        data = dict(data or {})
        restored_sources = sources if sources is not None else data.get("sources")
        scheduler = cls(
            restored_sources or [],
            ledger=_budget.Ledger.from_dict(data.get("ledger")),
            cursors=_incremental.CursorStore.from_dict(data.get("cursors")),
            cache=_conditional.CacheStore.from_dict(data.get("cache")),
            config=SchedulerConfig.from_dict(data.get("config")),
            now=now if now is not None else data.get("now"),
            day=data.get("day"),
        )
        scheduler.round = int(data.get("round", 0))
        scheduler.seq = int(data.get("seq", 0))
        scheduler.stop_reason = data.get("stop_reason")
        runtime = data.get("runtime") or {}
        for name, rt in scheduler.runtime.items():
            if name in runtime:
                scheduler.runtime[name] = SourceRuntime.from_dict(runtime[name])
        scheduler.history = [Decision.from_dict(d)
                             for d in (data.get("history") or [])]
        return scheduler

    def save(self, path: str | os.PathLike[str]) -> Path:
        """原子写入检查点，进程重启后可原样恢复。"""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        tmp.write_text(json.dumps(self.to_dict(), ensure_ascii=False,
                                  sort_keys=True), encoding="utf-8")
        os.replace(tmp, target)
        return target

    @classmethod
    def load(cls, path: str | os.PathLike[str], *, sources: Any = None,
             now: float | None = None) -> "Scheduler":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(data, sources=sources, now=now)


# ---------------------------------------------------------------- 便捷入口
def make_scheduler(sources: Any, budget_limit: int | None = None, *,
                   ledger: Any = None, cursors: Any = None, cache: Any = None,
                   config: SchedulerConfig | Mapping[str, Any] | None = None,
                   now: float | None = None) -> Scheduler:
    return Scheduler(sources, ledger=ledger, cursors=cursors, cache=cache,
                     config=config, now=now, budget_limit=budget_limit)


def plan(scheduler: Scheduler, n: int | None = None) -> list[dict[str, Any]]:
    """一次性取一批并返回可序列化决定。"""
    return [d.to_dict() for d in scheduler.next_batch(n)]


# ------------------------------------------------------------------ 流水线阶段
def process(record):
    """旧 happy-path 阶段：只做记录变换，不产生调度副作用。"""
    rec = dict(record)
    rec.setdefault("_stage_scheduler", "naive")
    return rec
