"""单一职责：每日请求预算账本与扣账规则。

调度器只负责“选谁”，真正能不能花、花多少、还剩多少，全部由本模块裁定：

* 全局日预算 ``limit``；
* 每个机构可选的日配额 ``allocations``（缺省等于全局预算）；
* 已花费 ``spent`` 与可审计的 ``entries`` 流水；
* 序列化 / 反序列化，供调度器重启后恢复账本。

账本不变量：

1. 任意机构花费不超过其配额；
2. 所有机构花费总和不超过全局日预算；
3. ``available(source)`` 同时受全局余额与机构余额约束；
4. 退款不会把花费降为负数。

旧 happy-path 的 ``process(record)`` 行为保持不变。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


class BudgetExhausted(RuntimeError):
    """请求额度不足。由 :meth:`Ledger.charge` 抛出。"""

    def __init__(self, source: str | None = None, requested: int = 0,
                 available: int = 0):
        self.source = source
        self.requested = requested
        self.available = available
        super().__init__(
            f"budget exhausted for {source!r}: requested {requested}, "
            f"available {available}"
        )


class Count(int):
    """``int`` 子类，既可当值用，也可当零参方法调用（``x`` / ``x()`` 都成立）。"""

    def __call__(self) -> int:  # pragma: no cover - 兼容性糖
        return int(self)


@dataclass
class Entry:
    """一条扣账/退款流水。"""

    source: str
    amount: int
    reason: str = "dispatch"
    at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"source": self.source, "amount": self.amount,
                "reason": self.reason, "at": self.at}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Entry":
        return cls(source=str(data.get("source", "")),
                   amount=int(data.get("amount", 0)),
                   reason=str(data.get("reason", "dispatch")),
                   at=data.get("at"))


@dataclass
class Ledger:
    """每日预算账本。"""

    limit: int = 0
    allocations: dict[str, int] = field(default_factory=dict)
    spent: dict[str, int] = field(default_factory=dict)
    entries: list[Entry] = field(default_factory=list)

    # ------------------------------------------------------------------ 查询
    @property
    def total_spent(self) -> Count:
        return Count(sum(int(v) for v in self.spent.values()))

    #: 兼容 ``ledger.remaining`` 与 ``ledger.remaining()`` 两种写法
    @property
    def remaining(self) -> Count:
        return Count(max(0, int(self.limit) - int(self.total_spent)))

    def allocation_for(self, source: str) -> int:
        """机构配额；未单独分配时继承全局日预算。"""
        return int(self.allocations.get(source, self.limit))

    def spent_by(self, source: str) -> Count:
        return Count(int(self.spent.get(source, 0)))

    def available(self, source: str | None = None) -> Count:
        """可用额度。

        ``source=None`` 时只看全局余额；指定机构时取
        ``min(全局余额, 机构配额 - 机构已花费)``。
        """
        global_left = int(self.remaining)
        if source is None:
            return Count(global_left)
        source_left = max(0, self.allocation_for(source) - int(self.spent_by(source)))
        return Count(min(global_left, source_left))

    def can_afford(self, source: str, amount: int = 1) -> bool:
        return int(amount) > 0 and int(self.available(source)) >= int(amount)

    # ------------------------------------------------------------------ 扣账
    def charge(self, source: str, amount: int = 1, reason: str = "dispatch",
               at: float | None = None) -> bool:
        """扣账；额度不足抛 :class:`BudgetExhausted`。"""
        amount = int(amount)
        if amount <= 0:
            raise ValueError("charge amount must be positive")
        left = int(self.available(source))
        if amount > left:
            raise BudgetExhausted(source, amount, left)
        self.spent[source] = int(self.spent.get(source, 0)) + amount
        self.entries.append(Entry(source, amount, reason, at))
        return True

    def try_charge(self, source: str, amount: int = 1, reason: str = "dispatch",
                   at: float | None = None) -> bool:
        """扣账；额度不足返回 ``False``（不抛异常）。"""
        try:
            return self.charge(source, amount, reason, at)
        except BudgetExhausted:
            return False

    #: 常用别名
    spend = charge
    consume = charge
    try_spend = try_charge
    try_consume = try_charge

    def refund(self, source: str, amount: int = 1, reason: str = "refund",
               at: float | None = None) -> int:
        """退款/回滚，返回实际退回的数量。"""
        amount = max(0, int(amount))
        current = int(self.spent.get(source, 0))
        back = min(current, amount)
        if back:
            self.spent[source] = current - back
            self.entries.append(Entry(source, -back, reason, at))
        return back

    def allocate(self, source: str, amount: int | None) -> None:
        """设置机构日配额；``None`` 表示继承全局预算。"""
        if amount is None:
            self.allocations.pop(source, None)
        else:
            self.allocations[source] = max(0, int(amount))

    set_allocation = allocate

    def reset(self, source: str | None = None) -> None:
        """清空花费（测试/新的一天使用）。"""
        if source is None:
            self.spent.clear()
        else:
            self.spent.pop(source, None)

    def usage(self) -> dict[str, dict[str, int]]:
        names = set(self.allocations) | set(self.spent)
        return {
            name: {
                "allocation": self.allocation_for(name),
                "spent": int(self.spent_by(name)),
                "available": int(self.available(name)),
            }
            for name in sorted(names)
        }

    # -------------------------------------------------------------- 序列化
    def to_dict(self) -> dict[str, Any]:
        return {
            "limit": int(self.limit),
            "allocations": {k: int(v) for k, v in sorted(self.allocations.items())},
            "spent": {k: int(v) for k, v in sorted(self.spent.items())},
            "entries": [e.to_dict() for e in self.entries],
            "total_spent": int(self.total_spent),
            "remaining": int(self.remaining),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "Ledger":
        if not data:
            return cls()
        return cls(
            limit=int(data.get("limit", 0)),
            allocations={str(k): int(v)
                         for k, v in (data.get("allocations") or {}).items()},
            spent={str(k): int(v) for k, v in (data.get("spent") or {}).items()},
            entries=[Entry.from_dict(e) for e in (data.get("entries") or [])],
        )


# 兼容常见命名
Budget = Ledger
BudgetLedger = Ledger


# ------------------------------------------------------------------ 便捷函数
def _as_ledger(ledger: Ledger | Mapping[str, Any] | None) -> Ledger:
    if isinstance(ledger, Ledger):
        return ledger
    if isinstance(ledger, Mapping):
        return Ledger.from_dict(ledger)
    return Ledger()


def new_ledger(limit: int, allocations: Mapping[str, int] | None = None) -> Ledger:
    return Ledger(limit=int(limit),
                  allocations={str(k): int(v)
                               for k, v in (allocations or {}).items()})


def remaining(ledger: Ledger | Mapping[str, Any]) -> Count:
    return _as_ledger(ledger).remaining


def available(ledger: Ledger | Mapping[str, Any], source: str | None = None) -> Count:
    return _as_ledger(ledger).available(source)


def can_afford(ledger: Ledger | Mapping[str, Any], source: str,
               amount: int = 1) -> bool:
    return _as_ledger(ledger).can_afford(source, amount)


def charge(ledger: Ledger | Mapping[str, Any], source: str, amount: int = 1,
           reason: str = "dispatch", at: float | None = None) -> bool:
    return _as_ledger(ledger).charge(source, amount, reason, at)


def refund(ledger: Ledger | Mapping[str, Any], source: str, amount: int = 1,
           reason: str = "refund", at: float | None = None) -> int:
    return _as_ledger(ledger).refund(source, amount, reason, at)


consume = charge
spend = charge


# ------------------------------------------------------------------ 流水线阶段
def process(record):
    """旧 happy-path 阶段：只做记录变换，不产生扣账副作用。"""
    rec = dict(record)
    rec.setdefault("_stage_budget", "ok")
    return rec
