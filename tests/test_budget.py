"""预算账本：扣账规则、机构配额与序列化。"""
import pytest

from src.quotaflow import budget


def test_global_and_per_source_limits():
    ledger = budget.Ledger(limit=10, allocations={"a": 4, "b": 10})
    assert ledger.available("a") == 4
    assert ledger.available("b") == 10
    assert ledger.available() == 10

    assert ledger.charge("a", 4) is True
    assert ledger.available("a") == 0
    assert ledger.available() == 6
    assert ledger.can_afford("a") is False
    with pytest.raises(budget.BudgetExhausted):
        ledger.charge("a", 1)


def test_global_budget_is_shared():
    ledger = budget.Ledger(limit=5)
    for _ in range(5):
        assert ledger.charge("a", 1)
    assert int(ledger.remaining) == 0
    assert ledger.can_afford("b") is False
    with pytest.raises(budget.BudgetExhausted):
        ledger.charge("b", 1)


def test_unallocated_source_inherits_global_limit():
    ledger = budget.Ledger(limit=7, allocations={"a": 2})
    assert ledger.allocation_for("a") == 2
    assert ledger.allocation_for("unknown") == 7


def test_try_charge_and_refund():
    ledger = budget.Ledger(limit=2)
    assert ledger.try_charge("a") is True
    assert ledger.try_charge("a") is True
    assert ledger.try_charge("a") is False
    assert ledger.refund("a") == 1
    assert int(ledger.remaining) == 1
    # 退款不会变成负数
    assert ledger.refund("a", 10) == 1
    assert int(ledger.spent_by("a")) == 0


def test_charge_rejects_non_positive_amount():
    ledger = budget.Ledger(limit=3)
    with pytest.raises(ValueError):
        ledger.charge("a", 0)


def test_roundtrip_serialization():
    ledger = budget.Ledger(limit=10, allocations={"a": 3})
    ledger.charge("a", 2, reason="dispatch", at=123.0)
    data = ledger.to_dict()
    restored = budget.Ledger.from_dict(data)
    assert restored.to_dict() == data
    assert int(restored.remaining) == 8
    assert restored.entries[0].source == "a"
    assert restored.entries[0].at == 123.0


def test_remaining_is_value_and_callable():
    ledger = budget.Ledger(limit=4)
    ledger.charge("a", 1)
    assert int(ledger.remaining) == 3
    assert ledger.remaining() == 3


def test_new_ledger_and_module_helpers():
    ledger = budget.new_ledger(3, {"a": 1})
    assert budget.available(ledger, "a") == 1
    assert budget.charge(ledger, "a") is True
    assert budget.can_afford(ledger, "a") is False
    assert budget.remaining(ledger) == 2


def test_happy_path_process_unchanged():
    rec = {"id": "r-1", "src": "alpha"}
    out = budget.process(rec)
    assert out["_stage_budget"] == "ok"
    assert rec == {"id": "r-1", "src": "alpha"}  # 不改入参
