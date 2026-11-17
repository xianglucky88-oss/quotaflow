import pytest

def test_conditional_is_open_gap():
    from src.quotaflow import conditional
    with pytest.raises(NotImplementedError):
        conditional.process({"id": "x"})

def test_incremental_is_open_gap():
    from src.quotaflow import incremental
    with pytest.raises(NotImplementedError):
        incremental.process({"id": "x"})
