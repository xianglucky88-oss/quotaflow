"""增量游标：未确认区间、部分成功与恢复。"""
from src.quotaflow import incremental


def test_confirmed_attempt_advances_cursor():
    cur = incremental.Cursor(source="a", head=10)
    cur.confirm(incremental.Interval(0, 5))
    assert cur.committed == 5
    assert cur.next_interval(5) == incremental.Interval(5, 10)
    assert cur.is_caught_up() is False
    cur.confirm(incremental.Interval(5, 10))
    assert cur.is_caught_up() is True


def test_failed_attempt_stays_unconfirmed():
    cur = incremental.Cursor(source="a", head=10)
    cur.fail((0, 5), reason="503")
    assert cur.committed == 0
    assert cur.unconfirmed_bounds() == [incremental.Interval(0, 5)]
    # 失败不会让游标前进，下次仍然抓同一段
    assert cur.next_interval(5) == incremental.Interval(0, 5)


def test_partial_success_confirms_prefix_only():
    cur = incremental.Cursor(source="a", head=10)
    cur.start_attempt((0, 5))
    cur.complete((0, 5), partial_until=2)
    assert cur.committed == 2
    assert cur.unconfirmed_bounds() == [incremental.Interval(2, 5)]
    assert cur.next_interval(5) == incremental.Interval(2, 5)


def test_gap_inside_confirmed_range_is_still_pending():
    cur = incremental.Cursor(source="a", head=20)
    cur.fail((5, 10), reason="boom")
    cur.confirm((0, 5))
    cur.confirm((10, 15))
    # committed 只到 5，不能因为后面确认了 15 就跳过 5-10 的缺口
    assert cur.committed == 5
    assert cur.next_interval() == incremental.Interval(5, 10)


def test_backlog_with_head_unknown_uses_unconfirmed():
    cur = incremental.Cursor(source="a", head=None)
    assert cur.next_interval() is None
    cur.fail((3, 8))
    assert cur.backlog() == 5
    assert cur.next_interval() == incremental.Interval(3, 8)


def test_confirm_beyond_committed_does_not_jump():
    cur = incremental.Cursor(source="a", head=30)
    cur.confirm((10, 20))     # 缺口未补，不应推进
    assert cur.committed == 0
    cur.confirm((0, 10))
    # 缺口补齐后，之前确认过的 10-20 可以连续接上
    assert cur.committed == 20
    assert cur.next_interval(5) == incremental.Interval(20, 25)


def test_cursor_roundtrip():
    cur = incremental.Cursor(source="a", head=10)
    cur.fail((0, 5), reason="503", at=1.0)
    cur.confirm((5, 10), at=2.0)
    data = cur.to_dict()
    restored = incremental.Cursor.from_dict(data)
    assert restored.to_dict() == data
    # 0-5 仍未确认，水位不能越过缺口
    assert restored.committed == 0
    assert restored.unconfirmed_bounds() == [incremental.Interval(0, 5)]


def test_cursor_store_roundtrip():
    store = incremental.CursorStore()
    store.get("a").fail((0, 3))
    store.get("b").confirm((0, 4))
    restored = incremental.CursorStore.from_dict(store.to_dict())
    assert restored.sources() == ["a", "b"]
    assert restored.get("b").committed == 4


def test_interval_helpers():
    assert incremental.merge([(0, 3), (2, 5), (7, 9)]) == [
        incremental.Interval(0, 5), incremental.Interval(7, 9)]
    assert incremental.subtract([(0, 10)], [(3, 5)]) == [
        incremental.Interval(0, 3), incremental.Interval(5, 10)]
    assert incremental.Interval(0, 4).overlaps(incremental.Interval(3, 8))
    assert not incremental.Interval(0, 4).overlaps(incremental.Interval(4, 8))


def test_happy_path_process_unchanged():
    rec = {"id": "r-1"}
    out = incremental.process(rec)
    assert out["_stage_incremental"] == "ok"
    assert rec == {"id": "r-1"}


def test_complete_with_attempt_object_does_not_leave_duplicate():
    cur = incremental.Cursor(source="a", head=10)
    attempt = cur.start_attempt((0, 6))
    cur.complete(attempt, partial_until=3)
    assert cur.committed == 3
    # 只剩 [3,6)，不应残留整段 [0,6)
    assert cur.unconfirmed_bounds() == [incremental.Interval(3, 6)]
    assert len(cur.attempts) == 3   # 原尝试 + 确认段 + 残段


def test_retry_after_partial_closes_the_gap():
    cur = incremental.Cursor(source="a", head=10)
    cur.complete((0, 5), partial_until=2)
    assert cur.next_interval(5) == incremental.Interval(2, 5)
    cur.confirm(cur.next_interval(5))
    assert cur.committed == 5
    assert cur.unconfirmed_bounds() == []
