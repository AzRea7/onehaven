# backend/tests/test_hqs_summary.py
from __future__ import annotations

from backend.app.domain.compliance.hqs import summarize_items


class DummyItem:
    def __init__(self, status: str):
        self.status = status


def test_summarize_items_done_failed_ratio():
    items = [
        DummyItem("done"),
        DummyItem("done"),
        DummyItem("todo"),
        DummyItem("failed"),
    ]
    s = summarize_items(items, latest_inspection_passed=True)
    assert s.total == 4
    assert s.done == 2
    assert s.failed == 1
    assert s.pct_done == 0.5
    assert s.passed is False


def test_summarize_items_pass_rule_requires_inspection():
    items = [DummyItem("done")] * 20
    s = summarize_items(items, latest_inspection_passed=False)
    assert s.pct_done == 1.0
    assert s.passed is False  # requires inspection passed