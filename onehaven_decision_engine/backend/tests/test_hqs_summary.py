# onehaven_decision_engine/backend/tests/test_hqs_summary.py
from __future__ import annotations

from backend.app.domain.compliance.hqs import summarize_checklist
from backend.app.models import PropertyChecklist, PropertyChecklistItem


def test_hqs_summary_counts_pass_fail_todo_correctly():
    checklist = PropertyChecklist(id=10, org_id=1, property_id=99, template="base_hqs")

    items = [
        PropertyChecklistItem(code="SMOKE_CO", status="pass"),
        PropertyChecklistItem(code="GFCI_BATH", status="fail"),
        PropertyChecklistItem(code="HANDRAILS", status="todo"),
        PropertyChecklistItem(code="ELECT_PANEL", status="todo"),
    ]

    s = summarize_checklist(checklist, items)

    assert s.total == 4
    assert s.passed == 1
    assert s.failed == 1
    assert s.todo == 2
    assert s.score_pct == 25.0
    assert "GFCI_BATH" in s.fail_codes
    assert "HANDRAILS" in s.todo_codes