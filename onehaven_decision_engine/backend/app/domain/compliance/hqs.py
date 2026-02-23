# onehaven_decision_engine/backend/app/domain/compliance/hqs.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Tuple

from ...models import PropertyChecklist, PropertyChecklistItem


@dataclass(frozen=True)
class HQSRunSummary:
    property_id: int
    checklist_id: int
    total: int
    passed: int
    failed: int
    todo: int
    score_pct: float
    fail_codes: list[str]
    todo_codes: list[str]


def summarize_checklist(checklist: PropertyChecklist, items: Iterable[PropertyChecklistItem]) -> HQSRunSummary:
    items_list = list(items)
    total = len(items_list)

    passed_items = [i for i in items_list if (i.status or "").lower() == "pass"]
    failed_items = [i for i in items_list if (i.status or "").lower() == "fail"]
    todo_items = [i for i in items_list if (i.status or "").lower() in ("todo", "", None)]

    passed = len(passed_items)
    failed = len(failed_items)
    todo = len(todo_items)

    # Score is simple and deterministic:
    # - "pass" counts as done
    # - fail/todo count against
    score_pct = 0.0
    if total > 0:
        score_pct = round((passed / total) * 100.0, 2)

    fail_codes = sorted([i.code for i in failed_items if i.code])
    todo_codes = sorted([i.code for i in todo_items if i.code])

    return HQSRunSummary(
        property_id=checklist.property_id,
        checklist_id=checklist.id,
        total=total,
        passed=passed,
        failed=failed,
        todo=todo,
        score_pct=score_pct,
        fail_codes=fail_codes,
        todo_codes=todo_codes,
    )