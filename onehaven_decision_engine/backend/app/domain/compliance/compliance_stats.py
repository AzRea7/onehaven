# backend/app/domain/compliance/compliance_stats.py
from __future__ import annotations

from typing import Any, Iterable

from .top_fail_points import top_fail_points


def compliance_stats(rows: Iterable[Any]) -> dict[str, Any]:
    """
    Lightweight, stable stats rollup for inspections/compliance dashboards.

    This deliberately avoids depending on a specific DB model shape.
    It works with:
      - InspectionEvent-like dicts: {"status": "...", "fail_items_json": "..."}
      - Inspection-like dicts: {"passed": True/False, "items": [...]}
      - Mixed sources

    Expand later:
      - avg days_to_resolve
      - reinspection rates
      - severity-weighted failure index
      - pass-rate by inspector / by city / by property
    """
    rows = list(rows or [])

    total = len(rows)
    passed = 0
    failed = 0

    for r in rows:
        if not isinstance(r, dict):
            continue

        # Common shapes:
        # - {"passed": True/False}
        # - {"status": "passed"/"failed"/"scheduled"}
        if r.get("passed") is True:
            passed += 1
            continue
        if r.get("passed") is False:
            failed += 1
            continue

        status = str(r.get("status") or "").strip().lower()
        if status == "passed":
            passed += 1
        elif status == "failed":
            failed += 1

    return {
        "total_events": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": (passed / total) if total else None,
        "top_fail_points": top_fail_points(rows, limit=10),
    }