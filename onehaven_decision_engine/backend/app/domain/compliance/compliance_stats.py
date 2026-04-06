from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable

from .top_fail_points import top_fail_points


def _status_of(row: Any) -> str:
    if not isinstance(row, dict):
        return ""

    if row.get("passed") is True:
        return "passed"
    if row.get("passed") is False:
        return "failed"

    status = str(
        row.get("result_status")
        or row.get("inspection_status")
        or row.get("status")
        or ""
    ).strip().lower()

    if status in {"pass", "passed"}:
        return "passed"
    if status in {"fail", "failed"}:
        return "failed"
    if status in {"blocked"}:
        return "blocked"
    if status in {"not_applicable", "na", "n/a"}:
        return "not_applicable"
    if status in {"inconclusive"}:
        return "inconclusive"
    if status in {"todo", "pending", "scheduled"}:
        return "pending"
    return status


def _decode_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def compliance_stats(rows: Iterable[Any]) -> dict[str, Any]:
    """
    Property-scoped inspection/compliance stats rollup.

    Works with:
      - persisted inspection item rows
      - inspection event dicts
      - lightweight inspection summaries
    """
    rows = [row for row in (rows or []) if isinstance(row, dict)]

    total = len(rows)
    status_counter: Counter[str] = Counter()
    jurisdictions: Counter[str] = Counter()
    inspectors: Counter[str] = Counter()
    template_versions: Counter[str] = Counter()
    reinspection_needed = 0
    evidence_count = 0
    photo_count = 0

    for row in rows:
        status_counter[_status_of(row)] += 1

        jurisdiction = str(row.get("jurisdiction") or "").strip()
        if jurisdiction:
            jurisdictions[jurisdiction] += 1

        inspector = str(
            row.get("inspector")
            or row.get("inspector_name")
            or row.get("performed_by")
            or ""
        ).strip()
        if inspector:
            inspectors[inspector] += 1

        template_version = str(row.get("template_version") or "").strip()
        if template_version:
            template_versions[template_version] += 1

        if bool(row.get("requires_reinspection")):
            reinspection_needed += 1

        evidence_count += len(_decode_json_list(row.get("evidence_json")))
        photo_count += len(_decode_json_list(row.get("photo_references_json")))

    passed = status_counter["passed"]
    failed = status_counter["failed"]
    blocked = status_counter["blocked"]
    inconclusive = status_counter["inconclusive"]
    pending = status_counter["pending"]
    unresolved = failed + blocked + inconclusive + pending

    return {
        "total_events": total,
        "passed": passed,
        "failed": failed,
        "blocked": blocked,
        "inconclusive": inconclusive,
        "pending": pending,
        "not_applicable": status_counter["not_applicable"],
        "unresolved": unresolved,
        "pass_rate": (passed / total) if total else None,
        "reinspection_needed_count": reinspection_needed,
        "evidence_count": evidence_count,
        "photo_count": photo_count,
        "top_fail_points": top_fail_points(rows, limit=10),
        "jurisdictions": dict(jurisdictions.most_common()),
        "inspectors": dict(inspectors.most_common()),
        "template_versions": dict(template_versions.most_common()),
    }
