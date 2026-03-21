from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional

from .inspection_rules import (
    normalize_inspection_item_status,
    normalize_rule_code,
    normalize_severity,
    score_readiness,
)


@dataclass(frozen=True)
class HQSSummary:
    total: int
    done: int
    failed: int
    blocked: int
    not_applicable: int
    pct_done: float
    passed: bool
    readiness_score: float
    readiness_status: str
    result_status: str


def _normalize_status(raw: Optional[str]) -> str:
    """
    Canonical checklist/compliance progression status.

    Canonical:
      - todo
      - in_progress
      - done
      - blocked
      - failed

    Supports both historical checklist statuses and inspection result statuses.
    """
    s = str(raw or "").strip().lower()

    if s in ("pass", "passed", "ok", "success"):
        return "done"
    if s in ("fail", "failed", "bad"):
        return "failed"
    if s in ("todo", "to_do", "not_started", "pending", "", "not_inspected"):
        return "todo"
    if s in ("in_progress", "doing", "wip"):
        return "in_progress"
    if s in ("blocked", "stuck", "needs_access", "cannot_verify"):
        return "blocked"
    if s in ("done", "complete", "completed"):
        return "done"
    if s in ("not_applicable", "n/a", "na"):
        return "done"

    return "todo"


def _normalize_item_status_for_summary(item: Any) -> str:
    if isinstance(item, dict):
        status = item.get("status")
        result_status = item.get("result_status")
        failed = item.get("failed")
    else:
        status = getattr(item, "status", None)
        result_status = getattr(item, "result_status", None)
        failed = getattr(item, "failed", None)

    normalized_result = normalize_inspection_item_status(result_status or status, failed=failed)

    if normalized_result == "pass":
        return "done"
    if normalized_result == "fail":
        return "failed"
    if normalized_result == "blocked":
        return "blocked"
    if normalized_result == "not_applicable":
        return "done"

    return _normalize_status(status)


def summarize_items(items: Iterable[Any], *, latest_inspection_passed: bool = False) -> HQSSummary:
    """
    Summarize checklist and inspection-aligned items.

    Passing rule:
      passed = latest_inspection_passed
               AND failed == 0
               AND blocked == 0
               AND pct_done >= 0.95
    """
    rows = list(items or [])
    total = 0
    done = 0
    failed = 0
    blocked = 0
    not_applicable = 0

    readiness_input: list[dict[str, Any]] = []

    for it in rows:
        total += 1

        if isinstance(it, dict):
            category = it.get("category")
            severity = it.get("severity")
            code = it.get("item_code") or it.get("code")
            result_status = it.get("result_status") or it.get("status")
            failed_raw = it.get("failed")
        else:
            category = getattr(it, "category", None)
            severity = getattr(it, "severity", None)
            code = getattr(it, "item_code", None) or getattr(it, "code", None)
            result_status = getattr(it, "result_status", None) or getattr(it, "status", None)
            failed_raw = getattr(it, "failed", None)

        canonical = _normalize_item_status_for_summary(it)
        normalized_result = normalize_inspection_item_status(result_status, failed=failed_raw)

        if canonical == "done":
            done += 1
        elif canonical == "failed":
            failed += 1
        elif canonical == "blocked":
            blocked += 1

        if normalized_result == "not_applicable":
            not_applicable += 1

        readiness_input.append(
            {
                "code": normalize_rule_code(code),
                "category": category,
                "status": result_status,
                "result_status": result_status,
                "severity": normalize_severity(severity),
                "failed": failed_raw,
            }
        )

    pct_done = (done / total) if total else 0.0
    readiness = score_readiness(readiness_input)
    passed = (pct_done >= 0.95) and (failed == 0) and (blocked == 0) and bool(latest_inspection_passed)

    return HQSSummary(
        total=total,
        done=done,
        failed=failed,
        blocked=blocked,
        not_applicable=not_applicable,
        pct_done=round(pct_done, 4),
        passed=bool(passed),
        readiness_score=float(readiness.readiness_score),
        readiness_status=readiness.readiness_status,
        result_status=readiness.result_status,
    )


def top_fix_candidates(items: Iterable[Any], *, limit: int = 10) -> list[dict]:
    """
    Deterministic fix order.

    Priority:
      1) failed
      2) blocked
      3) in_progress
      4) todo

    Within same status:
      - higher severity first
      - common_fail first
      - then stable item code
    """

    def get_field(it: Any, key: str, default=None):
        if isinstance(it, dict):
            return it.get(key, default)
        return getattr(it, key, default)

    severity_order = {"critical": 4, "fail": 3, "warn": 2, "info": 1}
    status_order = {"failed": 0, "blocked": 1, "in_progress": 2, "todo": 3, "done": 9}

    rows: list[dict] = []
    for it in items or []:
        status = _normalize_item_status_for_summary(it)
        if status == "done":
            continue

        item_code = normalize_rule_code(
            get_field(it, "item_code", None) or get_field(it, "code", None) or ""
        )
        category = str(get_field(it, "category", "") or "")
        desc = str(get_field(it, "description", "") or "")
        severity = normalize_severity(get_field(it, "severity", "fail"))
        common_fail = bool(get_field(it, "common_fail", True))
        fail_reason = get_field(it, "fail_reason", None)
        remediation_guidance = get_field(it, "remediation_guidance", None)
        result_status = normalize_inspection_item_status(
            get_field(it, "result_status", None) or get_field(it, "status", None),
            failed=get_field(it, "failed", None),
        )

        rows.append(
            {
                "item_code": item_code,
                "category": category,
                "description": desc,
                "severity": severity,
                "common_fail": common_fail,
                "status": status,
                "result_status": result_status,
                "fail_reason": str(fail_reason).strip() if fail_reason else None,
                "remediation_guidance": str(remediation_guidance).strip() if remediation_guidance else None,
            }
        )

    rows = sorted(
        rows,
        key=lambda r: (
            status_order.get(r["status"], 9),
            -severity_order.get(r["severity"], 0),
            0 if r["common_fail"] else 1,
            r["item_code"],
        ),
    )
    return rows[: max(1, int(limit))]
