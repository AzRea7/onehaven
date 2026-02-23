# backend/app/domain/compliance/hqs.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class HQSSummary:
    total: int
    done: int
    failed: int
    pct_done: float
    passed: bool


def _normalize_status(raw: Optional[str]) -> str:
    """
    Normalize multiple historical status vocabularies into a canonical set.

    Canonical:
      - todo
      - in_progress
      - done
      - blocked
      - failed

    Also supports older vocab:
      - pass -> done
      - fail -> failed
      - ok   -> done
    """
    s = (raw or "").strip().lower()

    if s in ("pass", "passed", "ok", "success"):
        return "done"
    if s in ("fail", "failed", "bad"):
        return "failed"
    if s in ("todo", "to_do", "not_started", ""):
        return "todo"
    if s in ("in_progress", "doing", "wip"):
        return "in_progress"
    if s in ("blocked", "stuck"):
        return "blocked"
    if s in ("done", "complete", "completed"):
        return "done"

    # Unknown values are treated as todo (conservative).
    return "todo"


def summarize_items(items: Iterable[Any], *, latest_inspection_passed: bool = False) -> HQSSummary:
    """
    Summarize checklist items. Works with:
      - SQLAlchemy PropertyChecklistItem rows (status on row.status)
      - dict-like items from JSON (status at ["status"])
      - older test objects with .status

    Passing rule (Phase 3 DoD):
      passed = pct_done >= 0.95 AND failed == 0 AND latest_inspection_passed == True
    """
    total = 0
    done = 0
    failed = 0

    for it in items:
        total += 1
        status = None
        if isinstance(it, dict):
            status = it.get("status")
        else:
            status = getattr(it, "status", None)

        s = _normalize_status(status)
        if s == "done":
            done += 1
        elif s == "failed":
            failed += 1

    pct_done = (done / total) if total else 0.0
    passed = (pct_done >= 0.95) and (failed == 0) and bool(latest_inspection_passed)

    return HQSSummary(
        total=total,
        done=done,
        failed=failed,
        pct_done=round(pct_done, 4),
        passed=bool(passed),
    )


def top_fix_candidates(items: Iterable[Any], *, limit: int = 10) -> list[dict]:
    """
    Produce a deterministic “what to fix next” list.

    Priority:
      1) failed
      2) blocked
      3) in_progress
      4) todo

    Within same status, higher severity first, then common_fail, then stable sort by item_code.
    """
    def get_field(it: Any, key: str, default=None):
        if isinstance(it, dict):
            return it.get(key, default)
        return getattr(it, key, default)

    order = {"failed": 0, "blocked": 1, "in_progress": 2, "todo": 3, "done": 9}

    rows: list[dict] = []
    for it in items:
        status = _normalize_status(get_field(it, "status"))
        if status == "done":
            continue

        item_code = get_field(it, "item_code", None) or get_field(it, "code", None) or ""
        category = get_field(it, "category", "")
        desc = get_field(it, "description", "")
        severity = int(get_field(it, "severity", 3) or 3)
        common_fail = bool(get_field(it, "common_fail", True))

        rows.append(
            {
                "item_code": str(item_code),
                "category": str(category),
                "description": str(desc),
                "severity": severity,
                "common_fail": common_fail,
                "status": status,
            }
        )

    rows = sorted(
        rows,
        key=lambda r: (
            order.get(r["status"], 9),
            -int(r["severity"]),
            0 if r["common_fail"] else 1,
            r["item_code"],
        ),
    )
    return rows[: max(1, int(limit))]