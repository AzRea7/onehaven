from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Iterable


def _utcnow() -> datetime:
    return datetime.utcnow()


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _json_loads_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def compute_next_retry_due(
    *,
    retry_count: int,
    base_dt: datetime | None = None,
    min_hours: int = 6,
    max_days: int = 14,
) -> datetime:
    base = base_dt or _utcnow()
    hours = min(max(min_hours, min_hours * (2 ** max(0, int(retry_count)))), max_days * 24)
    return base + timedelta(hours=hours)


def build_source_change_summary(
    *,
    previous_fingerprint: str | None,
    current_fingerprint: str | None,
    previous_version_id: int | None = None,
    current_version_id: int | None = None,
    http_status: int | None = None,
    fetch_error: str | None = None,
    authoritative: bool = False,
    previous_last_changed_at: datetime | None = None,
) -> dict[str, Any]:
    now = _utcnow()
    previous_fp = (previous_fingerprint or "").strip() or None
    current_fp = (current_fingerprint or "").strip() or None

    if fetch_error:
        return {
            "comparison_state": "fetch_failed",
            "changed": False,
            "change_detected": False,
            "change_kind": "fetch_failed",
            "change_severity": "failed",
            "requires_revalidation": False,
            "revalidation_reason": None,
            "actionable_outcome": "retry_fetch",
            "previous_fingerprint": previous_fp,
            "current_fingerprint": current_fp,
            "previous_version_id": previous_version_id,
            "current_version_id": current_version_id,
            "http_status": http_status,
            "fetch_error": fetch_error,
            "changed_at": None,
            "last_changed_at": previous_last_changed_at.isoformat() if previous_last_changed_at else None,
        }

    if previous_fp is None and current_fp is not None:
        changed = True
        kind = "first_fetch"
    elif previous_fp is not None and current_fp is not None and previous_fp != current_fp:
        changed = True
        kind = "content_changed"
    else:
        changed = False
        kind = "unchanged"

    changed_at = now if changed else None
    requires_revalidation = bool(changed and authoritative)
    severity = "none"
    actionable_outcome = "monitor"
    if changed and authoritative:
        severity = "high"
        actionable_outcome = "revalidate"
    elif changed:
        severity = "medium"
        actionable_outcome = "review_changed_content"
    elif current_fp is None:
        severity = "low"
        actionable_outcome = "missing_fingerprint"

    return {
        "comparison_state": "compared",
        "changed": changed,
        "change_detected": changed,
        "change_kind": kind,
        "change_severity": severity,
        "requires_revalidation": requires_revalidation,
        "revalidation_reason": "authoritative_source_changed" if requires_revalidation else None,
        "actionable_outcome": actionable_outcome,
        "previous_fingerprint": previous_fp,
        "current_fingerprint": current_fp,
        "previous_version_id": previous_version_id,
        "current_version_id": current_version_id,
        "http_status": http_status,
        "fetch_error": None,
        "changed_at": changed_at.isoformat() if changed_at else None,
        "last_changed_at": changed_at.isoformat() if changed_at else (previous_last_changed_at.isoformat() if previous_last_changed_at else None),
    }


def determine_source_refresh_state(
    *,
    fetch_ok: bool,
    change_summary: dict[str, Any] | None = None,
    blocked_reason: str | None = None,
) -> dict[str, Any]:
    summary = dict(change_summary or {})
    changed = bool(summary.get("changed"))
    requires_revalidation = bool(summary.get("requires_revalidation"))
    fetch_error = summary.get("fetch_error")
    change_kind = summary.get("change_kind")

    if blocked_reason:
        return {
            "refresh_state": "blocked",
            "status_reason": blocked_reason,
            "blocked_reason": blocked_reason,
            "next_step": "manual_unblock",
            "revalidation_required": False,
        }
    if not fetch_ok:
        next_step = "retry_fetch"
        state = "failed" if fetch_error else "degraded"
        return {
            "refresh_state": state,
            "status_reason": fetch_error or "fetch_failed",
            "blocked_reason": None,
            "next_step": next_step,
            "revalidation_required": False,
        }
    if requires_revalidation:
        return {
            "refresh_state": "validating",
            "status_reason": summary.get("revalidation_reason") or "authoritative_change_detected",
            "blocked_reason": None,
            "next_step": "recompute",
            "revalidation_required": True,
        }
    if changed:
        return {
            "refresh_state": "healthy",
            "status_reason": change_kind or "content_changed",
            "blocked_reason": None,
            "next_step": "monitor",
            "revalidation_required": False,
        }
    return {
        "refresh_state": "healthy",
        "status_reason": "no_change_detected",
        "blocked_reason": None,
        "next_step": "monitor",
        "revalidation_required": False,
    }


def determine_validation_refresh_state(
    *,
    validated_count: int,
    weak_support_count: int,
    ambiguous_count: int,
    conflicting_count: int,
    unsupported_count: int,
    blocking_issue_count: int = 0,
) -> dict[str, Any]:
    if blocking_issue_count > 0 or conflicting_count > 0:
        return {
            "refresh_state": "blocked",
            "status_reason": "validation_blocking_conflicts",
            "next_step": "manual_review",
            "revalidation_required": False,
        }
    if unsupported_count > 0:
        return {
            "refresh_state": "degraded",
            "status_reason": "validation_missing_support",
            "next_step": "retry_validation",
            "revalidation_required": False,
        }
    if ambiguous_count > 0 or weak_support_count > 0:
        return {
            "refresh_state": "degraded",
            "status_reason": "validation_needs_review",
            "next_step": "review_validation_results",
            "revalidation_required": False,
        }
    if validated_count > 0:
        return {
            "refresh_state": "healthy",
            "status_reason": "validation_complete",
            "next_step": "monitor",
            "revalidation_required": False,
        }
    return {
        "refresh_state": "pending",
        "status_reason": "validation_no_assertions",
        "next_step": "extract_or_retry",
        "revalidation_required": False,
    }


def summarize_refresh_runs(source_runs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(source_runs or [])
    changed_count = 0
    failed_count = 0
    blocked_count = 0
    validating_count = 0
    states: dict[str, int] = {}
    next_steps: dict[str, int] = {}
    for row in rows:
        refresh = row.get("refresh") if isinstance(row, dict) else {}
        state = str((refresh or {}).get("refresh_state") or "unknown").strip().lower()
        next_step = str((refresh or {}).get("next_step") or "unknown").strip().lower()
        states[state] = states.get(state, 0) + 1
        next_steps[next_step] = next_steps.get(next_step, 0) + 1
        if bool((refresh or {}).get("change_detected") or (refresh or {}).get("changed")):
            changed_count += 1
        if not bool((refresh or {}).get("ok", False)):
            failed_count += 1
        if state == "blocked":
            blocked_count += 1
        if state == "validating":
            validating_count += 1
    market_state = "healthy"
    if blocked_count:
        market_state = "blocked"
    elif failed_count:
        market_state = "degraded"
    elif validating_count:
        market_state = "validating"
    return {
        "refresh_state": market_state,
        "states": states,
        "next_steps": next_steps,
        "changed_count": changed_count,
        "failed_count": failed_count,
        "blocked_count": blocked_count,
        "validating_count": validating_count,
    }


def determine_profile_refresh_state(
    *,
    refresh_runs: Iterable[dict[str, Any]],
    recompute_ok: bool,
    missing_categories: list[str] | None = None,
    stale_categories: list[str] | None = None,
    profile_is_stale: bool = False,
) -> dict[str, Any]:
    summary = summarize_refresh_runs(refresh_runs)
    missing = [str(x).strip().lower() for x in (missing_categories or []) if str(x).strip()]
    stale = [str(x).strip().lower() for x in (stale_categories or []) if str(x).strip()]

    if summary["blocked_count"] > 0:
        state = "blocked"
        next_step = "manual_review"
        reason = "blocked_sources_present"
    elif not recompute_ok:
        state = "failed"
        next_step = "retry_recompute"
        reason = "recompute_failed"
    elif summary["failed_count"] > 0 or stale or profile_is_stale:
        state = "degraded"
        next_step = "retry_refresh"
        reason = "stale_or_failed_sources"
    elif missing:
        state = "pending"
        next_step = "search_retry"
        reason = "unresolved_gaps"
    elif summary["validating_count"] > 0:
        state = "validating"
        next_step = "recompute"
        reason = "authoritative_changes_detected"
    else:
        state = "healthy"
        next_step = "monitor"
        reason = "refresh_complete"

    return {
        "refresh_state": state,
        "status_reason": reason,
        "next_step": next_step,
        "summary": summary,
    }
