from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.policy_models import PolicySource
from app.services.policy_discovery_service import (
    sync_policy_source_into_inventory,
    update_inventory_after_fetch,
)


def _change_summary(fetch_result: dict[str, Any]) -> dict[str, Any]:
    raw = fetch_result.get("change_summary")
    return dict(raw) if isinstance(raw, dict) else {}


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def sync_crawl_result_to_inventory(
    db: Session,
    *,
    source: PolicySource,
    fetch_result: dict[str, Any],
) -> dict[str, Any]:
    change_summary = _change_summary(fetch_result)
    normalized_fetch = dict(fetch_result or {})

    normalized_fetch.setdefault("source_id", int(getattr(source, "id", 0) or 0))
    normalized_fetch.setdefault("source_version_id", fetch_result.get("source_version_id"))
    normalized_fetch.setdefault(
        "current_fingerprint",
        fetch_result.get("current_fingerprint")
        or change_summary.get("current_fingerprint")
        or getattr(source, "current_fingerprint", None)
        or getattr(source, "content_sha256", None),
    )
    normalized_fetch.setdefault(
        "previous_fingerprint",
        fetch_result.get("previous_fingerprint")
        or change_summary.get("previous_fingerprint"),
    )
    normalized_fetch.setdefault(
        "comparison_state",
        fetch_result.get("comparison_state")
        or change_summary.get("comparison_state"),
    )
    normalized_fetch.setdefault(
        "change_kind",
        fetch_result.get("change_kind")
        or change_summary.get("change_kind"),
    )
    normalized_fetch.setdefault(
        "actionable_outcome",
        fetch_result.get("actionable_outcome")
        or change_summary.get("actionable_outcome"),
    )
    normalized_fetch.setdefault(
        "changed",
        bool(fetch_result.get("changed") or change_summary.get("changed")),
    )
    normalized_fetch.setdefault(
        "change_detected",
        bool(
            fetch_result.get("change_detected")
            or change_summary.get("change_detected")
            or normalized_fetch.get("changed")
        ),
    )
    normalized_fetch.setdefault(
        "revalidation_required",
        bool(
            fetch_result.get("revalidation_required")
            or change_summary.get("requires_revalidation")
        ),
    )
    normalized_fetch.setdefault(
        "raw_path",
        fetch_result.get("raw_path")
        or change_summary.get("raw_path")
        or getattr(source, "raw_path", None),
    )
    normalized_fetch.setdefault(
        "content_sha256",
        fetch_result.get("content_sha256")
        or change_summary.get("current_fingerprint")
        or getattr(source, "content_sha256", None),
    )
    normalized_fetch.setdefault(
        "retry_due_at",
        fetch_result.get("retry_due_at")
        or change_summary.get("retry_due_at")
        or getattr(source, "next_refresh_due_at", None),
    )

    inventory = update_inventory_after_fetch(
        db,
        source=source,
        fetch_result=normalized_fetch,
        source_version_id=normalized_fetch.get("source_version_id"),
    )

    if inventory is None:
        inventory = sync_policy_source_into_inventory(
            db,
            source=source,
            org_id=getattr(source, "org_id", None),
        )

    return {
        "ok": inventory is not None,
        "inventory_id": int(inventory.id) if inventory is not None else None,
        "lifecycle_state": getattr(inventory, "lifecycle_state", None) if inventory is not None else None,
        "crawl_status": getattr(inventory, "crawl_status", None) if inventory is not None else None,
        "refresh_state": getattr(inventory, "refresh_state", None) if inventory is not None else None,
        "refresh_status_reason": getattr(inventory, "refresh_status_reason", None) if inventory is not None else None,
        "next_refresh_step": getattr(inventory, "next_refresh_step", None) if inventory is not None else None,
        "revalidation_required": bool(getattr(inventory, "revalidation_required", False)) if inventory is not None else False,
        "validation_due_at": (
            _iso_or_none(getattr(inventory, "validation_due_at", None))
            if inventory is not None
            else None
        ),
        "current_source_version_id": getattr(inventory, "current_source_version_id", None) if inventory is not None else None,
        "last_change_summary": change_summary,
    }