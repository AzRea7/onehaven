from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.policy_models import PolicySource
from app.services.policy_discovery_service import sync_policy_source_into_inventory, update_inventory_after_fetch


def sync_crawl_result_to_inventory(
    db: Session,
    *,
    source: PolicySource,
    fetch_result: dict[str, Any],
) -> dict[str, Any]:
    inventory = update_inventory_after_fetch(
        db,
        source=source,
        fetch_result=fetch_result,
        source_version_id=fetch_result.get("source_version_id"),
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
        "validation_due_at": getattr(inventory, "validation_due_at", None).isoformat() if inventory is not None and getattr(inventory, "validation_due_at", None) else None,
        "current_source_version_id": getattr(inventory, "current_source_version_id", None) if inventory is not None else None,
    }
