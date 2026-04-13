from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.policy_models import PolicySource
from app.services.policy_discovery_service import update_inventory_after_fetch


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
    return {
        "ok": inventory is not None,
        "inventory_id": int(inventory.id) if inventory is not None else None,
        "lifecycle_state": getattr(inventory, "lifecycle_state", None) if inventory is not None else None,
        "crawl_status": getattr(inventory, "crawl_status", None) if inventory is not None else None,
        "refresh_state": getattr(inventory, "refresh_state", None) if inventory is not None else None,
        "next_refresh_step": getattr(inventory, "next_refresh_step", None) if inventory is not None else None,
        "revalidation_required": bool(getattr(inventory, "revalidation_required", False)) if inventory is not None else False,
    }
