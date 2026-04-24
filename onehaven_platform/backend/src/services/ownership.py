from __future__ import annotations

from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.workflow.panes import allowed_panes_for_principal, clamp_pane
from onehaven_platform.backend.src.models import Lease, Property, Tenant


def must_get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    row = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
    if not row:
        raise HTTPException(status_code=404, detail="property not found")
    return row


def must_get_tenant(db: Session, *, org_id: int, tenant_id: int) -> Tenant:
    row = db.scalar(select(Tenant).where(Tenant.id == tenant_id, Tenant.org_id == org_id))
    if not row:
        raise HTTPException(status_code=404, detail="tenant not found")
    return row


def must_get_lease(db: Session, *, org_id: int, lease_id: int) -> Lease:
    row = db.scalar(select(Lease).where(Lease.id == lease_id, Lease.org_id == org_id))
    if not row:
        raise HTTPException(status_code=404, detail="lease not found")
    return row


def must_belong_to_org(row: Any, *, org_id: int, not_found_detail: str = "resource not found") -> Any:
    row_org_id = getattr(row, "org_id", None)
    if row is None or row_org_id != org_id:
        raise HTTPException(status_code=404, detail=not_found_detail)
    return row


def ensure_pane_access(*, principal: Any, pane: Optional[str]) -> str:
    pane_key = clamp_pane(pane)
    allowed = allowed_panes_for_principal(principal)
    if pane_key not in allowed and pane_key != "admin":
        raise HTTPException(
            status_code=403,
            detail={
                "error": "pane_not_allowed",
                "pane": pane_key,
                "allowed_panes": allowed,
            },
        )
    return pane_key