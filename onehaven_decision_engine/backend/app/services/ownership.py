# backend/app/services/ownership.py
from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Property, Tenant, Lease


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
