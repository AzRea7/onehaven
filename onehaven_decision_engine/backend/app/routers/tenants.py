from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Tenant, Lease, Property
from ..schemas import TenantCreate, TenantOut, LeaseCreate, LeaseOut
from ..domain.audit import emit_audit

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.post("", response_model=TenantOut)
def create_tenant(payload: TenantCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = Tenant(**payload.model_dump(), org_id=p.org_id)
    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="tenant.create",
        entity_type="Tenant",
        entity_id=str(row.id),
        before=None,
        after=row.model_dump(),
    )
    db.commit()

    return row


@router.get("", response_model=list[TenantOut])
def list_tenants(
    limit: int = Query(default=100, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = (
        select(Tenant)
        .where(Tenant.org_id == p.org_id)
        .order_by(desc(Tenant.id))
        .limit(limit)
    )
    return list(db.scalars(q).all())


@router.post("/leases", response_model=LeaseOut)
def create_lease(payload: LeaseCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    row = Lease(**payload.model_dump(), org_id=p.org_id)
    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="lease.create",
        entity_type="Lease",
        entity_id=str(row.id),
        before=None,
        after=row.model_dump(),
    )
    db.commit()

    return row


@router.get("/leases", response_model=list[LeaseOut])
def list_leases(
    property_id: int | None = Query(default=None),
    tenant_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Lease).where(Lease.org_id == p.org_id)

    if property_id is not None:
        q = q.where(Lease.property_id == property_id)
    if tenant_id is not None:
        q = q.where(Lease.tenant_id == tenant_id)

    q = q.order_by(desc(Lease.id)).limit(limit)
    return list(db.scalars(q).all())
