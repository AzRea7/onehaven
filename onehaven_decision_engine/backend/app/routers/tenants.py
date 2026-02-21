# backend/app/routers/tenants.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Tenant, Lease
from ..schemas import TenantCreate, TenantOut, LeaseCreate, LeaseOut
from ..domain.audit import emit_audit

from ..services.ownership import must_get_property, must_get_tenant, must_get_lease
from ..services.events_facade import wf
from ..services.property_state_machine import advance_stage_if_needed
from ..services.lease_rules import ensure_no_lease_overlap

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
    wf(db, org_id=p.org_id, actor_user_id=p.user_id, event_type="tenant.created", payload={"tenant_id": row.id})
    db.commit()

    return row


@router.get("", response_model=list[TenantOut])
def list_tenants(
    limit: int = Query(default=100, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Tenant).where(Tenant.org_id == p.org_id).order_by(desc(Tenant.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/{tenant_id}", response_model=TenantOut)
def update_tenant(
    tenant_id: int,
    payload: TenantCreate,  # full-update for simplicity
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = must_get_tenant(db, org_id=p.org_id, tenant_id=tenant_id)
    before = row.model_dump()

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="tenant.update",
        entity_type="Tenant",
        entity_id=str(row.id),
        before=before,
        after=row.model_dump(),
    )
    wf(db, org_id=p.org_id, actor_user_id=p.user_id, event_type="tenant.updated", payload={"tenant_id": row.id})
    db.commit()

    return row


@router.delete("/{tenant_id}")
def delete_tenant(tenant_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = must_get_tenant(db, org_id=p.org_id, tenant_id=tenant_id)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="tenant.delete",
        entity_type="Tenant",
        entity_id=str(row.id),
        before=row.model_dump(),
        after=None,
    )
    wf(db, org_id=p.org_id, actor_user_id=p.user_id, event_type="tenant.deleted", payload={"tenant_id": row.id})

    db.delete(row)
    db.commit()
    return {"ok": True}


@router.post("/leases", response_model=LeaseOut)
def create_lease(payload: LeaseCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    # ownership checks (constitutional)
    _ = must_get_property(db, org_id=p.org_id, property_id=payload.property_id)
    _ = must_get_tenant(db, org_id=p.org_id, tenant_id=payload.tenant_id)

    # Phase 4 DoD: block overlapping leases
    ensure_no_lease_overlap(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        start_date=payload.start_date,
        end_date=payload.end_date,
        exclude_lease_id=None,
    )

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

    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="lease.created",
        property_id=payload.property_id,
        payload={"lease_id": row.id, "tenant_id": payload.tenant_id},
    )

    # Phase 4: lease implies we’re now in tenant stage
    advance_stage_if_needed(db, org_id=p.org_id, property_id=payload.property_id, suggested_stage="tenant")

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
        must_get_property(db, org_id=p.org_id, property_id=property_id)
        q = q.where(Lease.property_id == property_id)

    if tenant_id is not None:
        must_get_tenant(db, org_id=p.org_id, tenant_id=tenant_id)
        q = q.where(Lease.tenant_id == tenant_id)

    q = q.order_by(desc(Lease.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/leases/{lease_id}", response_model=LeaseOut)
def update_lease(
    lease_id: int,
    payload: LeaseCreate,  # full-update for simplicity
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = must_get_lease(db, org_id=p.org_id, lease_id=lease_id)
    before = row.model_dump()

    # if property/tenant changes, validate ownership
    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)
    must_get_tenant(db, org_id=p.org_id, tenant_id=payload.tenant_id)

    # Phase 4 DoD: block overlapping leases (excluding self)
    ensure_no_lease_overlap(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        start_date=payload.start_date,
        end_date=payload.end_date,
        exclude_lease_id=row.id,
    )

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="lease.update",
        entity_type="Lease",
        entity_id=str(row.id),
        before=before,
        after=row.model_dump(),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="lease.updated",
        property_id=row.property_id,
        payload={"lease_id": row.id, "tenant_id": row.tenant_id},
    )
    db.commit()
    return row


@router.delete("/leases/{lease_id}")
def delete_lease(lease_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = must_get_lease(db, org_id=p.org_id, lease_id=lease_id)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="lease.delete",
        entity_type="Lease",
        entity_id=str(row.id),
        before=row.model_dump(),
        after=None,
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="lease.deleted",
        property_id=row.property_id,
        payload={"lease_id": row.id, "tenant_id": row.tenant_id},
    )

    db.delete(row)
    db.commit()
    return {"ok": True}