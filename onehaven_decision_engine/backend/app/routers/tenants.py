from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.audit import emit_audit
from ..models import Lease, Tenant
from ..schemas import LeaseCreate, LeaseOut, TenantCreate, TenantOut
from ..services.events_facade import wf
from ..services.lease_rules import ensure_no_lease_overlap
from ..services.ownership import must_get_lease, must_get_property, must_get_tenant
from ..services.property_state_machine import sync_property_state
from ..services.stage_guard import require_stage

router = APIRouter(prefix="/tenants", tags=["tenants"])


def _tenant_payload(row: Tenant) -> dict:
    return {
        "id": row.id,
        "full_name": row.full_name,
        "phone": row.phone,
        "email": row.email,
        "voucher_status": row.voucher_status,
        "notes": row.notes,
    }


def _lease_payload(row: Lease) -> dict:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "tenant_id": row.tenant_id,
        "start_date": row.start_date.isoformat() if row.start_date else None,
        "end_date": row.end_date.isoformat() if row.end_date else None,
        "total_rent": row.total_rent,
        "tenant_portion": row.tenant_portion,
        "housing_authority_portion": row.housing_authority_portion,
        "hap_contract_status": row.hap_contract_status,
        "notes": row.notes,
    }


@router.post("", response_model=TenantOut)
def create_tenant(
    payload: TenantCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = Tenant(**payload.model_dump(), org_id=p.org_id)
    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="tenant.create",
        entity_type="Tenant",
        entity_id=str(row.id),
        before=None,
        after=_tenant_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="tenant.created",
        payload={"tenant_id": row.id},
    )

    db.commit()
    db.refresh(row)
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
    payload: TenantCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = must_get_tenant(db, org_id=p.org_id, tenant_id=tenant_id)
    before = _tenant_payload(row)

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="tenant.update",
        entity_type="Tenant",
        entity_id=str(row.id),
        before=before,
        after=_tenant_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="tenant.updated",
        payload={"tenant_id": row.id},
    )

    db.commit()
    db.refresh(row)
    return row


@router.delete("/{tenant_id}")
def delete_tenant(
    tenant_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = must_get_tenant(db, org_id=p.org_id, tenant_id=tenant_id)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="tenant.delete",
        entity_type="Tenant",
        entity_id=str(row.id),
        before=_tenant_payload(row),
        after=None,
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="tenant.deleted",
        payload={"tenant_id": row.id},
    )

    db.delete(row)
    db.commit()
    return {"ok": True}


@router.post("/leases", response_model=LeaseOut)
def create_lease(
    payload: LeaseCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)
    must_get_tenant(db, org_id=p.org_id, tenant_id=payload.tenant_id)

    require_stage(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        min_stage="tenant",
        action="create lease",
    )

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
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="lease.create",
        entity_type="Lease",
        entity_id=str(row.id),
        before=None,
        after=_lease_payload(row),
    )

    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="lease.created",
        property_id=payload.property_id,
        payload={"lease_id": row.id, "tenant_id": payload.tenant_id},
    )

    sync_property_state(db, org_id=p.org_id, property_id=payload.property_id)

    db.commit()
    db.refresh(row)
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
        require_stage(
            db,
            org_id=p.org_id,
            property_id=property_id,
            min_stage="tenant",
            action="view leases",
        )
        q = q.where(Lease.property_id == property_id)

    if tenant_id is not None:
        must_get_tenant(db, org_id=p.org_id, tenant_id=tenant_id)
        q = q.where(Lease.tenant_id == tenant_id)

    q = q.order_by(desc(Lease.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/leases/{lease_id}", response_model=LeaseOut)
def update_lease(
    lease_id: int,
    payload: LeaseCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = must_get_lease(db, org_id=p.org_id, lease_id=lease_id)
    before = _lease_payload(row)

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="tenant",
        action="update lease",
    )

    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)
    must_get_tenant(db, org_id=p.org_id, tenant_id=payload.tenant_id)

    ensure_no_lease_overlap(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        start_date=payload.start_date,
        end_date=payload.end_date,
        exclude_lease_id=row.id,
    )

    old_property_id = row.property_id

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="lease.update",
        entity_type="Lease",
        entity_id=str(row.id),
        before=before,
        after=_lease_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="lease.updated",
        property_id=row.property_id,
        payload={"lease_id": row.id, "tenant_id": row.tenant_id},
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)
    if old_property_id != row.property_id:
        sync_property_state(db, org_id=p.org_id, property_id=old_property_id)

    db.commit()
    db.refresh(row)
    return row


@router.delete("/leases/{lease_id}")
def delete_lease(
    lease_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = must_get_lease(db, org_id=p.org_id, lease_id=lease_id)

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="tenant",
        action="delete lease",
    )

    prop_id = row.property_id

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="lease.delete",
        entity_type="Lease",
        entity_id=str(row.id),
        before=_lease_payload(row),
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
    db.flush()

    sync_property_state(db, org_id=p.org_id, property_id=prop_id)

    db.commit()
    return {"ok": True}
