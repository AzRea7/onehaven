from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, or_, select
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
from ..services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/tenants", tags=["tenants"])


def _now() -> datetime:
    return datetime.utcnow()


def _tenant_payload(row: Tenant) -> dict[str, Any]:
    return {
        "id": row.id,
        "full_name": row.full_name,
        "phone": row.phone,
        "email": row.email,
        "voucher_status": row.voucher_status,
        "notes": row.notes,
    }


def _lease_payload(row: Lease) -> dict[str, Any]:
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


def _lease_status(row: Lease, *, now: datetime | None = None) -> str:
    now = now or _now()
    start = row.start_date
    end = row.end_date

    if start and start > now:
        return "upcoming"
    if end and end < now:
        return "ended"
    return "active"


def _lease_row(row: Lease, tenant: Tenant | None = None) -> dict[str, Any]:
    status = _lease_status(row)
    total_rent = float(row.total_rent or 0.0)
    tenant_portion = float(row.tenant_portion or 0.0) if row.tenant_portion is not None else None
    hap_portion = (
        float(row.housing_authority_portion or 0.0)
        if row.housing_authority_portion is not None
        else None
    )

    if tenant_portion is None and hap_portion is not None:
        tenant_portion = max(0.0, total_rent - hap_portion)

    if hap_portion is None and tenant_portion is not None:
        hap_portion = max(0.0, total_rent - tenant_portion)

    return {
        **_lease_payload(row),
        "status": status,
        "tenant_name": getattr(tenant, "full_name", None),
        "tenant_email": getattr(tenant, "email", None),
        "voucher_status": getattr(tenant, "voucher_status", None),
        "total_rent": total_rent,
        "tenant_portion": tenant_portion,
        "housing_authority_portion": hap_portion,
        "is_section8_like": bool((hap_portion or 0.0) > 0),
    }


def _pipeline_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    must_get_property(db, org_id=org_id, property_id=property_id)

    tenants = list(
        db.scalars(
            select(Tenant).where(Tenant.org_id == org_id).order_by(desc(Tenant.id))
        ).all()
    )
    tenant_map = {t.id: t for t in tenants}

    leases = list(
        db.scalars(
            select(Lease)
            .where(Lease.org_id == org_id, Lease.property_id == property_id)
            .order_by(desc(Lease.start_date), desc(Lease.id))
        ).all()
    )

    rows = [_lease_row(l, tenant_map.get(l.tenant_id)) for l in leases]

    active = [x for x in rows if x["status"] == "active"]
    upcoming = [x for x in rows if x["status"] == "upcoming"]
    ended = [x for x in rows if x["status"] == "ended"]

    active_primary = active[0] if active else None

    occupancy_status = "vacant"
    if active_primary:
        occupancy_status = "occupied"
    elif upcoming:
        occupancy_status = "leased_not_started"

    return {
        "property_id": property_id,
        "occupancy_status": occupancy_status,
        "counts": {
            "tenants_total": len(tenants),
            "leases_total": len(rows),
            "active_leases": len(active),
            "upcoming_leases": len(upcoming),
            "ended_leases": len(ended),
        },
        "active_lease": active_primary,
        "upcoming_leases": upcoming[:5],
        "ended_leases": ended[:5],
        "leases": rows,
        "workflow": build_workflow_summary(
            db,
            org_id=org_id,
            property_id=property_id,
            recompute=False,
        ),
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
    q: str | None = Query(default=None),
    voucher_status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    stmt = select(Tenant).where(Tenant.org_id == p.org_id)

    if q:
        needle = f"%{q.strip()}%"
        stmt = stmt.where(
            or_(
                Tenant.full_name.ilike(needle),
                Tenant.email.ilike(needle),
                Tenant.phone.ilike(needle),
            )
        )

    if voucher_status:
        stmt = stmt.where(Tenant.voucher_status == voucher_status)

    stmt = stmt.order_by(desc(Tenant.id)).limit(limit)
    return list(db.scalars(stmt).all())


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
    active_only: bool = Query(default=False),
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

    rows = list(db.scalars(q.order_by(desc(Lease.id)).limit(limit)).all())

    if active_only:
        rows = [x for x in rows if _lease_status(x) == "active"]

    return rows


@router.get("/leases/active", response_model=list[dict])
def list_active_leases(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    tenant_map = {
        t.id: t
        for t in db.scalars(select(Tenant).where(Tenant.org_id == p.org_id)).all()
    }

    stmt = select(Lease).where(Lease.org_id == p.org_id)
    if property_id is not None:
        must_get_property(db, org_id=p.org_id, property_id=property_id)
        stmt = stmt.where(Lease.property_id == property_id)

    rows = list(db.scalars(stmt.order_by(desc(Lease.start_date), desc(Lease.id)).limit(limit)).all())
    return [_lease_row(x, tenant_map.get(x.tenant_id)) for x in rows if _lease_status(x) == "active"]


@router.get("/property/{property_id}/snapshot", response_model=dict)
def tenant_property_snapshot(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="tenant",
        action="view tenant/lease snapshot",
    )
    return _pipeline_snapshot(db, org_id=p.org_id, property_id=property_id)


@router.get("/pipeline", response_model=dict)
def tenant_pipeline(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    tenant_map = {
        t.id: t
        for t in db.scalars(select(Tenant).where(Tenant.org_id == p.org_id)).all()
    }

    stmt = select(Lease).where(Lease.org_id == p.org_id)
    if property_id is not None:
        must_get_property(db, org_id=p.org_id, property_id=property_id)
        stmt = stmt.where(Lease.property_id == property_id)

    leases = list(db.scalars(stmt.order_by(desc(Lease.start_date), desc(Lease.id)).limit(limit)).all())
    rows = [_lease_row(l, tenant_map.get(l.tenant_id)) for l in leases]

    return {
        "rows": rows,
        "counts": {
            "total": len(rows),
            "active": sum(1 for x in rows if x["status"] == "active"),
            "upcoming": sum(1 for x in rows if x["status"] == "upcoming"),
            "ended": sum(1 for x in rows if x["status"] == "ended"),
            "voucher_backed": sum(1 for x in rows if x["is_section8_like"]),
        },
    }


@router.get("/leases/workflow/{property_id}", response_model=dict)
def lease_workflow_snapshot(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=property_id)
    return build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=True)


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
