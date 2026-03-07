from __future__ import annotations

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.audit import emit_audit
from ..models import Property, Valuation
from ..schemas import ValuationCreate, ValuationOut
from ..services.events_facade import wf
from ..services.ownership import must_get_property
from ..services.property_state_machine import sync_property_state
from ..services.stage_guard import require_stage
from ..services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/equity", tags=["equity"])


def _valuation_payload(row: Valuation) -> dict:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "as_of": row.as_of.isoformat() if row.as_of else None,
        "estimated_value": row.estimated_value,
        "loan_balance": row.loan_balance,
        "notes": row.notes,
    }


@router.post("/valuations", response_model=ValuationOut)
def create_valuation(
    payload: ValuationCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        min_stage="cash",
        action="create valuation",
    )

    data = payload.model_dump()
    data["org_id"] = p.org_id
    data.setdefault("as_of", datetime.utcnow())

    row = Valuation(**data)
    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.create",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=None,
        after=_valuation_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.created",
        property_id=row.property_id,
        payload={
            "valuation_id": row.id,
            "as_of": row.as_of.isoformat() if row.as_of else None,
            "estimated_value": row.estimated_value,
        },
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)

    db.commit()
    db.refresh(row)
    return row


@router.get("/valuations", response_model=list[ValuationOut])
def list_valuations(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Valuation).where(Valuation.org_id == p.org_id)

    if property_id is not None:
        prop = db.get(Property, property_id)
        if not prop or prop.org_id != p.org_id:
            raise HTTPException(status_code=404, detail="property not found")
        require_stage(
            db,
            org_id=p.org_id,
            property_id=property_id,
            min_stage="cash",
            action="view valuations",
        )
        q = q.where(Valuation.property_id == property_id)

    q = q.order_by(desc(Valuation.as_of), desc(Valuation.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/valuations/{valuation_id}", response_model=ValuationOut)
def update_valuation(
    valuation_id: int,
    payload: ValuationCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Valuation).where(Valuation.id == valuation_id, Valuation.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="valuation not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="cash",
        action="update valuation",
    )

    before = _valuation_payload(row)

    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    old_property_id = row.property_id

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    row.org_id = p.org_id
    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.update",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=before,
        after=_valuation_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.updated",
        property_id=row.property_id,
        payload={
            "valuation_id": row.id,
            "estimated_value": row.estimated_value,
        },
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)
    if old_property_id != row.property_id:
        sync_property_state(db, org_id=p.org_id, property_id=old_property_id)

    db.commit()
    db.refresh(row)
    return row


@router.delete("/valuations/{valuation_id}")
def delete_valuation(
    valuation_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Valuation).where(Valuation.id == valuation_id, Valuation.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="valuation not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="cash",
        action="delete valuation",
    )

    prop_id = row.property_id
    before = _valuation_payload(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.delete",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=before,
        after=None,
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.deleted",
        property_id=row.property_id,
        payload={"valuation_id": row.id},
    )

    db.delete(row)
    db.flush()

    sync_property_state(db, org_id=p.org_id, property_id=prop_id)

    db.commit()
    return {"ok": True}


@router.get("/valuation/suggestions", response_model=dict)
def valuation_suggestions(
    property_id: int,
    cadence: str = Query(default="quarterly", description="quarterly|monthly"),
    count: int = Query(default=4, ge=1, le=24),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="cash",
        action="view valuation suggestions",
    )
    must_get_property(db, org_id=p.org_id, property_id=property_id)

    latest = db.scalar(
        select(Valuation)
        .where(Valuation.org_id == p.org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.as_of), desc(Valuation.id))
        .limit(1)
    )

    base = latest.as_of if latest else datetime.utcnow()

    cadence_clean = (cadence or "quarterly").strip().lower()
    if cadence_clean not in {"quarterly", "monthly"}:
        raise HTTPException(status_code=400, detail="cadence must be quarterly or monthly")

    step_days = 90 if cadence_clean == "quarterly" else 30
    suggestions = []
    dt = base
    for _ in range(count):
        dt = dt + timedelta(days=step_days)
        suggestions.append({"suggested_as_of": dt.isoformat()})

    return {
        "property_id": property_id,
        "cadence": cadence_clean,
        "latest_valuation_as_of": latest.as_of.isoformat() if latest and latest.as_of else None,
        "suggestions": suggestions,
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False),
    }