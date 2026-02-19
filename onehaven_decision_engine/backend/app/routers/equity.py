# backend/app/routers/equity.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Valuation, Property
from ..schemas import ValuationCreate, ValuationOut
from ..domain.audit import emit_audit

from ..services.events_facade import wf
from ..services.property_state_machine import advance_stage_if_needed

router = APIRouter(prefix="/equity", tags=["equity"])


@router.post("/valuations", response_model=ValuationOut)
def create_valuation(payload: ValuationCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    data = payload.model_dump()
    data["org_id"] = p.org_id
    data.setdefault("as_of", datetime.utcnow())

    row = Valuation(**data)
    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.create",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=None,
        after=row.model_dump(),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.created",
        property_id=row.property_id,
        payload={"valuation_id": row.id, "as_of": str(row.as_of), "value": row.value},
    )

    advance_stage_if_needed(db, org_id=p.org_id, property_id=row.property_id, suggested_stage="equity")

    db.commit()
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
        # verify property belongs to org (hard boundary)
        prop = db.get(Property, property_id)
        if not prop or prop.org_id != p.org_id:
            raise HTTPException(status_code=404, detail="property not found")
        q = q.where(Valuation.property_id == property_id)

    q = q.order_by(desc(Valuation.as_of), desc(Valuation.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/valuations/{valuation_id}", response_model=ValuationOut)
def update_valuation(
    valuation_id: int,
    payload: ValuationCreate,  # full-update for simplicity
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Valuation).where(Valuation.id == valuation_id, Valuation.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="valuation not found")

    before = row.model_dump()

    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    row.org_id = p.org_id

    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.update",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=before,
        after=row.model_dump(),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.updated",
        property_id=row.property_id,
        payload={"valuation_id": row.id, "value": row.value},
    )
    db.commit()
    return row


@router.delete("/valuations/{valuation_id}")
def delete_valuation(valuation_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(Valuation).where(Valuation.id == valuation_id, Valuation.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="valuation not found")

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.delete",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=row.model_dump(),
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
    db.commit()
    return {"ok": True}
