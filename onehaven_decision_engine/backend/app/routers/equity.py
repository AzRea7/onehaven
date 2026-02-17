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
    db.commit()

    return row


@router.get("/valuations", response_model=list[ValuationOut])
def list_valuations(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Valuation).where(Valuation.org_id == p.org_id)

    if property_id:
        q = q.where(Valuation.property_id == property_id)

    q = q.order_by(desc(Valuation.as_of)).limit(limit)
    return list(db.scalars(q).all())
