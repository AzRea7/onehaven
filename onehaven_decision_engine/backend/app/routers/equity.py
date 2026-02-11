# backend/app/routers/equity.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Valuation
from ..schemas import ValuationCreate, ValuationOut

router = APIRouter(prefix="/equity", tags=["equity"])


@router.post("/valuations", response_model=ValuationOut)
def create_valuation(payload: ValuationCreate, db: Session = Depends(get_db)):
    data = payload.model_dump()
    if not data.get("as_of"):
        data["as_of"] = datetime.utcnow()
    row = Valuation(**data)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/valuations", response_model=list[ValuationOut])
def list_valuations(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    q = select(Valuation).order_by(desc(Valuation.as_of), desc(Valuation.id))
    if property_id is not None:
        q = q.where(Valuation.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())
