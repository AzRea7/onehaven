# backend/app/routers/cash.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Transaction
from ..schemas import TransactionCreate, TransactionOut

router = APIRouter(prefix="/cash", tags=["cash"])


@router.post("/transactions", response_model=TransactionOut)
def create_txn(payload: TransactionCreate, db: Session = Depends(get_db)):
    data = payload.model_dump()

    # Back-compat: allow clients to send {"type": "..."} instead of {"txn_type": "..."}
    if not data.get("txn_type") and data.get("type"):
        data["txn_type"] = data.pop("type")

    if not data.get("txn_date"):
        data["txn_date"] = datetime.utcnow()

    row = Transaction(**data)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/transactions", response_model=list[TransactionOut])
def list_txns(
    property_id: int | None = Query(default=None),
    txn_type: str | None = Query(default=None),
    # Back-compat query param: ?type=income
    type: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    effective_type = (txn_type or type)
    q = select(Transaction).order_by(desc(Transaction.txn_date), desc(Transaction.id))
    if property_id is not None:
        q = q.where(Transaction.property_id == property_id)
    if effective_type:
        q = q.where(Transaction.txn_type == effective_type)

    return list(db.scalars(q.limit(limit)).all())
