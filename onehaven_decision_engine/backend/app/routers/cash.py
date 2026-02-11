# backend/app/routers/cash.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Transaction
from ..schemas import TransactionCreate, TransactionOut

router = APIRouter(prefix="/cash", tags=["cash"])


@router.post("/transactions", response_model=TransactionOut)
def create_txn(payload: TransactionCreate, db: Session = Depends(get_db)):
    data = payload.model_dump()
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
    type: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    q = select(Transaction).order_by(desc(Transaction.txn_date), desc(Transaction.id))
    if property_id is not None:
        q = q.where(Transaction.property_id == property_id)
    if type:
        q = q.where(Transaction.type == type)
    return list(db.scalars(q.limit(limit)).all())
