from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Transaction, Property
from ..schemas import TransactionCreate, TransactionOut
from ..domain.audit import emit_audit

router = APIRouter(prefix="/cash", tags=["cash"])


@router.post("/transactions", response_model=TransactionOut)
def create_txn(payload: TransactionCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    data = payload.model_dump()
    data["org_id"] = p.org_id
    data.setdefault("txn_date", datetime.utcnow())

    row = Transaction(**data)
    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="transaction.create",
        entity_type="Transaction",
        entity_id=str(row.id),
        before=None,
        after=row.model_dump(),
    )
    db.commit()

    return row


@router.get("/transactions", response_model=list[TransactionOut])
def list_txns(
    property_id: int | None = Query(default=None),
    txn_type: str | None = Query(default=None),
    limit: int = Query(default=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Transaction).where(Transaction.org_id == p.org_id)

    if property_id:
        q = q.where(Transaction.property_id == property_id)
    if txn_type:
        q = q.where(Transaction.txn_type == txn_type)

    q = q.order_by(desc(Transaction.txn_date)).limit(limit)
    return list(db.scalars(q).all())
