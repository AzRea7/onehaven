# backend/app/routers/cash.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Transaction
from ..schemas import TransactionCreate, TransactionOut
from ..domain.audit import emit_audit

from ..services.ownership import must_get_property
from ..services.events_facade import wf
from ..services.property_state_machine import advance_stage_if_needed

router = APIRouter(prefix="/cash", tags=["cash"])


@router.post("/transactions", response_model=TransactionOut)
def create_txn(payload: TransactionCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)

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
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="transaction.created",
        property_id=row.property_id,
        payload={"transaction_id": row.id, "txn_type": row.txn_type, "amount": row.amount},
    )

    # Phase 4: cash activity pushes you into cash stage
    advance_stage_if_needed(db, org_id=p.org_id, property_id=row.property_id, suggested_stage="cash")

    db.commit()
    return row


@router.get("/transactions", response_model=list[TransactionOut])
def list_txns(
    property_id: int | None = Query(default=None),
    txn_type: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Transaction).where(Transaction.org_id == p.org_id)

    if property_id is not None:
        must_get_property(db, org_id=p.org_id, property_id=property_id)
        q = q.where(Transaction.property_id == property_id)

    if txn_type:
        q = q.where(Transaction.txn_type == txn_type)

    q = q.order_by(desc(Transaction.txn_date)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/transactions/{transaction_id}", response_model=TransactionOut)
def update_txn(
    transaction_id: int,
    payload: TransactionCreate,  # full-update for simplicity
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Transaction).where(Transaction.id == transaction_id, Transaction.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="transaction not found")

    before = row.model_dump()

    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)

    data = payload.model_dump()
    for k, v in data.items():
        setattr(row, k, v)

    db.add(row)
    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="transaction.update",
        entity_type="Transaction",
        entity_id=str(row.id),
        before=before,
        after=row.model_dump(),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="transaction.updated",
        property_id=row.property_id,
        payload={"transaction_id": row.id, "txn_type": row.txn_type, "amount": row.amount},
    )
    db.commit()
    return row


@router.delete("/transactions/{transaction_id}")
def delete_txn(transaction_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(Transaction).where(Transaction.id == transaction_id, Transaction.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="transaction not found")

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="transaction.delete",
        entity_type="Transaction",
        entity_id=str(row.id),
        before=row.model_dump(),
        after=None,
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="transaction.deleted",
        property_id=row.property_id,
        payload={"transaction_id": row.id},
    )

    db.delete(row)
    db.commit()
    return {"ok": True}
