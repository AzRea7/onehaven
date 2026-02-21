# backend/app/routers/cash.py
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Transaction, Lease
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


@router.get("/rollup", response_model=dict)
def cash_rollup(
    property_id: int,
    year: int = Query(..., ge=2000, le=2200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Phase 4: portfolio-grade cash rollups (simple v1).
    Returns months with:
      - expected rent (leases)
      - collected income (transactions)
      - expenses/capex (transactions)
      - net + delta vs expected
    """
    must_get_property(db, org_id=p.org_id, property_id=property_id)

    leases = db.scalars(
        select(Lease).where(Lease.org_id == p.org_id, Lease.property_id == property_id)
    ).all()

    txns = db.scalars(
        select(Transaction).where(Transaction.org_id == p.org_id, Transaction.property_id == property_id)
    ).all()

    expected = {f"{year}-{m:02d}": 0.0 for m in range(1, 13)}
    collected = {f"{year}-{m:02d}": 0.0 for m in range(1, 13)}
    expenses = {f"{year}-{m:02d}": 0.0 for m in range(1, 13)}

    def month_start(y: int, m: int) -> datetime:
        return datetime(y, m, 1)

    def next_month_start(y: int, m: int) -> datetime:
        if m == 12:
            return datetime(y + 1, 1, 1)
        return datetime(y, m + 1, 1)

    # Expected rent: if a lease overlaps a month, add total_rent for that month (v1)
    for l in leases:
        start = l.start_date
        end = l.end_date or datetime(2100, 1, 1)

        for m in range(1, 13):
            ms = month_start(year, m)
            me = next_month_start(year, m)
            overlaps = (start < me) and (ms < end)
            if overlaps:
                expected[f"{year}-{m:02d}"] += float(l.total_rent or 0.0)

    # Collected + expenses from transactions
    for t in txns:
        d = t.txn_date or datetime.utcnow()
        if d.year != year:
            continue

        key = f"{d.year}-{d.month:02d}"
        typ = (t.txn_type or "other").lower()
        amt = float(t.amount or 0.0)

        if typ in {"income", "rent"}:
            collected[key] += amt
        elif typ in {"expense", "capex"}:
            expenses[key] += abs(amt) if amt < 0 else amt
        else:
            # heuristic: positive => income, negative => expense
            if amt >= 0:
                collected[key] += amt
            else:
                expenses[key] += abs(amt)

    months = []
    for m in range(1, 13):
        key = f"{year}-{m:02d}"
        exp = expected[key]
        col = collected[key]
        expn = expenses[key]
        months.append(
            {
                "month": key,
                "expected_rent": round(exp, 2),
                "collected_income": round(col, 2),
                "expenses": round(expn, 2),
                "net": round(col - expn, 2),
                "delta_vs_expected": round(col - exp, 2),
            }
        )

    return {"property_id": property_id, "year": year, "months": months}