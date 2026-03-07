from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.audit import emit_audit
from ..models import Lease, Transaction
from ..schemas import TransactionCreate, TransactionOut
from ..services.events_facade import wf
from ..services.ownership import must_get_property
from ..services.property_state_machine import sync_property_state
from ..services.stage_guard import require_stage
from ..services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/cash", tags=["cash"])


def _txn_payload(row: Transaction) -> dict:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "txn_date": row.txn_date.isoformat() if row.txn_date else None,
        "txn_type": row.txn_type,
        "amount": row.amount,
        "memo": row.memo,
    }


@router.post("/transactions", response_model=TransactionOut)
def create_txn(
    payload: TransactionCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        min_stage="lease",
        action="create cash transaction",
    )

    data = payload.model_dump()
    data["org_id"] = p.org_id
    data.setdefault("txn_date", datetime.utcnow())

    row = Transaction(**data)
    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="transaction.create",
        entity_type="Transaction",
        entity_id=str(row.id),
        before=None,
        after=_txn_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="transaction.created",
        property_id=row.property_id,
        payload={"transaction_id": row.id, "txn_type": row.txn_type, "amount": row.amount},
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)

    db.commit()
    db.refresh(row)
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
        require_stage(
            db,
            org_id=p.org_id,
            property_id=property_id,
            min_stage="lease",
            action="view cash transactions",
        )
        q = q.where(Transaction.property_id == property_id)

    if txn_type:
        q = q.where(Transaction.txn_type == txn_type)

    q = q.order_by(desc(Transaction.txn_date), desc(Transaction.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/transactions/{transaction_id}", response_model=TransactionOut)
def update_txn(
    transaction_id: int,
    payload: TransactionCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Transaction).where(Transaction.id == transaction_id, Transaction.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="transaction not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="lease",
        action="update cash transaction",
    )

    before = _txn_payload(row)

    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)

    old_property_id = row.property_id
    data = payload.model_dump()
    for k, v in data.items():
        setattr(row, k, v)

    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="transaction.update",
        entity_type="Transaction",
        entity_id=str(row.id),
        before=before,
        after=_txn_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="transaction.updated",
        property_id=row.property_id,
        payload={"transaction_id": row.id, "txn_type": row.txn_type, "amount": row.amount},
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)
    if old_property_id != row.property_id:
        sync_property_state(db, org_id=p.org_id, property_id=old_property_id)

    db.commit()
    db.refresh(row)
    return row


@router.delete("/transactions/{transaction_id}")
def delete_txn(
    transaction_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Transaction).where(Transaction.id == transaction_id, Transaction.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="transaction not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="lease",
        action="delete cash transaction",
    )

    prop_id = row.property_id
    before = _txn_payload(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="transaction.delete",
        entity_type="Transaction",
        entity_id=str(row.id),
        before=before,
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
    db.flush()

    sync_property_state(db, org_id=p.org_id, property_id=prop_id)

    db.commit()
    return {"ok": True}


@router.get("/rollup", response_model=dict)
def cash_rollup(
    property_id: int,
    year: int = Query(..., ge=2000, le=2200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="lease",
        action="view cash rollup",
    )

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

    for l in leases:
        start = l.start_date
        end = l.end_date or datetime(2100, 1, 1)

        for m in range(1, 13):
            ms = month_start(year, m)
            me = next_month_start(year, m)
            overlaps = (start < me) and (ms < end)
            if overlaps:
                expected[f"{year}-{m:02d}"] += float(l.total_rent or 0.0)

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

    return {
        "property_id": property_id,
        "year": year,
        "months": months,
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False),
    }
