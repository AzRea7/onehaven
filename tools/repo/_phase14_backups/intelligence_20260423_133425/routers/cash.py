from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.auth import get_principal
from app.db import get_db
from app.domain.audit import emit_audit
from app.models import Lease, Transaction
from app.schemas import TransactionCreate, TransactionOut
from app.services.events_facade import wf
from app.services.ownership import must_get_property
from products.ops.backend.src.services.properties.state_machine import sync_property_state
from app.services.stage_guard import require_stage
from app.products.compliance.services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/cash", tags=["cash"])


def _now() -> datetime:
    return datetime.utcnow()


def _txn_type_norm(v: str | None) -> str:
    s = (v or "other").strip().lower()
    aliases = {
        "rent": "income",
        "income": "income",
        "hap": "income",
        "voucher": "income",
        "expense": "expense",
        "repair": "expense",
        "maintenance": "expense",
        "capex": "capex",
        "tax": "expense",
        "insurance": "expense",
        "mortgage": "expense",
    }
    return aliases.get(s, s if s else "other")


def _txn_bucket(txn_type: str | None) -> str:
    t = _txn_type_norm(txn_type)
    if t == "income":
        return "income"
    if t == "expense":
        return "expense"
    if t == "capex":
        return "capex"
    return "other"


def _txn_payload(row: Transaction) -> dict[str, Any]:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "txn_date": row.txn_date.isoformat() if row.txn_date else None,
        "txn_type": row.txn_type,
        "amount": row.amount,
        "memo": row.memo,
    }


def _cash_window(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    days: int,
) -> dict[str, float]:
    since = _now() - timedelta(days=days)
    rows = list(
        db.scalars(
            select(Transaction)
            .where(Transaction.org_id == org_id, Transaction.property_id == property_id)
            .order_by(desc(Transaction.txn_date), desc(Transaction.id))
        ).all()
    )

    income = 0.0
    expense = 0.0
    capex = 0.0
    other = 0.0

    for row in rows:
        dt = row.txn_date or _now()
        if dt < since:
            continue

        amt = float(row.amount or 0.0)
        bucket = _txn_bucket(getattr(row, "txn_type", None))

        if bucket == "income":
            income += amt
        elif bucket == "expense":
            expense += abs(amt)
        elif bucket == "capex":
            capex += abs(amt)
        else:
            other += amt

    operating_expenses = expense
    net = income - operating_expenses - capex

    return {
        "income": round(income, 2),
        "expense": round(operating_expenses, 2),
        "capex": round(capex, 2),
        "other": round(other, 2),
        "net": round(net, 2),
    }


def _expected_rent_for_window(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    days: int,
) -> float:
    rows = list(
        db.scalars(
            select(Lease)
            .where(Lease.org_id == org_id, Lease.property_id == property_id)
            .order_by(desc(Lease.start_date), desc(Lease.id))
        ).all()
    )
    now = _now()
    since = now - timedelta(days=days)

    total = 0.0
    for l in rows:
        start = l.start_date
        end = l.end_date or datetime(2100, 1, 1)
        if not start:
            continue
        overlaps = start <= now and end >= since
        if overlaps:
            total += float(l.total_rent or 0.0)
    return round(total, 2)


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
    data["txn_type"] = _txn_type_norm(data.get("txn_type") or data.get("type"))
    data.setdefault("txn_date", _now())

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
    days: int | None = Query(default=None, ge=1, le=3650),
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
        q = q.where(Transaction.txn_type == _txn_type_norm(txn_type))

    rows = list(db.scalars(q.order_by(desc(Transaction.txn_date), desc(Transaction.id)).limit(limit)).all())
    if days is not None:
        since = _now() - timedelta(days=days)
        rows = [x for x in rows if (x.txn_date or _now()) >= since]

    return rows


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
    data["txn_type"] = _txn_type_norm(data.get("txn_type") or data.get("type"))
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


@router.get("/property/{property_id}/snapshot", response_model=dict)
def cash_snapshot(
    property_id: int,
    days: int = Query(default=90, ge=7, le=365),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="lease",
        action="view cash snapshot",
    )

    recent = _cash_window(db, org_id=p.org_id, property_id=property_id, days=days)
    last_30 = _cash_window(db, org_id=p.org_id, property_id=property_id, days=30)
    expected_rent = _expected_rent_for_window(db, org_id=p.org_id, property_id=property_id, days=days)
    collection_rate = 0.0 if expected_rent <= 0 else round((recent["income"] / expected_rent) * 100.0, 2)

    return {
        "property_id": property_id,
        "days": days,
        "expected_rent_window": expected_rent,
        "collection_rate_pct": collection_rate,
        "last_30_days": last_30,
        f"last_{days}_days": recent,
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False),
    }


@router.get("/property/{property_id}/ledger", response_model=dict)
def cash_ledger(
    property_id: int,
    days: int = Query(default=180, ge=7, le=3650),
    limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="lease",
        action="view cash ledger",
    )

    since = _now() - timedelta(days=days)
    rows = list(
        db.scalars(
            select(Transaction)
            .where(Transaction.org_id == p.org_id, Transaction.property_id == property_id)
            .order_by(desc(Transaction.txn_date), desc(Transaction.id))
            .limit(limit)
        ).all()
    )
    rows = [x for x in rows if (x.txn_date or _now()) >= since]

    running = 0.0
    ledger_rows = []
    for row in sorted(rows, key=lambda x: (x.txn_date or _now(), x.id)):
        amt = float(row.amount or 0.0)
        bucket = _txn_bucket(row.txn_type)
        if bucket in {"expense", "capex"} and amt > 0:
            running -= amt
        else:
            running += amt

        ledger_rows.append(
            {
                **_txn_payload(row),
                "bucket": bucket,
                "running_cash_effect": round(running, 2),
            }
        )

    return {
        "property_id": property_id,
        "days": days,
        "rows": ledger_rows,
        "count": len(ledger_rows),
    }


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
    capex = {f"{year}-{m:02d}": 0.0 for m in range(1, 13)}

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
        d = t.txn_date or _now()
        if d.year != year:
            continue

        key = f"{d.year}-{d.month:02d}"
        bucket = _txn_bucket(t.txn_type)
        amt = float(t.amount or 0.0)

        if bucket == "income":
            collected[key] += amt
        elif bucket == "expense":
            expenses[key] += abs(amt)
        elif bucket == "capex":
            capex[key] += abs(amt)
        else:
            if amt >= 0:
                collected[key] += amt

    months = []
    total_expected = total_collected = total_expenses = total_capex = 0.0

    for m in range(1, 13):
        key = f"{year}-{m:02d}"
        exp = expected[key]
        col = collected[key]
        opx = expenses[key]
        cx = capex[key]
        net = col - opx - cx

        total_expected += exp
        total_collected += col
        total_expenses += opx
        total_capex += cx

        months.append(
            {
                "month": key,
                "expected_rent": round(exp, 2),
                "collected_income": round(col, 2),
                "expenses": round(opx, 2),
                "capex": round(cx, 2),
                "net": round(net, 2),
                "delta_vs_expected": round(col - exp, 2),
                "collection_rate_pct": round((col / exp) * 100.0, 2) if exp > 0 else 0.0,
            }
        )

    return {
        "property_id": property_id,
        "year": year,
        "months": months,
        "kpis": {
            "expected_rent": round(total_expected, 2),
            "collected_income": round(total_collected, 2),
            "expenses": round(total_expenses, 2),
            "capex": round(total_capex, 2),
            "noi_like": round(total_collected - total_expenses, 2),
            "net_after_capex": round(total_collected - total_expenses - total_capex, 2),
            "collection_rate_pct": round((total_collected / total_expected) * 100.0, 2)
            if total_expected > 0
            else 0.0,
        },
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False),
    }
