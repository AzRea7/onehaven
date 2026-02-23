# backend/app/services/dashboard_rollups.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..models import (
    Property,
    PropertyChecklistItem,
    Inspection,
    RehabTask,
    Transaction,
    Valuation,
)
from ..services.property_state_machine import get_state_payload


def _compliance_bucket(db: Session, *, org_id: int, property_id: int) -> str:
    items = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()
    if not items:
        return "no_checklist"

    total = len(items)
    done = sum(1 for x in items if (x.status or "").lower() == "done")
    failed = sum(1 for x in items if (x.status or "").lower() == "failed")
    pct_done = (done / total) if total else 0.0

    latest_insp = db.scalar(
        select(Inspection).where(Inspection.property_id == property_id).order_by(desc(Inspection.id)).limit(1)
    )
    latest_passed = bool(latest_insp.passed) if latest_insp else False

    passed = (pct_done >= 0.95) and (failed == 0) and latest_passed
    return "passed" if passed else "failing"


def compute_rollups(db: Session, *, org_id: int, state: str = "MI", limit: int = 500) -> dict[str, Any]:
    prop_ids = [
        r[0]
        for r in db.execute(
            select(Property.id)
            .where(Property.org_id == org_id, Property.state == state)
            .order_by(desc(Property.id))
            .limit(limit)
        ).all()
    ]

    # Stage counts + next actions
    stage_counts: dict[str, int] = {}
    properties_with_next_actions = 0

    # Compliance health
    compliance_counts = {"passed": 0, "failing": 0, "no_checklist": 0}

    # Rehab
    rehab_open = 0
    rehab_estimated_total = 0.0
    rehab_actual_total = 0.0

    for pid in prop_ids:
        st = get_state_payload(db, org_id=org_id, property_id=pid, recompute=True)
        stage = str(st.get("current_stage") or "deal")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        if (st.get("next_actions") or []):
            properties_with_next_actions += 1

        bucket = _compliance_bucket(db, org_id=org_id, property_id=pid)
        compliance_counts[bucket] = compliance_counts.get(bucket, 0) + 1

        tasks = db.scalars(
            select(RehabTask).where(RehabTask.org_id == org_id, RehabTask.property_id == pid)
        ).all()
        for t in tasks:
            if (t.status or "").lower() != "done":
                rehab_open += 1
            rehab_estimated_total += float(t.estimated_cost or 0.0)
            rehab_actual_total += float(t.actual_cost or 0.0)

    # Cashflow rollup (last 30 days)
    since = datetime.utcnow() - timedelta(days=30)
    txns = db.scalars(
        select(Transaction).where(Transaction.org_id == org_id, Transaction.txn_date >= since)
    ).all()
    income = sum(float(t.amount) for t in txns if (t.txn_type or "").lower() == "income")
    expense = sum(float(t.amount) for t in txns if (t.txn_type or "").lower() == "expense")
    capex = sum(float(t.amount) for t in txns if (t.txn_type or "").lower() == "capex")
    net_30d = income - expense - capex

    # Equity rollup: latest valuation per property + delta vs previous valuation
    latest_vals = {}
    delta_total = 0.0
    have_delta = 0

    for pid in prop_ids:
        vals = db.scalars(
            select(Valuation)
            .where(Valuation.org_id == org_id, Valuation.property_id == pid)
            .order_by(desc(Valuation.as_of), desc(Valuation.id))
            .limit(2)
        ).all()
        if not vals:
            continue
        latest = vals[0]
        latest_vals[pid] = float(latest.value or 0.0)

        if len(vals) >= 2:
            prev = vals[1]
            delta_total += float(latest.value or 0.0) - float(prev.value or 0.0)
            have_delta += 1

    return {
        "properties": len(prop_ids),
        "stage_counts": stage_counts,
        "properties_with_next_actions": properties_with_next_actions,
        "compliance_counts": compliance_counts,
        "rehab": {
            "open_tasks": rehab_open,
            "estimated_total": round(rehab_estimated_total, 2),
            "actual_total": round(rehab_actual_total, 2),
        },
        "cashflow_30d": {
            "income": round(income, 2),
            "expense": round(expense, 2),
            "capex": round(capex, 2),
            "net": round(net_30d, 2),
        },
        "equity": {
            "properties_with_valuation": len(latest_vals),
            "avg_delta_if_available": round((delta_total / have_delta), 2) if have_delta else 0.0,
            "properties_with_delta": have_delta,
        },
    }