from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.auth import get_principal
from onehaven_platform.backend.src.db import get_db
from onehaven_platform.backend.src.domain.audit import emit_audit
from onehaven_platform.backend.src.models import Property, Valuation
from onehaven_platform.backend.src.schemas import ValuationCreate, ValuationOut
from onehaven_platform.backend.src.services.events_facade import wf
from onehaven_platform.backend.src.services.ownership import must_get_property
from onehaven_platform.backend.src.services.state_machine_service import sync_property_state
from onehaven_platform.backend.src.services.stage_guard_service import require_stage
from products.compliance.backend.src.services import build_workflow_summary

router = APIRouter(prefix="/equity", tags=["equity"])


def _now() -> datetime:
    return datetime.utcnow()


def _valuation_payload(row: Valuation) -> dict[str, Any]:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "as_of": row.as_of.isoformat() if row.as_of else None,
        "estimated_value": row.estimated_value,
        "loan_balance": row.loan_balance,
        "notes": row.notes,
    }


def _valuation_row(row: Valuation) -> dict[str, Any]:
    estimated_value = float(row.estimated_value or 0.0)
    loan_balance = float(row.loan_balance or 0.0) if row.loan_balance is not None else 0.0
    equity = estimated_value - loan_balance
    ltv = round((loan_balance / estimated_value) * 100.0, 2) if estimated_value > 0 else None

    return {
        **_valuation_payload(row),
        "estimated_equity": round(equity, 2),
        "ltv_pct": ltv,
    }


@router.post("/valuations", response_model=ValuationOut)
def create_valuation(
    payload: ValuationCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        min_stage="cash",
        action="create valuation",
    )

    data = payload.model_dump()
    data["org_id"] = p.org_id
    data.setdefault("as_of", _now())

    row = Valuation(**data)
    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.create",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=None,
        after=_valuation_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.created",
        property_id=row.property_id,
        payload={
            "valuation_id": row.id,
            "as_of": row.as_of.isoformat() if row.as_of else None,
            "estimated_value": row.estimated_value,
        },
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)

    db.commit()
    db.refresh(row)
    return row


@router.get("/valuations", response_model=list[ValuationOut])
def list_valuations(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(Valuation).where(Valuation.org_id == p.org_id)

    if property_id is not None:
        prop = db.get(Property, property_id)
        if not prop or prop.org_id != p.org_id:
            raise HTTPException(status_code=404, detail="property not found")
        require_stage(
            db,
            org_id=p.org_id,
            property_id=property_id,
            min_stage="cash",
            action="view valuations",
        )
        q = q.where(Valuation.property_id == property_id)

    q = q.order_by(desc(Valuation.as_of), desc(Valuation.id)).limit(limit)
    return list(db.scalars(q).all())


@router.patch("/valuations/{valuation_id}", response_model=ValuationOut)
def update_valuation(
    valuation_id: int,
    payload: ValuationCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Valuation).where(Valuation.id == valuation_id, Valuation.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="valuation not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="cash",
        action="update valuation",
    )

    before = _valuation_payload(row)

    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    old_property_id = row.property_id

    for k, v in payload.model_dump().items():
        setattr(row, k, v)

    row.org_id = p.org_id
    db.add(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.update",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=before,
        after=_valuation_payload(row),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.updated",
        property_id=row.property_id,
        payload={
            "valuation_id": row.id,
            "estimated_value": row.estimated_value,
        },
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)
    if old_property_id != row.property_id:
        sync_property_state(db, org_id=p.org_id, property_id=old_property_id)

    db.commit()
    db.refresh(row)
    return row


@router.delete("/valuations/{valuation_id}")
def delete_valuation(
    valuation_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(select(Valuation).where(Valuation.id == valuation_id, Valuation.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="valuation not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="cash",
        action="delete valuation",
    )

    prop_id = row.property_id
    before = _valuation_payload(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="valuation.delete",
        entity_type="Valuation",
        entity_id=str(row.id),
        before=before,
        after=None,
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="valuation.deleted",
        property_id=row.property_id,
        payload={"valuation_id": row.id},
    )

    db.delete(row)
    db.flush()

    sync_property_state(db, org_id=p.org_id, property_id=prop_id)

    db.commit()
    return {"ok": True}


@router.get("/property/{property_id}/snapshot", response_model=dict)
def equity_snapshot(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="cash",
        action="view equity snapshot",
    )

    rows = list(
        db.scalars(
            select(Valuation)
            .where(Valuation.org_id == p.org_id, Valuation.property_id == property_id)
            .order_by(desc(Valuation.as_of), desc(Valuation.id))
        ).all()
    )

    latest = rows[0] if rows else None
    oldest = rows[-1] if rows else None

    latest_row = _valuation_row(latest) if latest else None
    oldest_row = _valuation_row(oldest) if oldest else None

    appreciation_value = None
    appreciation_pct = None
    if latest and oldest:
        base = float(oldest.estimated_value or 0.0)
        current = float(latest.estimated_value or 0.0)
        appreciation_value = round(current - base, 2)
        appreciation_pct = round(((current - base) / base) * 100.0, 2) if base > 0 else None

    return {
        "property_id": property_id,
        "has_valuation": latest is not None,
        "latest": latest_row,
        "first": oldest_row,
        "kpis": {
            "estimated_value": latest_row["estimated_value"] if latest_row else None,
            "loan_balance": latest_row["loan_balance"] if latest_row else None,
            "estimated_equity": latest_row["estimated_equity"] if latest_row else None,
            "ltv_pct": latest_row["ltv_pct"] if latest_row else None,
            "valuation_count": len(rows),
            "appreciation_value": appreciation_value,
            "appreciation_pct": appreciation_pct,
        },
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False),
    }


@router.get("/property/{property_id}/timeline", response_model=dict)
def equity_timeline(
    property_id: int,
    limit: int = Query(default=24, ge=1, le=240),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="cash",
        action="view equity timeline",
    )

    rows = list(
        db.scalars(
            select(Valuation)
            .where(Valuation.org_id == p.org_id, Valuation.property_id == property_id)
            .order_by(desc(Valuation.as_of), desc(Valuation.id))
            .limit(limit)
        ).all()
    )

    items = [_valuation_row(x) for x in reversed(rows)]
    return {
        "property_id": property_id,
        "items": items,
        "count": len(items),
    }


@router.get("/valuation/suggestions", response_model=dict)
def valuation_suggestions(
    property_id: int,
    cadence: str = Query(default="quarterly", description="quarterly|monthly"),
    count: int = Query(default=4, ge=1, le=24),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="cash",
        action="view valuation suggestions",
    )
    must_get_property(db, org_id=p.org_id, property_id=property_id)

    latest = db.scalar(
        select(Valuation)
        .where(Valuation.org_id == p.org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.as_of), desc(Valuation.id))
        .limit(1)
    )

    base = latest.as_of if latest else _now()

    cadence_clean = (cadence or "quarterly").strip().lower()
    if cadence_clean not in {"quarterly", "monthly"}:
        raise HTTPException(status_code=400, detail="cadence must be quarterly or monthly")

    step_days = 90 if cadence_clean == "quarterly" else 30
    suggestions = []
    dt = base
    for _ in range(count):
        dt = dt + timedelta(days=step_days)
        suggestions.append({"suggested_as_of": dt.isoformat()})

    return {
        "property_id": property_id,
        "cadence": cadence_clean,
        "latest_valuation_as_of": latest.as_of.isoformat() if latest and latest.as_of else None,
        "suggestions": suggestions,
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False),
    }
