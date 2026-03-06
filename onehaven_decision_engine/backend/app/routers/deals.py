from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.audit import emit_audit
from ..domain.operating_truth_enforcement import enforce_constitution_for_property_and_price
from ..models import Deal, ImportSnapshot, Property, RentAssumption, UnderwritingResult
from ..schemas import (
    DealCreate,
    DealIntakeIn,
    DealIntakeOut,
    DealOut,
    PropertyOut,
    RentAssumptionOut,
    RentAssumptionUpsert,
    SurvivorOut,
)
from ..services.events_facade import wf
from ..services.property_state_machine import sync_property_state

router = APIRouter(prefix="/deals", tags=["deals"])


def _get_or_create_manual_snapshot(db: Session, *, org_id: int) -> ImportSnapshot:
    snap = db.scalar(
        select(ImportSnapshot).where(
            ImportSnapshot.org_id == int(org_id),
            ImportSnapshot.source == "manual",
        )
    )
    if snap:
        return snap

    snap = ImportSnapshot(
        org_id=int(org_id),
        source="manual",
        notes="Manual intake snapshot",
        created_at=datetime.utcnow(),
    )
    db.add(snap)
    db.flush()
    return snap


def _require_snapshot_belongs_to_org(db: Session, *, snapshot_id: int, org_id: int) -> ImportSnapshot:
    snap = db.get(ImportSnapshot, int(snapshot_id))
    if not snap or int(getattr(snap, "org_id", -1)) != int(org_id):
        raise HTTPException(status_code=400, detail="Invalid snapshot_id")
    return snap


def _require_property_belongs_to_org(db: Session, *, property_id: int, org_id: int) -> Property:
    prop = db.get(Property, int(property_id))
    if not prop or int(prop.org_id) != int(org_id):
        raise HTTPException(status_code=400, detail="Invalid property_id")
    return prop


def _maybe_apply_pipeline_fields(d: Deal, data: dict) -> None:
    if "decision" in data:
        d.decision = data.get("decision")
    if "purchase_price" in data:
        d.purchase_price = data.get("purchase_price")
    if "closing_date" in data:
        d.closing_date = data.get("closing_date")
    if "loan_amount" in data:
        d.loan_amount = data.get("loan_amount")


@router.get("/survivors", response_model=list[SurvivorOut])
def survivors(
    snapshot_id: int | None = None,
    decision: str = "PASS",
    min_dscr: float = 1.20,
    min_cashflow: float = 400.0,
    limit: int = 25,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = (
        select(Deal, Property, UnderwritingResult)
        .join(Property, Property.id == Deal.property_id)
        .join(UnderwritingResult, UnderwritingResult.deal_id == Deal.id)
        .where(Deal.org_id == p.org_id)
        .where(Property.org_id == p.org_id)
        .where(UnderwritingResult.org_id == p.org_id)
        .where(UnderwritingResult.decision == decision)
        .where(UnderwritingResult.dscr >= min_dscr)
        .where(UnderwritingResult.cash_flow >= min_cashflow)
        .order_by(desc(UnderwritingResult.score), desc(UnderwritingResult.dscr), desc(UnderwritingResult.cash_flow))
        .limit(limit)
    )

    if snapshot_id is not None:
        q = q.where(Deal.snapshot_id == int(snapshot_id))

    rows = db.execute(q).all()

    out: list[SurvivorOut] = []
    for d, prop, r in rows:
        out.append(
            SurvivorOut(
                deal_id=d.id,
                property_id=prop.id,
                address=prop.address,
                city=prop.city,
                zip=prop.zip,
                decision=r.decision,
                score=r.score,
                reasons=json.loads(r.reasons_json),
                dscr=r.dscr,
                cash_flow=r.cash_flow,
                gross_rent_used=r.gross_rent_used,
                asking_price=d.asking_price,
            )
        )

    return out


@router.post("/intake", response_model=DealIntakeOut)
def intake(payload: DealIntakeIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    strategy = (payload.strategy or "section8").strip().lower()
    if strategy not in {"section8", "market"}:
        raise HTTPException(status_code=400, detail="strategy must be 'section8' or 'market'")

    enforce_constitution_for_property_and_price(
        address=payload.address.strip(),
        city=payload.city.strip(),
        state=(payload.state or "MI").strip(),
        zip=payload.zip.strip(),
        bedrooms=int(payload.bedrooms),
        bathrooms=float(payload.bathrooms),
        asking_price=float(payload.purchase_price),
    )

    snap_id = payload.snapshot_id
    if snap_id is None:
        snap = _get_or_create_manual_snapshot(db, org_id=p.org_id)
        snap_id = int(snap.id)
    else:
        _require_snapshot_belongs_to_org(db, snapshot_id=int(snap_id), org_id=p.org_id)

    now = datetime.utcnow()

    prop = Property(
        org_id=p.org_id,
        address=payload.address.strip(),
        city=payload.city.strip(),
        state=(payload.state or "MI").strip(),
        zip=payload.zip.strip(),
        bedrooms=int(payload.bedrooms),
        bathrooms=float(payload.bathrooms),
        square_feet=payload.square_feet,
        year_built=payload.year_built,
        has_garage=bool(payload.has_garage),
        property_type=(payload.property_type or "single_family").strip(),
        created_at=now,
        updated_at=now,
    )
    db.add(prop)
    db.flush()

    d = Deal(
        org_id=p.org_id,
        property_id=int(prop.id),
        asking_price=float(payload.purchase_price),
        estimated_purchase_price=float(payload.purchase_price),
        rehab_estimate=float(payload.est_rehab or 0.0),
        strategy=strategy,
        financing_type=(payload.financing_type or "dscr").strip(),
        interest_rate=float(payload.interest_rate),
        term_years=int(payload.term_years),
        down_payment_pct=float(payload.down_payment_pct),
        snapshot_id=int(snap_id),
        created_at=now,
        updated_at=now,
    )
    db.add(d)
    db.flush()

    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.property_id == int(prop.id))
        .where(RentAssumption.org_id == p.org_id)
    )
    if ra is None:
        ra = RentAssumption(
            property_id=int(prop.id),
            org_id=p.org_id,
            created_at=now,
        )
        db.add(ra)
        db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="deal.intake",
        entity_type="Deal",
        entity_id=str(d.id),
        before=None,
        after={
            "deal_id": d.id,
            "property_id": prop.id,
            "strategy": d.strategy,
            "asking_price": d.asking_price,
        },
    )
    wf.emit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="deal.intake_created",
        property_id=prop.id,
        payload={"deal_id": d.id, "property_id": prop.id},
    )

    sync_property_state(db, org_id=p.org_id, property_id=prop.id)
    db.commit()
    db.refresh(prop)
    db.refresh(d)

    return DealIntakeOut(
        property=PropertyOut.model_validate(prop, from_attributes=True),
        deal=DealOut.model_validate(d, from_attributes=True),
    )


@router.post("", response_model=DealOut)
def create_deal(payload: DealCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = _require_property_belongs_to_org(db, property_id=int(payload.property_id), org_id=p.org_id)

    data = payload.model_dump()
    data["strategy"] = (data.get("strategy") or "section8").strip().lower()
    if data["strategy"] not in {"section8", "market"}:
        raise HTTPException(status_code=400, detail="strategy must be 'section8' or 'market'")

    if data.get("snapshot_id") is None:
        snap = _get_or_create_manual_snapshot(db, org_id=p.org_id)
        data["snapshot_id"] = int(snap.id)
    else:
        _require_snapshot_belongs_to_org(db, snapshot_id=int(data["snapshot_id"]), org_id=p.org_id)

    enforce_constitution_for_property_and_price(
        address=prop.address,
        city=prop.city,
        state=prop.state,
        zip=prop.zip,
        bedrooms=int(prop.bedrooms),
        bathrooms=float(prop.bathrooms),
        asking_price=float(data.get("asking_price") or data.get("estimated_purchase_price") or 0.0),
    )

    data["org_id"] = p.org_id
    d = Deal(**data)
    db.add(d)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="deal.create",
        entity_type="Deal",
        entity_id=str(d.id),
        before=None,
        after={
            "deal_id": d.id,
            "property_id": d.property_id,
            "strategy": d.strategy,
            "asking_price": d.asking_price,
            "decision": getattr(d, "decision", None),
        },
    )
    wf.emit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="deal.created",
        property_id=d.property_id,
        payload={"deal_id": d.id},
    )

    sync_property_state(db, org_id=p.org_id, property_id=d.property_id)
    db.commit()
    db.refresh(d)
    return d


@router.put("/{deal_id}", response_model=DealOut)
def update_deal(deal_id: int, payload: DealCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    d = db.get(Deal, int(deal_id))
    if not d or int(d.org_id) != int(p.org_id):
        raise HTTPException(status_code=404, detail="Deal not found")

    before = {
        "property_id": d.property_id,
        "asking_price": d.asking_price,
        "estimated_purchase_price": d.estimated_purchase_price,
        "rehab_estimate": d.rehab_estimate,
        "strategy": d.strategy,
        "financing_type": d.financing_type,
        "interest_rate": d.interest_rate,
        "term_years": d.term_years,
        "down_payment_pct": d.down_payment_pct,
        "decision": getattr(d, "decision", None),
        "purchase_price": getattr(d, "purchase_price", None),
        "closing_date": getattr(d, "closing_date", None).isoformat() if getattr(d, "closing_date", None) else None,
        "loan_amount": getattr(d, "loan_amount", None),
    }

    prop = _require_property_belongs_to_org(db, property_id=int(payload.property_id), org_id=p.org_id)

    data = payload.model_dump()
    data["strategy"] = (data.get("strategy") or "section8").strip().lower()
    if data["strategy"] not in {"section8", "market"}:
        raise HTTPException(status_code=400, detail="strategy must be 'section8' or 'market'")

    if data.get("snapshot_id") is not None:
        _require_snapshot_belongs_to_org(db, snapshot_id=int(data["snapshot_id"]), org_id=p.org_id)

    enforce_constitution_for_property_and_price(
        address=prop.address,
        city=prop.city,
        state=prop.state,
        zip=prop.zip,
        bedrooms=int(prop.bedrooms),
        bathrooms=float(prop.bathrooms),
        asking_price=float(data.get("asking_price") or data.get("estimated_purchase_price") or 0.0),
    )

    old_property_id = int(d.property_id)

    d.property_id = int(data["property_id"])
    d.asking_price = float(data.get("asking_price") or d.asking_price or 0.0)
    d.estimated_purchase_price = float(data.get("estimated_purchase_price") or d.estimated_purchase_price or 0.0)
    d.rehab_estimate = float(data.get("rehab_estimate") or d.rehab_estimate or 0.0)
    d.strategy = data["strategy"]
    d.financing_type = (data.get("financing_type") or d.financing_type or "dscr").strip()
    d.interest_rate = float(data.get("interest_rate") or d.interest_rate or 0.0)
    d.term_years = int(data.get("term_years") or d.term_years or 30)
    d.down_payment_pct = float(data.get("down_payment_pct") or d.down_payment_pct or 0.0)
    d.updated_at = datetime.utcnow()

    _maybe_apply_pipeline_fields(d, data)

    if data.get("snapshot_id") is None:
        snap = _get_or_create_manual_snapshot(db, org_id=p.org_id)
        d.snapshot_id = int(snap.id)
    else:
        d.snapshot_id = int(data["snapshot_id"])

    db.add(d)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="deal.update",
        entity_type="Deal",
        entity_id=str(d.id),
        before=before,
        after={
            "property_id": d.property_id,
            "asking_price": d.asking_price,
            "estimated_purchase_price": d.estimated_purchase_price,
            "rehab_estimate": d.rehab_estimate,
            "strategy": d.strategy,
            "financing_type": d.financing_type,
            "interest_rate": d.interest_rate,
            "term_years": d.term_years,
            "down_payment_pct": d.down_payment_pct,
            "decision": getattr(d, "decision", None),
            "purchase_price": getattr(d, "purchase_price", None),
            "closing_date": getattr(d, "closing_date", None).isoformat() if getattr(d, "closing_date", None) else None,
            "loan_amount": getattr(d, "loan_amount", None),
        },
    )
    wf.emit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="deal.updated",
        property_id=d.property_id,
        payload={"deal_id": d.id},
    )

    sync_property_state(db, org_id=p.org_id, property_id=d.property_id)
    if old_property_id != int(d.property_id):
        sync_property_state(db, org_id=p.org_id, property_id=old_property_id)

    db.commit()
    db.refresh(d)
    return d


@router.get("/{deal_id}", response_model=DealOut)
def get_deal(deal_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    d = db.get(Deal, int(deal_id))
    if not d or int(d.org_id) != int(p.org_id):
        raise HTTPException(status_code=404, detail="Deal not found")
    return d


@router.put("/property/{property_id}/rent", response_model=RentAssumptionOut)
def upsert_rent(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, int(property_id))
    if not prop or int(prop.org_id) != int(p.org_id):
        raise HTTPException(status_code=404, detail="Property not found")

    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.property_id == int(property_id))
        .where(RentAssumption.org_id == p.org_id)
    )
    data = payload.model_dump(exclude_unset=True)

    if ra is None:
        ra = RentAssumption(property_id=int(property_id), org_id=p.org_id, **data, created_at=datetime.utcnow())
        db.add(ra)
    else:
        for k, v in data.items():
            setattr(ra, k, v)

    db.flush()

    wf.emit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="rent_assumption.updated",
        property_id=property_id,
        payload={"property_id": property_id},
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)

    db.commit()
    db.refresh(ra)
    return ra


@router.get("/property/{property_id}/rent", response_model=RentAssumptionOut)
def get_rent(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, int(property_id))
    if not prop or int(prop.org_id) != int(p.org_id):
        raise HTTPException(status_code=404, detail="Property not found")

    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.property_id == int(property_id))
        .where(RentAssumption.org_id == p.org_id)
    )
    if not ra:
        raise HTTPException(status_code=404, detail="Rent assumptions not found")
    return ra
