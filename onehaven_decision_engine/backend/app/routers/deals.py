# backend/app/routers/deals.py
from __future__ import annotations

import json
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult
from ..schemas import DealCreate, DealOut, RentAssumptionUpsert, RentAssumptionOut, SurvivorOut

router = APIRouter(prefix="/deals", tags=["deals"])


@router.get("/survivors", response_model=list[SurvivorOut])
def survivors(
    snapshot_id: int | None = None,
    decision: str = "PASS",
    min_dscr: float = 1.20,
    min_cashflow: float = 400.0,
    limit: int = 25,
    db: Session = Depends(get_db),
):
    q = (
        select(Deal, Property, UnderwritingResult)
        .join(Property, Property.id == Deal.property_id)
        .join(UnderwritingResult, UnderwritingResult.deal_id == Deal.id)
        .where(UnderwritingResult.decision == decision)
        .where(UnderwritingResult.dscr >= min_dscr)
        .where(UnderwritingResult.cash_flow >= min_cashflow)
        .order_by(desc(UnderwritingResult.score), desc(UnderwritingResult.dscr), desc(UnderwritingResult.cash_flow))
        .limit(limit)
    )

    if snapshot_id is not None:
        q = q.where(Deal.snapshot_id == snapshot_id)

    rows = db.execute(q).all()

    out: list[SurvivorOut] = []
    for d, p, r in rows:
        out.append(
            SurvivorOut(
                deal_id=d.id,
                property_id=p.id,
                address=p.address,
                city=p.city,
                zip=p.zip,
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


@router.post("", response_model=DealOut)
def create_deal(payload: DealCreate, db: Session = Depends(get_db)):
    prop = db.get(Property, payload.property_id)
    if not prop:
        raise HTTPException(status_code=400, detail="Invalid property_id")

    data = payload.model_dump()
    data["strategy"] = (data.get("strategy") or "section8").strip().lower()

    if data["strategy"] not in {"section8", "market"}:
        raise HTTPException(status_code=400, detail="strategy must be 'section8' or 'market'")

    d = Deal(**data)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


@router.get("/{deal_id}", response_model=DealOut)
def get_deal(deal_id: int, db: Session = Depends(get_db)):
    d = db.get(Deal, deal_id)
    if not d:
        raise HTTPException(status_code=404, detail="Deal not found")
    return d


@router.put("/property/{property_id}/rent", response_model=RentAssumptionOut)
def upsert_rent(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    data = payload.model_dump(exclude_unset=True)

    if ra is None:
        ra = RentAssumption(property_id=property_id, **data)
        db.add(ra)
    else:
        for k, v in data.items():
            setattr(ra, k, v)

    db.commit()
    db.refresh(ra)
    return ra


@router.get("/property/{property_id}/rent", response_model=RentAssumptionOut)
def get_rent(property_id: int, db: Session = Depends(get_db)):
    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    if not ra:
        raise HTTPException(status_code=404, detail="Rent assumptions not found")
    return ra
