from __future__ import annotations

from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Property, RentAssumption, RentComp, RentObservation, RentCalibration
from ..schemas import (
    RentAssumptionOut,
    RentAssumptionUpsert,
    RentCompsBatchIn,
    RentCompOut,
    RentCompsSummaryOut,
    RentObservationCreate,
    RentObservationOut,
    RentCalibrationOut,
    RentRecomputeOut,
)
from ..domain.rent_learning import (
    get_or_create_rent_assumption,
    summarize_comps,
    update_calibration_from_observation,
    recompute_rent_fields,
)

router = APIRouter(prefix="/rent", tags=["rent"])


@router.get("/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption(property_id: int, db: Session = Depends(get_db)):
    ra = db.execute(select(RentAssumption).where(RentAssumption.property_id == property_id)).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")
    return ra


# ✅ alias: your earlier curl used /rent/assumption/{id}
@router.get("/assumption/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption_alias(property_id: int, db: Session = Depends(get_db)):
    return get_rent_assumption(property_id=property_id, db=db)


@router.post("/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(ra, k, v)

    db.add(ra)
    db.commit()
    db.refresh(ra)
    return ra


# ✅ alias POST route too
@router.post("/assumption/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption_alias(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db)):
    return upsert_rent_assumption(property_id=property_id, payload=payload, db=db)


@router.post("/comps/{property_id}", response_model=RentCompsSummaryOut)
def add_comps_batch(property_id: int, payload: RentCompsBatchIn, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)

    rents: List[float] = []
    for c in payload.comps:
        comp = RentComp(
            property_id=property_id,
            source=c.source,
            address=c.address,
            url=c.url,
            rent=float(c.rent),
            bedrooms=c.bedrooms,
            bathrooms=c.bathrooms,
            square_feet=c.square_feet,
            notes=c.notes,
            created_at=datetime.utcnow(),
        )
        db.add(comp)
        rents.append(float(c.rent))

    summary = summarize_comps(rents)

    # Persist median into rent_reasonableness_comp
    ra.rent_reasonableness_comp = summary.median_rent
    db.add(ra)

    db.commit()

    return RentCompsSummaryOut(
        property_id=property_id,
        count=summary.count,
        median_rent=summary.median_rent,
        mean_rent=summary.mean_rent,
        min_rent=summary.min_rent,
        max_rent=summary.max_rent,
    )


@router.get("/comps/{property_id}", response_model=list[RentCompOut])
def list_comps(property_id: int, db: Session = Depends(get_db)):
    rows = (
        db.execute(
            select(RentComp)
            .where(RentComp.property_id == property_id)
            .order_by(RentComp.created_at.desc(), RentComp.id.desc())
        )
        .scalars()
        .all()
    )
    return rows


@router.post("/observe", response_model=RentObservationOut)
def add_rent_observation(payload: RentObservationCreate, db: Session = Depends(get_db)):
    prop = db.get(Property, payload.property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, payload.property_id)

    obs = RentObservation(
        property_id=payload.property_id,
        strategy=payload.strategy,
        achieved_rent=float(payload.achieved_rent),
        tenant_portion=payload.tenant_portion,
        hap_portion=payload.hap_portion,
        lease_start=payload.lease_start,
        lease_end=payload.lease_end,
        notes=payload.notes,
        created_at=datetime.utcnow(),
    )
    db.add(obs)

    # Update calibration using *current* market estimate as the predictor
    update_calibration_from_observation(
        db,
        property_row=prop,
        strategy=payload.strategy,
        predicted_market_rent=ra.market_rent_estimate,
        achieved_rent=float(payload.achieved_rent),
    )

    db.commit()
    db.refresh(obs)
    return obs


@router.get("/calibration", response_model=list[RentCalibrationOut])
def list_calibration(
    zip: str | None = Query(default=None),
    bedrooms: int | None = Query(default=None),
    strategy: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    q = select(RentCalibration).order_by(RentCalibration.updated_at.desc())
    if zip:
        q = q.where(RentCalibration.zip == zip)
    if bedrooms is not None:
        q = q.where(RentCalibration.bedrooms == bedrooms)
    if strategy:
        q = q.where(RentCalibration.strategy == strategy)
    return db.execute(q).scalars().all()


@router.post("/recompute/{property_id}", response_model=RentRecomputeOut)
def recompute(
    property_id: int,
    strategy: str = Query(default="section8"),
    payment_standard_pct: float | None = None,
    db: Session = Depends(get_db),
):
    try:
        computed = recompute_rent_fields(
            db,
            property_id=property_id,
            strategy=strategy,
            payment_standard_pct=payment_standard_pct,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    ra = db.execute(select(RentAssumption).where(RentAssumption.property_id == property_id)).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    # Persist approved ceiling (computed) only if none set manually
    if ra.approved_rent_ceiling is None:
        ra.approved_rent_ceiling = computed["approved_rent_ceiling"]
        db.add(ra)
        db.commit()
        db.refresh(ra)

    return RentRecomputeOut(
        property_id=property_id,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ra.approved_rent_ceiling,
        calibrated_market_rent=computed["calibrated_market_rent"],
        strategy=strategy,
        rent_used=computed["rent_used"],
    )
