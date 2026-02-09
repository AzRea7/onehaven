from __future__ import annotations

import json
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import Property, Deal, RentAssumption, ImportSnapshot
from ..schemas import ImportResultOut, ImportErrorRow
from ..domain.importers.base import parse_csv_bytes, fingerprint
from ..domain.importers.investorlift import normalize_investorlift
from ..domain.importers.zillow import normalize_zillow

router = APIRouter(prefix="/import", tags=["import"])


def _get_or_create_property(db: Session, n) -> Property:
    # simple dedupe: address+zip. Good enough for MVP.
    existing = db.scalar(select(Property).where(Property.address == n.address, Property.zip == n.zip))
    if existing:
        # update basics if missing
        changed = False
        for attr in ["city", "state", "bedrooms", "bathrooms", "square_feet", "year_built", "has_garage", "property_type"]:
            val = getattr(n, attr)
            if val is not None and getattr(existing, attr) != val:
                setattr(existing, attr, val)
                changed = True
        if changed:
            db.commit()
        return existing

    p = Property(
        address=n.address,
        city=n.city,
        state=n.state,
        zip=n.zip,
        bedrooms=n.bedrooms,
        bathrooms=n.bathrooms,
        square_feet=n.square_feet,
        year_built=n.year_built,
        has_garage=n.has_garage,
        property_type=n.property_type,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


def _upsert_rent(db: Session, property_id: int, n) -> None:
    if all(
        x is None
        for x in [
            n.market_rent_estimate,
            n.section8_fmr,
            n.approved_rent_ceiling,
            n.rent_reasonableness_comp,
            n.inventory_count,
            n.starbucks_minutes,
        ]
    ):
        return

    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    if not ra:
        ra = RentAssumption(property_id=property_id)
        db.add(ra)

    ra.market_rent_estimate = n.market_rent_estimate
    ra.section8_fmr = n.section8_fmr
    ra.approved_rent_ceiling = n.approved_rent_ceiling
    ra.rent_reasonableness_comp = n.rent_reasonableness_comp
    ra.inventory_count = n.inventory_count
    ra.starbucks_minutes = n.starbucks_minutes

    db.commit()


def _import_rows(db: Session, source: str, rows: list[dict[str, str]], notes: str | None) -> ImportResultOut:
    snap = ImportSnapshot(source=source, notes=notes)
    db.add(snap)
    db.commit()
    db.refresh(snap)

    imported = 0
    skipped = 0
    errors: list[ImportErrorRow] = []

    normalizer = normalize_investorlift if source == "investorlift" else normalize_zillow

    for idx, row in enumerate(rows, start=2):  # start=2 because header row is 1
        try:
            n = normalizer(row)
            prop = _get_or_create_property(db, n)

            fp = fingerprint(source, prop.address, prop.zip, n.asking_price)

            existing = db.scalar(select(Deal).where(Deal.source_fingerprint == fp))
            if existing:
                skipped += 1
                continue

            d = Deal(
                property_id=prop.id,
                snapshot_id=snap.id,
                source=source,
                source_fingerprint=fp,
                source_raw_json=json.dumps(n.raw),
                asking_price=n.asking_price,
                estimated_purchase_price=n.estimated_purchase_price,
                rehab_estimate=n.rehab_estimate,
                financing_type="dscr",
                interest_rate=0.07,
                term_years=30,
                down_payment_pct=0.20,
            )
            db.add(d)
            db.commit()
            db.refresh(d)

            _upsert_rent(db, prop.id, n)

            imported += 1

        except Exception as e:
            errors.append(ImportErrorRow(row=idx, error=str(e)))

    return ImportResultOut(
        snapshot_id=snap.id,
        source=source,
        imported=imported,
        skipped_duplicates=skipped,
        errors=errors,
    )


@router.post("/investorlift", response_model=ImportResultOut)
async def import_investorlift(
    file: UploadFile = File(...),
    notes: str | None = Query(None),
    db: Session = Depends(get_db),
):
    data = await file.read()
    rows = parse_csv_bytes(data)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV appears empty or unreadable")
    return _import_rows(db, "investorlift", rows, notes)


@router.post("/zillow", response_model=ImportResultOut)
async def import_zillow(
    file: UploadFile = File(...),
    notes: str | None = Query(None),
    db: Session = Depends(get_db),
):
    data = await file.read()
    rows = parse_csv_bytes(data)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV appears empty or unreadable")
    return _import_rows(db, "zillow", rows, notes)
