from __future__ import annotations

import asyncio
import csv
import io
import json
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.fingerprint import fingerprint
from ..domain.importers.investorlift import normalize_investorlift
from ..domain.importers.zillow import normalize_zillow
from ..domain.operating_truth import TruthViolation, enforce_deal_truth, enforce_property_truth
from ..models import Deal, ImportSnapshot, Property, RentAssumption
from ..schemas import ImportErrorRow, ImportResultOut
from ..services.geo_enrichment import enrich_property_geo

router = APIRouter(prefix="/import", tags=["import"])


def _safe_attr(obj, name: str, default=None):
    return getattr(obj, name, default)


def _maybe_geo_enrich_property(db: Session, *, org_id: int, property_id: int, force: bool = False) -> None:
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    try:
        asyncio.run(
            enrich_property_geo(
                db,
                org_id=org_id,
                property_id=property_id,
                google_api_key=key,
                force=force,
            )
        )
    except RuntimeError:
        # If something weird is already running an event loop in this context,
        # fail soft instead of exploding the import.
        pass
    except Exception:
        pass


def _get_or_create_property(db: Session, org_id: int, n) -> Property:
    try:
        enforce_property_truth(
            {
                "address": n.address,
                "city": n.city,
                "state": n.state,
                "zip": n.zip,
                "bedrooms": n.bedrooms,
                "bathrooms": n.bathrooms,
                "square_feet": n.square_feet,
                "year_built": n.year_built,
            }
        )
    except TruthViolation as tv:
        raise ValueError(tv.message)

    p = db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.address == n.address,
            Property.city == n.city,
            Property.state == n.state,
            Property.zip == n.zip,
        )
    )

    if p:
        dirty = False

        if p.bedrooms is None and n.bedrooms is not None:
            p.bedrooms = n.bedrooms
            dirty = True
        if p.bathrooms is None and n.bathrooms is not None:
            p.bathrooms = n.bathrooms
            dirty = True
        if p.square_feet is None and n.square_feet is not None:
            p.square_feet = n.square_feet
            dirty = True
        if p.year_built is None and n.year_built is not None:
            p.year_built = n.year_built
            dirty = True
        if p.has_garage is False and n.has_garage is True:
            p.has_garage = True
            dirty = True

        # geo/risk fields from normalizer if present
        if getattr(p, "lat", None) is None and _safe_attr(n, "lat") is not None:
            p.lat = float(_safe_attr(n, "lat"))
            dirty = True
        if getattr(p, "lng", None) is None and _safe_attr(n, "lng") is not None:
            p.lng = float(_safe_attr(n, "lng"))
            dirty = True
        if not getattr(p, "county", None) and _safe_attr(n, "county"):
            p.county = str(_safe_attr(n, "county")).strip()
            dirty = True

        if _safe_attr(n, "is_red_zone") is not None:
            p.is_red_zone = bool(_safe_attr(n, "is_red_zone"))
            dirty = True
        if _safe_attr(n, "crime_density") is not None:
            p.crime_density = _safe_attr(n, "crime_density")
            dirty = True
        if _safe_attr(n, "crime_score") is not None:
            p.crime_score = _safe_attr(n, "crime_score")
            dirty = True
        if _safe_attr(n, "offender_count") is not None:
            p.offender_count = _safe_attr(n, "offender_count")
            dirty = True

        if dirty:
            db.add(p)
            db.commit()
            db.refresh(p)

        if getattr(p, "lat", None) is None or getattr(p, "lng", None) is None or not getattr(p, "county", None):
            _maybe_geo_enrich_property(db, org_id=org_id, property_id=int(p.id), force=False)
            db.refresh(p)

        return p

    p = Property(
        org_id=org_id,
        address=n.address,
        city=n.city,
        state=n.state,
        zip=n.zip,
        bedrooms=n.bedrooms,
        bathrooms=n.bathrooms or 1.0,
        square_feet=n.square_feet,
        year_built=n.year_built,
        has_garage=bool(n.has_garage),

        # geo/risk
        lat=_safe_attr(n, "lat"),
        lng=_safe_attr(n, "lng"),
        county=_safe_attr(n, "county"),
        is_red_zone=bool(_safe_attr(n, "is_red_zone", False)),
        crime_density=_safe_attr(n, "crime_density"),
        crime_score=_safe_attr(n, "crime_score"),
        offender_count=_safe_attr(n, "offender_count"),
    )
    db.add(p)
    db.commit()
    db.refresh(p)

    if getattr(p, "lat", None) is None or getattr(p, "lng", None) is None or not getattr(p, "county", None):
        _maybe_geo_enrich_property(db, org_id=org_id, property_id=int(p.id), force=False)
        db.refresh(p)

    return p


def _upsert_rent(db: Session, org_id: int, property_id: int, n) -> None:
    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.property_id == property_id)
        .where(RentAssumption.org_id == org_id)
    )
    if not ra:
        ra = RentAssumption(property_id=property_id, org_id=org_id)
        db.add(ra)

    if getattr(ra, "org_id", None) != org_id:
        ra.org_id = org_id

    if n.market_rent_estimate is not None:
        ra.market_rent_estimate = n.market_rent_estimate
    if n.section8_fmr is not None:
        ra.section8_fmr = n.section8_fmr
    if n.approved_rent_ceiling is not None:
        ra.approved_rent_ceiling = n.approved_rent_ceiling
    if n.rent_reasonableness_comp is not None:
        ra.rent_reasonableness_comp = n.rent_reasonableness_comp
    if n.inventory_count is not None:
        ra.inventory_count = n.inventory_count
    if n.starbucks_minutes is not None:
        ra.starbucks_minutes = n.starbucks_minutes

    db.commit()


def _backfill_inventory_counts_from_snapshot(db: Session, org_id: int, snapshot_id: int) -> None:
    rows = db.execute(
        select(Property.city, Property.state, func.count(Deal.id))
        .join(Deal, Deal.property_id == Property.id)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Deal.org_id == org_id)
        .where(Property.org_id == org_id)
        .group_by(Property.city, Property.state)
    ).all()

    inv_map = {(city, state): int(cnt) for (city, state, cnt) in rows}

    deals = db.scalars(
        select(Deal)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Deal.org_id == org_id)
    ).all()

    for d in deals:
        p = db.scalar(select(Property).where(Property.id == d.property_id, Property.org_id == org_id))
        if not p:
            continue

        inv = inv_map.get((p.city, p.state))
        if inv is None:
            continue

        ra = db.scalar(
            select(RentAssumption)
            .where(RentAssumption.property_id == p.id)
            .where(RentAssumption.org_id == org_id)
        )
        if not ra:
            ra = RentAssumption(property_id=p.id, org_id=org_id)
            db.add(ra)

        if ra.inventory_count is None:
            ra.inventory_count = inv

    db.commit()


def _read_csv_rows(file: UploadFile) -> list[dict[str, str]]:
    content = file.file.read()
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def _import_rows(
    db: Session,
    source: str,
    rows: list[dict[str, str]],
    notes: str | None,
    org_id: int,
) -> ImportResultOut:
    snap = ImportSnapshot(
        org_id=org_id,
        source=source,
        notes=notes,
        created_at=datetime.utcnow(),
    )
    db.add(snap)
    db.commit()
    db.refresh(snap)

    imported = 0
    skipped = 0
    errors: list[ImportErrorRow] = []

    normalizer = normalize_investorlift if source == "investorlift" else normalize_zillow

    for idx, row in enumerate(rows, start=2):
        try:
            n = normalizer(row)
            prop = _get_or_create_property(db, org_id, n)

            fp = fingerprint(source, prop.address, prop.zip, n.asking_price)

            existing = db.scalar(
                select(Deal)
                .where(Deal.org_id == org_id)
                .where(Deal.source_fingerprint == fp)
            )
            if existing:
                skipped += 1
                continue

            try:
                enforce_deal_truth(
                    {
                        "asking_price": n.asking_price,
                        "rehab_estimate": n.rehab_estimate,
                        "strategy": getattr(n, "strategy", None) or "section8",
                    }
                )
            except TruthViolation as tv:
                raise ValueError(tv.message)

            d = Deal(
                org_id=org_id,
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

            _upsert_rent(db, org_id, prop.id, n)
            imported += 1

        except Exception as e:
            db.rollback()
            errors.append(ImportErrorRow(row=idx, error=str(e)))

    _backfill_inventory_counts_from_snapshot(db, org_id, snap.id)

    return ImportResultOut(
        snapshot_id=snap.id,
        source=source,
        imported=imported,
        skipped_duplicates=skipped,
        errors=errors,
    )


@router.post("/zillow", response_model=ImportResultOut)
def import_zillow(
    notes: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = _read_csv_rows(file)
    return _import_rows(db, "zillow", rows, notes, org_id=principal.org_id)


@router.post("/investorlift", response_model=ImportResultOut)
def import_investorlift(
    notes: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = _read_csv_rows(file)
    return _import_rows(db, "investorlift", rows, notes, org_id=principal.org_id)


@router.get("/status")
def import_status(
    snapshot_id: int = Query(...),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    snap = db.scalar(
        select(ImportSnapshot)
        .where(ImportSnapshot.id == snapshot_id)
        .where(ImportSnapshot.org_id == principal.org_id)
    )
    if snap is None:
        return {"snapshot_id": snapshot_id, "exists": False}

    deal_count = db.scalar(
        select(func.count()).select_from(Deal)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Deal.org_id == principal.org_id)
    ) or 0

    distinct_props = db.scalar(
        select(func.count(func.distinct(Deal.property_id)))
        .select_from(Deal)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Deal.org_id == principal.org_id)
    ) or 0

    return {
        "snapshot_id": snapshot_id,
        "exists": True,
        "org_id": snap.org_id,
        "source": snap.source,
        "notes": snap.notes,
        "created_at": snap.created_at,
        "deal_count": int(deal_count),
        "distinct_property_count": int(distinct_props),
    }
