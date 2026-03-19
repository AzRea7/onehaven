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
from ..domain.operating_truth import (
    TruthViolation,
    enforce_deal_truth,
    enforce_property_truth,
)
from ..models import Deal, ImportSnapshot, IngestionRun, Property, RentAssumption
from ..schemas import ImportErrorRow, ImportResultOut, IngestionOverviewOut
from ..services.geo_enrichment import enrich_property_geo
from ..services.ingestion_run_service import get_ingestion_overview
from ..services.ingestion_source_service import ensure_default_manual_sources
from ..services.property_photo_service import upsert_zillow_photos
from ..services.zillow_photo_source import extract_zillow_photo_urls

router = APIRouter(prefix="/import", tags=["import"])


def _safe_attr(obj, name: str, default=None):
    return getattr(obj, name, default)


def _maybe_attach_zillow_photos(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    raw_payload: dict | str | None,
) -> None:
    urls = extract_zillow_photo_urls(raw_payload)
    if not urls:
        return

    upsert_zillow_photos(
        db,
        org_id=org_id,
        property_id=property_id,
        urls=urls,
    )


def _maybe_geo_enrich_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    force: bool = False,
) -> None:
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

        if (
            getattr(p, "lat", None) is None
            or getattr(p, "lng", None) is None
            or not getattr(p, "county", None)
        ):
            _maybe_geo_enrich_property(
                db,
                org_id=org_id,
                property_id=int(p.id),
                force=False,
            )
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

    if (
        getattr(p, "lat", None) is None
        or getattr(p, "lng", None) is None
        or not getattr(p, "county", None)
    ):
        _maybe_geo_enrich_property(
            db,
            org_id=org_id,
            property_id=int(p.id),
            force=False,
        )
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


def _backfill_inventory_counts_from_snapshot(
    db: Session,
    org_id: int,
    snapshot_id: int,
) -> None:
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
        p = db.scalar(
            select(Property).where(
                Property.id == d.property_id,
                Property.org_id == org_id,
            )
        )
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
                if source == "zillow":
                    try:
                        _maybe_attach_zillow_photos(
                            db,
                            org_id=org_id,
                            property_id=prop.id,
                            raw_payload=n.raw,
                        )
                    except Exception:
                        pass
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

            if source == "zillow":
                try:
                    _maybe_attach_zillow_photos(
                        db,
                        org_id=org_id,
                        property_id=prop.id,
                        raw_payload=n.raw,
                    )
                except Exception:
                    pass

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


def _build_run_status(run: IngestionRun) -> dict:
    summary = dict(run.summary_json or {})
    return {
        "mode": "ingestion_run",
        "run_id": int(run.id),
        "exists": True,
        "org_id": run.org_id,
        "source_id": run.source_id,
        "trigger_type": run.trigger_type,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "records_seen": int(getattr(run, "records_seen", 0) or 0),
        "records_imported": int(getattr(run, "records_imported", 0) or 0),
        "properties_created": int(getattr(run, "properties_created", 0) or 0),
        "properties_updated": int(getattr(run, "properties_updated", 0) or 0),
        "deals_created": int(getattr(run, "deals_created", 0) or 0),
        "deals_updated": int(getattr(run, "deals_updated", 0) or 0),
        "rent_rows_upserted": int(getattr(run, "rent_rows_upserted", 0) or 0),
        "photos_upserted": int(getattr(run, "photos_upserted", 0) or 0),
        "duplicates_skipped": int(getattr(run, "duplicates_skipped", 0) or 0),
        "invalid_rows": int(getattr(run, "invalid_rows", 0) or 0),
        "pipeline": {
            "attempted": int(summary.get("post_import_pipeline_attempted", 0) or 0),
            "geo_enriched": int(summary.get("geo_enriched", 0) or 0),
            "rent_refreshed": int(summary.get("rent_refreshed", 0) or 0),
            "evaluated": int(summary.get("evaluated", 0) or 0),
            "state_synced": int(summary.get("state_synced", 0) or 0),
            "workflow_synced": int(summary.get("workflow_synced", 0) or 0),
            "next_actions_seeded": int(summary.get("next_actions_seeded", 0) or 0),
            "failures": int(summary.get("post_import_failures", 0) or 0),
            "partials": int(summary.get("post_import_partials", 0) or 0),
            "errors": list(summary.get("post_import_errors") or []),
        },
        "summary_json": summary,
        "error_summary": run.error_summary,
        "error_json": run.error_json,
    }


@router.get("/overview", response_model=IngestionOverviewOut)
def imports_overview(
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    ensure_default_manual_sources(db, org_id=principal.org_id)
    return get_ingestion_overview(db, org_id=principal.org_id)


@router.post("/bootstrap", response_model=dict)
def bootstrap_import_sources(
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = ensure_default_manual_sources(db, org_id=principal.org_id)
    return {
        "ok": True,
        "count": len(rows),
        "sources": [{"id": r.id, "provider": r.provider, "slug": r.slug} for r in rows],
    }


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
    run_id: int | None = Query(default=None),
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    if run_id is not None:
        run = db.scalar(
            select(IngestionRun)
            .where(IngestionRun.id == int(run_id))
            .where(IngestionRun.org_id == principal.org_id)
        )
        if run is None:
            return {"mode": "ingestion_run", "run_id": int(run_id), "exists": False}
        return _build_run_status(run)

    if snapshot_id is not None:
        snap = db.scalar(
            select(ImportSnapshot)
            .where(ImportSnapshot.id == snapshot_id)
            .where(ImportSnapshot.org_id == principal.org_id)
        )
        if snap is None:
            return {"mode": "legacy_snapshot", "snapshot_id": snapshot_id, "exists": False}

        deal_count = db.scalar(
            select(func.count())
            .select_from(Deal)
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
            "mode": "legacy_snapshot",
            "snapshot_id": snapshot_id,
            "exists": True,
            "org_id": snap.org_id,
            "source": snap.source,
            "notes": snap.notes,
            "created_at": snap.created_at,
            "deal_count": int(deal_count),
            "distinct_property_count": int(distinct_props),
        }

    return {
        "exists": False,
        "detail": "Provide run_id for ingestion-run status or snapshot_id for legacy manual CSV import status.",
    }
