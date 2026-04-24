from __future__ import annotations

from sqlalchemy import select

from .celery_app import celery_app
from onehaven_platform.backend.src.db import SessionLocal
from onehaven_platform.backend.src.models import Property
from onehaven_platform.backend.src.services.geo_enrichment import enrich_property_geo


@celery_app.task(name="risk.recompute_property", autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def recompute_property_risk(org_id: int, property_id: int, force: bool = True) -> dict:
    db = SessionLocal()
    try:
        return enrich_property_geo(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            force=bool(force),
            commit=True,
        )
    finally:
        db.close()


@celery_app.task(name="risk.recompute_org", autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def recompute_org_risk(org_id: int, state: str = "MI", limit: int = 500) -> dict:
    db = SessionLocal()
    try:
        rows = db.scalars(
            select(Property)
            .where(Property.org_id == int(org_id), Property.state == state.upper())
            .order_by(Property.id.asc())
            .limit(int(limit))
        ).all()

        attempted = 0
        updated = 0
        errors: list[dict] = []

        for prop in rows:
            attempted += 1
            try:
                res = enrich_property_geo(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                    force=True,
                    commit=True,
                )
                if res.get("updated"):
                    updated += 1
            except Exception as e:
                errors.append({"property_id": int(prop.id), "error": str(e)})

        return {
            "org_id": int(org_id),
            "state": state.upper(),
            "attempted": attempted,
            "updated": updated,
            "errors": errors,
        }
    finally:
        db.close()