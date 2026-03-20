from __future__ import annotations

import asyncio
import inspect
import os
from typing import Any, Optional

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Property, RentAssumption
from .rentcast_service import RentCastClient, persist_rentcast_comps_and_get_median


def derive_photo_kind(url: str) -> str:
    u = (url or "").lower()
    if any(x in u for x in ["front", "exterior", "outside", "street"]):
        return "exterior"
    if any(x in u for x in ["kitchen", "bath", "bed", "living", "interior", "inside"]):
        return "interior"
    return "unknown"


def normalize_photos(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not raw:
        return out

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                url = item.strip()
                if not url:
                    continue
                out.append({"url": url, "kind": derive_photo_kind(url)})
                continue

            if isinstance(item, dict):
                url = str(
                    item.get("url")
                    or item.get("href")
                    or item.get("photoUrl")
                    or item.get("src")
                    or ""
                ).strip()
                if not url:
                    continue

                kind = str(item.get("kind") or "").strip() or derive_photo_kind(url)
                out.append({"url": url, "kind": kind})
    return out


def canonical_listing_payload(row: dict[str, Any]) -> dict[str, Any]:
    row = row or {}
    price = row.get("asking_price") or row.get("price") or row.get("listPrice") or 0
    market_rent = (
        row.get("market_rent_estimate")
        or row.get("rent_estimate")
        or row.get("rentEstimate")
        or row.get("predictedRent")
    )

    bedrooms_raw = row.get("bedrooms")
    bathrooms_raw = row.get("bathrooms")

    try:
        bedrooms = int(bedrooms_raw or 0)
    except Exception:
        bedrooms = 0

    try:
        bathrooms = float(bathrooms_raw or 1)
    except Exception:
        bathrooms = 1.0

    return {
        "external_record_id": str(
            row.get("external_record_id")
            or row.get("listing_id")
            or row.get("listingId")
            or row.get("id")
            or row.get("zpid")
            or ""
        ).strip(),
        "external_url": row.get("external_url") or row.get("listingUrl") or row.get("url"),
        "address": str(row.get("address") or row.get("formattedAddress") or "").strip(),
        "city": str(row.get("city") or "").strip(),
        "county": str(row.get("county") or "").strip() or None,
        "state": str(row.get("state") or "MI").strip() or "MI",
        "zip": str(row.get("zip") or row.get("zipCode") or row.get("postalCode") or "").strip(),
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "square_feet": row.get("square_feet") or row.get("squareFootage") or row.get("livingArea"),
        "year_built": row.get("year_built") or row.get("yearBuilt"),
        "property_type": row.get("property_type") or row.get("propertyType") or "single_family",
        "asking_price": float(price or 0),
        "estimated_purchase_price": row.get("estimated_purchase_price") or price,
        "rehab_estimate": float(row.get("rehab_estimate") or 0),
        "market_rent_estimate": market_rent,
        "section8_fmr": row.get("section8_fmr"),
        "approved_rent_ceiling": row.get("approved_rent_ceiling"),
        "inventory_count": row.get("inventory_count"),
        "photos": normalize_photos(row.get("photos")),
        "raw": row.get("raw") or row,
    }


def build_post_import_actions() -> list[dict[str, str]]:
    return [
        {"key": "geo", "label": "Geocode and enrich location"},
        {"key": "risk", "label": "Update crime and zone risk"},
        {"key": "rent", "label": "Refresh rent assumptions"},
        {"key": "evaluate", "label": "Run underwriting"},
        {"key": "workflow", "label": "Refresh workflow gates"},
        {"key": "next_actions", "label": "Seed next actions"},
    ]


def get_rentcast_api_key() -> Optional[str]:
    key = (
        settings.rentcast_api_key
        or os.getenv("RENTCAST_INGESTION_API_KEY")
        or os.getenv("RENTCAST_API_KEY")
        or ""
    ).strip()
    return key or None


def get_google_maps_api_key() -> Optional[str]:
    key = (
        settings.google_geocode_api_key
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or os.getenv("GOOGLE_GEOCODING_API_KEY")
        or ""
    ).strip()
    return key or None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _compute_approved_ceiling(
    *,
    section8_fmr: Any,
    rent_reasonableness_comp: Any,
    approved_rent_ceiling: Any,
) -> Optional[float]:
    approved_existing = _safe_float(approved_rent_ceiling)
    if approved_existing is not None and approved_existing > 0:
        return approved_existing

    fmr_existing = _safe_float(section8_fmr)
    rr_existing = _safe_float(rent_reasonableness_comp)

    approved_candidates: list[float] = []
    if rr_existing is not None and rr_existing > 0:
        approved_candidates.append(rr_existing)
    if fmr_existing is not None and fmr_existing > 0:
        approved_candidates.append(fmr_existing)

    return min(approved_candidates) if approved_candidates else None


def refresh_property_rent_assumptions(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    rentcast_api_key: Optional[str] = None,
    replace_existing_comps: bool = True,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.id == int(property_id),
        )
    )
    if prop is None:
        return {"ok": False, "error": "property_not_found"}

    address = str(getattr(prop, "address", "") or "").strip()
    city = str(getattr(prop, "city", "") or "").strip()
    state = str(getattr(prop, "state", "") or "").strip() or "MI"
    zip_code = str(getattr(prop, "zip", "") or "").strip()
    bedrooms = int(getattr(prop, "bedrooms", 0) or 0)
    bathrooms = float(getattr(prop, "bathrooms", 0) or 0)
    square_feet = getattr(prop, "square_feet", None)

    if not address or not city or not state or not zip_code:
        return {
            "ok": False,
            "error": "missing_address_fields",
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
        }

    api_key = (rentcast_api_key or get_rentcast_api_key() or "").strip()
    if not api_key:
        return {"ok": False, "error": "missing_rentcast_api_key"}

    client = RentCastClient(api_key)
    payload = client.rent_estimate(
        address=address,
        city=city,
        state=state,
        zip_code=zip_code,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        square_feet=int(square_feet) if square_feet is not None else None,
    )

    market_rent_estimate = client.pick_estimated_rent(payload)
    rent_reasonableness_comp = persist_rentcast_comps_and_get_median(
        db,
        property_id=int(property_id),
        payload=payload,
        replace_existing=replace_existing_comps,
    )

    existing = db.scalar(
        select(RentAssumption).where(
            RentAssumption.org_id == int(org_id),
            RentAssumption.property_id == int(property_id),
        ).order_by(desc(RentAssumption.id))
    )

    created = False
    if existing is None:
        existing = RentAssumption(
            org_id=int(org_id),
            property_id=int(property_id),
        )
        created = True

    updated_fields: list[str] = []

    if market_rent_estimate is not None:
        existing.market_rent_estimate = float(market_rent_estimate)
        updated_fields.append("market_rent_estimate")

    if hasattr(existing, "rent_reasonableness_comp") and rent_reasonableness_comp is not None:
        existing.rent_reasonableness_comp = float(rent_reasonableness_comp)
        updated_fields.append("rent_reasonableness_comp")

    computed_ceiling = _compute_approved_ceiling(
        section8_fmr=getattr(existing, "section8_fmr", None),
        rent_reasonableness_comp=getattr(existing, "rent_reasonableness_comp", None),
        approved_rent_ceiling=getattr(existing, "approved_rent_ceiling", None),
    )
    if _safe_float(getattr(existing, "approved_rent_ceiling", None)) is None and computed_ceiling is not None:
        existing.approved_rent_ceiling = float(computed_ceiling)
        updated_fields.append("approved_rent_ceiling")

    db.add(existing)
    db.commit()
    db.refresh(existing)

    return {
        "ok": True,
        "created": created,
        "property_id": int(property_id),
        "updated_fields": updated_fields,
        "market_rent_estimate": _safe_float(getattr(existing, "market_rent_estimate", None)),
        "rent_reasonableness_comp": _safe_float(getattr(existing, "rent_reasonableness_comp", None))
        if hasattr(existing, "rent_reasonableness_comp")
        else rent_reasonableness_comp,
        "approved_rent_ceiling": _safe_float(getattr(existing, "approved_rent_ceiling", None)),
        "section8_fmr": _safe_float(getattr(existing, "section8_fmr", None)),
    }


def _run_maybe_async(value: Any) -> Any:
    if inspect.isawaitable(value):
        try:
            return asyncio.run(value)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(value)
            finally:
                loop.close()
    return value


def _seed_next_actions_if_available(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    candidates: list[tuple[str, str]] = [
        ("app.services.next_actions_service", "seed_property_next_actions"),
        ("app.services.next_actions_service", "generate_property_next_actions"),
        ("app.services.next_actions_service", "recompute_property_next_actions"),
    ]

    for module_name, fn_name in candidates:
        try:
            module = __import__(module_name, fromlist=[fn_name])
            fn = getattr(module, fn_name, None)
            if callable(fn):
                out = fn(db, org_id=int(org_id), property_id=int(property_id))
                return {"ok": True, "handler": fn_name, "result": out}
        except Exception:
            continue

    return {"ok": False, "skipped": True, "reason": "next_actions_service_unavailable"}


def _try_risk_refresh(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    candidates: list[tuple[str, str]] = [
        ("app.services.risk_scoring", "refresh_property_risk"),
        ("app.services.risk_scoring", "score_property_risk"),
        ("app.services.risk_scoring", "recompute_property_risk"),
        ("app.services.geo_enrichment", "enrich_property_risk"),
    ]

    for module_name, fn_name in candidates:
        try:
            module = __import__(module_name, fromlist=[fn_name])
            fn = getattr(module, fn_name, None)
            if callable(fn):
                out = fn(db, org_id=int(org_id), property_id=int(property_id))
                out = out if isinstance(out, dict) else {"ok": bool(out)}
                out["handler"] = fn_name
                return out
        except Exception:
            continue

    return {"ok": False, "skipped": True, "reason": "risk_service_unavailable"}


def execute_post_ingestion_pipeline(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    actor_user_id: int | None = None,
    emit_events: bool = False,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "geo_ok": False,
        "risk_ok": False,
        "rent_ok": False,
        "evaluate_ok": False,
        "state_ok": False,
        "workflow_ok": False,
        "next_actions_ok": False,
        "partial": False,
        "errors": [],
    }

    try:
        from ..services.geo_enrichment import enrich_property_geo

        geo_res = _run_maybe_async(
            enrich_property_geo(
                db,
                org_id=int(org_id),
                property_id=int(property_id),
                google_api_key=get_google_maps_api_key(),
                force=False,
            )
        )
        geo_res = geo_res if isinstance(geo_res, dict) else {"ok": bool(geo_res)}
        result["geo_ok"] = bool(geo_res.get("ok"))
        result["geo"] = geo_res
    except Exception as e:
        result["errors"].append(f"geo:{type(e).__name__}:{e}")

    try:
        risk_res = _try_risk_refresh(db, org_id=int(org_id), property_id=int(property_id))
        result["risk_ok"] = bool(risk_res.get("ok"))
        result["risk"] = risk_res
    except Exception as e:
        result["errors"].append(f"risk:{type(e).__name__}:{e}")

    try:
        rent_res = refresh_property_rent_assumptions(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        result["rent_ok"] = bool(rent_res.get("ok"))
        result["rent"] = rent_res
    except Exception as e:
        result["errors"].append(f"rent:{type(e).__name__}:{e}")

    try:
        from ..routers.evaluate import evaluate_property_core
        from ..routers.rent import explain_rent

        principal_shim = type("PrincipalShim", (), {"org_id": int(org_id), "user_id": actor_user_id})()

        explain_res = explain_rent(
            property_id=int(property_id),
            strategy="section8",
            payment_standard_pct=None,
            persist=True,
            db=db,
            p=principal_shim,
        )
        result["rent_explain"] = {
            "run_id": getattr(explain_res, "run_id", None),
            "rent_used": getattr(explain_res, "rent_used", None),
            "approved_rent_ceiling": getattr(explain_res, "approved_rent_ceiling", None),
            "cap_reason": getattr(explain_res, "cap_reason", None),
        }

        eval_res = evaluate_property_core(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            emit_events=bool(emit_events),
            actor_user_id=actor_user_id,
            commit=True,
        )
        result["evaluate_ok"] = bool(eval_res.get("ok"))
        result["evaluate"] = eval_res
    except Exception as e:
        result["errors"].append(f"evaluate:{type(e).__name__}:{e}")

    try:
        from ..services.property_state_machine import sync_property_state

        sync_property_state(db, org_id=int(org_id), property_id=int(property_id))
        result["state_ok"] = True
    except Exception as e:
        result["errors"].append(f"state:{type(e).__name__}:{e}")

    try:
        from ..services.workflow_gate_service import build_workflow_summary

        build_workflow_summary(db, org_id=int(org_id), property_id=int(property_id), recompute=True)
        result["workflow_ok"] = True
    except Exception as e:
        result["errors"].append(f"workflow:{type(e).__name__}:{e}")

    try:
        next_actions_res = _seed_next_actions_if_available(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        result["next_actions_ok"] = bool(next_actions_res.get("ok"))
        result["next_actions"] = next_actions_res
    except Exception as e:
        result["errors"].append(f"next_actions:{type(e).__name__}:{e}")

    oks = [
        bool(result.get("geo_ok")),
        bool(result.get("risk_ok")) or bool(result.get("risk", {}).get("skipped")),
        bool(result.get("rent_ok")),
        bool(result.get("evaluate_ok")),
        bool(result.get("state_ok")),
        bool(result.get("workflow_ok")),
    ]
    result["partial"] = any(oks) and not all(oks)
    return result


def apply_pipeline_summary(summary: dict[str, Any], pipeline_res: dict[str, Any], property_id: int) -> None:
    summary["post_import_pipeline_attempted"] = int(summary.get("post_import_pipeline_attempted", 0) or 0) + 1

    if pipeline_res.get("geo_ok"):
        summary["geo_enriched"] = int(summary.get("geo_enriched", 0) or 0) + 1
    if pipeline_res.get("risk_ok"):
        summary["risk_scored"] = int(summary.get("risk_scored", 0) or 0) + 1
    if pipeline_res.get("rent_ok"):
        summary["rent_refreshed"] = int(summary.get("rent_refreshed", 0) or 0) + 1
    if pipeline_res.get("evaluate_ok"):
        summary["evaluated"] = int(summary.get("evaluated", 0) or 0) + 1
    if pipeline_res.get("state_ok"):
        summary["state_synced"] = int(summary.get("state_synced", 0) or 0) + 1
    if pipeline_res.get("workflow_ok"):
        summary["workflow_synced"] = int(summary.get("workflow_synced", 0) or 0) + 1
    if pipeline_res.get("next_actions_ok"):
        summary["next_actions_seeded"] = int(summary.get("next_actions_seeded", 0) or 0) + 1
    if pipeline_res.get("partial"):
        summary["post_import_partials"] = int(summary.get("post_import_partials", 0) or 0) + 1

    errors = list(pipeline_res.get("errors") or [])
    if errors:
        summary["post_import_failures"] = int(summary.get("post_import_failures", 0) or 0) + 1
        post_import_errors = list(summary.get("post_import_errors") or [])
        post_import_errors.append(
            {
                "property_id": int(property_id),
                "errors": errors,
            }
        )
        summary["post_import_errors"] = post_import_errors