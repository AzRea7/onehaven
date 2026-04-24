from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import os
import time
from typing import Any, Optional

from sqlalchemy import desc, select, text
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Property, RentAssumption
from .rent_refresh_queue_service import publish_without_rent, should_run_inline_rent_refresh
from .rentcast_service import RentCastClient, persist_rentcast_comps_and_get_median
from .address_normalization import normalize_full_address

log = logging.getLogger("onehaven.ingestion.enrichment")


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
                if url:
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

    try:
        bedrooms = int(row.get("bedrooms") or 0)
    except Exception:
        bedrooms = 0
    try:
        bathrooms = float(row.get("bathrooms") or 1)
    except Exception:
        bathrooms = 1.0

    raw_address = (
        row.get("addressLine1")
        or row.get("address")
        or row.get("formattedAddress")
        or ""
    )
    raw_city = str(row.get("city") or "").strip()
    raw_state = str(row.get("state") or "MI").strip() or "MI"
    raw_zip = str(row.get("zip") or row.get("zipCode") or row.get("postalCode") or "").strip()

    normalized = normalize_full_address(
        str(raw_address or "").strip(),
        raw_city,
        raw_state,
        raw_zip,
    )

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
        "address": normalized.address_line1 or str(raw_address or "").strip(),
        "city": normalized.city or raw_city,
        "county": str(row.get("county") or "").strip() or None,
        "state": normalized.state or raw_state,
        "zip": normalized.postal_code or raw_zip,
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
    rent_label = (
        "Refresh rent assumptions inline"
        if should_run_inline_rent_refresh()
        else "Queue budgeted rent refresh"
    )
    return [
        {"key": "geo", "label": "Geocode and enrich location"},
        {"key": "risk", "label": "Update crime and zone risk"},
        {"key": "rent", "label": rent_label},
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


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _nonblank(value: Any) -> str | None:
    s = str(value or "").strip()
    return s or None


def _compute_approved_ceiling(*, section8_fmr: Any, rent_reasonableness_comp: Any, approved_rent_ceiling: Any) -> Optional[float]:
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


def _timed_step(result: dict[str, Any], *, step_key: str, fn):
    t0 = time.perf_counter()
    try:
        out = fn()
        result.setdefault("timings_ms", {})[step_key] = round((time.perf_counter() - t0) * 1000, 2)
        return out
    except Exception as exc:
        result.setdefault("timings_ms", {})[step_key] = round((time.perf_counter() - t0) * 1000, 2)
        raise exc


def _safe_rollback(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        pass




def _get_retry_limit_from_env(retry_type: str) -> int:
    env_key = f"GEO_{str(retry_type).upper()}_RETRY_LIMIT"
    raw_value = os.getenv(env_key, "").strip()
    if raw_value:
        try:
            return max(0, int(raw_value))
        except Exception:
            return 1
    return 1



def _property_metadata_dict(prop: Property) -> dict[str, Any]:
    for attr in ("acquisition_metadata_json", "raw_json"):
        raw = getattr(prop, attr, None)
        if isinstance(raw, dict):
            return dict(raw)
    return {}


def _get_property_retry_budget(db: Session, *, property_id: int, retry_type: str) -> int:
    prop = db.scalar(select(Property).where(Property.id == int(property_id)))
    if prop is None:
        return _get_retry_limit_from_env(retry_type)

    raw = _property_metadata_dict(prop)
    if not isinstance(raw, dict):
        return _get_retry_limit_from_env(retry_type)

    meta = raw.get("_geo_retry_meta")
    if not isinstance(meta, dict):
        return _get_retry_limit_from_env(retry_type)

    used = meta.get("used")
    if not isinstance(used, dict):
        return _get_retry_limit_from_env(retry_type)

    try:
        used_count = int(used.get(retry_type) or 0)
    except Exception:
        used_count = 0

    return max(0, _get_retry_limit_from_env(retry_type) - used_count)

def _status_from_bool(ok: bool) -> str:
    return "complete" if ok else "missing"


def _derive_geo_status(pipeline_result: dict[str, Any]) -> str:
    geo = dict(pipeline_result.get("geo") or {})
    if pipeline_result.get("geo_ok"):
        return "complete"
    if geo.get("lat") is not None or geo.get("lng") is not None or geo.get("normalized_address") or geo.get("county"):
        return "partial"
    return "missing"


def _derive_risk_status(pipeline_result: dict[str, Any]) -> str:
    risk = dict(pipeline_result.get("risk") or {})
    if pipeline_result.get("risk_ok"):
        return "complete"
    if risk.get("crime_score") is not None or risk.get("offender_count") is not None or risk.get("risk_score") is not None:
        return "partial"
    return "missing"


def _derive_rent_status(pipeline_result: dict[str, Any]) -> str:
    rent = dict(pipeline_result.get("rent") or {})
    if pipeline_result.get("rent_ok"):
        return "complete"
    if pipeline_result.get("rent_deferred"):
        return "deferred"
    if (
        rent.get("market_rent_estimate") is not None
        or rent.get("approved_rent_ceiling") is not None
        or rent.get("section8_fmr") is not None
    ):
        return "partial"
    return "missing"


def _derive_jurisdiction_status(pipeline_result: dict[str, Any]) -> str:
    workflow = (((pipeline_result.get("workflow") or {}).get("summary") or {}).get("summary") or {})
    jurisdiction = dict(workflow.get("jurisdiction") or {})
    if jurisdiction.get("exists"):
        return "complete"
    if jurisdiction:
        return "partial"
    return "missing"


def _derive_rehab_status(pipeline_result: dict[str, Any]) -> str:
    workflow = (((pipeline_result.get("workflow") or {}).get("summary") or {}).get("summary") or {})
    rehab = dict(workflow.get("rehab") or {})
    if rehab.get("has_plan"):
        return "complete"
    if rehab:
        return "partial"
    return "missing"


def _update_property_acquisition_completeness(db: Session, *, org_id: int, property_id: int, pipeline_result: dict[str, Any]) -> dict[str, str]:
    statuses = {
        "geo": _derive_geo_status(pipeline_result),
        "rent": _derive_rent_status(pipeline_result),
        "rehab": _derive_rehab_status(pipeline_result),
        "risk": _derive_risk_status(pipeline_result),
        "jurisdiction": _derive_jurisdiction_status(pipeline_result),
        "cashflow": _status_from_bool(bool(pipeline_result.get("evaluate_ok"))),
    }
    db.execute(
        text(
            """
            UPDATE properties
            SET completeness_geo_status = :geo,
                completeness_rent_status = :rent,
                completeness_rehab_status = :rehab,
                completeness_risk_status = :risk,
                completeness_jurisdiction_status = :jurisdiction,
                completeness_cashflow_status = :cashflow
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id), **statuses},
    )
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise
    return statuses


def refresh_property_rent_assumptions(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    rentcast_api_key: Optional[str] = None,
    replace_existing_comps: bool = True,
) -> dict[str, Any]:
    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
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

    t0 = time.perf_counter()
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
    provider_ms = round((time.perf_counter() - t0) * 1000, 2)

    t1 = time.perf_counter()
    market_rent_estimate = client.pick_estimated_rent(payload)
    rent_reasonableness_comp = persist_rentcast_comps_and_get_median(
        db,
        property_id=int(property_id),
        payload=payload,
        replace_existing=replace_existing_comps,
    )
    persist_ms = round((time.perf_counter() - t1) * 1000, 2)

    existing = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.org_id == int(org_id), RentAssumption.property_id == int(property_id))
        .order_by(desc(RentAssumption.id))
    )

    created = False
    if existing is None:
        existing = RentAssumption(org_id=int(org_id), property_id=int(property_id))
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

    t2 = time.perf_counter()
    db.add(existing)
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise
    db.refresh(existing)
    commit_ms = round((time.perf_counter() - t2) * 1000, 2)

    ok = bool(
        _safe_float(getattr(existing, "market_rent_estimate", None)) is not None
        or _safe_float(getattr(existing, "approved_rent_ceiling", None)) is not None
        or _safe_float(getattr(existing, "section8_fmr", None)) is not None
    )

    return {
        "ok": ok,
        "created": created,
        "property_id": int(property_id),
        "updated_fields": updated_fields,
        "market_rent_estimate": _safe_float(getattr(existing, "market_rent_estimate", None)),
        "rent_reasonableness_comp": _safe_float(getattr(existing, "rent_reasonableness_comp", None)) if hasattr(existing, "rent_reasonableness_comp") else rent_reasonableness_comp,
        "approved_rent_ceiling": _safe_float(getattr(existing, "approved_rent_ceiling", None)),
        "section8_fmr": _safe_float(getattr(existing, "section8_fmr", None)),
        "timings_ms": {"provider_call": provider_ms, "persist_comps": persist_ms, "commit": commit_ms},
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


def _module_candidates(*names: str) -> list[str]:
    out: list[str] = []
    for name in names:
        if name and name not in out:
            out.append(name)
    return out


def _seed_next_actions_if_available(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    module_candidates = _module_candidates(
        "app.services.next_actions_service",
    )
    handler_candidates = [
        "seed_property_next_actions",
        "generate_property_next_actions",
        "recompute_property_next_actions",
    ]

    last_missing: str | None = None
    for module_name in module_candidates:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                last_missing = module_name
                continue
            log.exception("next_actions_seed_handler_failed", extra={"org_id": int(org_id), "property_id": int(property_id), "module_name": module_name})
            return {"ok": False, "skipped": True, "reason": "next_actions_dependency_import_failed", "module_name": module_name, "error": str(exc)}
        except Exception as exc:
            log.exception("next_actions_seed_handler_failed", extra={"org_id": int(org_id), "property_id": int(property_id), "module_name": module_name})
            return {"ok": False, "skipped": True, "reason": "next_actions_module_import_failed", "module_name": module_name, "error": str(exc)}

        for fn_name in handler_candidates:
            fn = getattr(module, fn_name, None)
            if not callable(fn):
                continue
            try:
                out = _run_maybe_async(fn(db, org_id=int(org_id), property_id=int(property_id)))
                return {"ok": True, "module_name": module_name, "handler": fn_name, "result": out}
            except Exception as exc:
                log.exception("next_actions_seed_handler_failed", extra={"org_id": int(org_id), "property_id": int(property_id), "handler": fn_name, "module_name": module_name})
                return {"ok": False, "skipped": True, "reason": "next_actions_handler_failed", "module_name": module_name, "handler": fn_name, "error": str(exc)}

    if last_missing:
        log.info(
            "next_actions_seed_skipped_missing_module org_id=%s property_id=%s module=%s",
            int(org_id),
            int(property_id),
            last_missing,
        )
    return {"ok": False, "skipped": True, "reason": "next_actions_service_unavailable", "module_name": last_missing or module_candidates[0], "candidates": handler_candidates}


def _try_risk_refresh(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    candidates: list[tuple[str, str]] = [
        ("app.services.risk_scoring", "refresh_property_risk"),
        ("app.services.risk_scoring", "score_property_risk"),
        ("app.services.risk_scoring", "recompute_property_risk"),
        ("app.services.geo_enrichment", "enrich_property_risk"),
    ]
    for module_name, fn_name in candidates:
        try:
            module = importlib.import_module(module_name)
            fn = getattr(module, fn_name, None)
            if callable(fn):
                out = fn(db, org_id=int(org_id), property_id=int(property_id))
                out = out if isinstance(out, dict) else {"ok": bool(out)}
                out["handler"] = fn_name
                out["module_name"] = module_name
                return out
        except Exception:
            _safe_rollback(db)
            log.exception("risk_refresh_handler_failed", extra={"org_id": int(org_id), "property_id": int(property_id), "handler": fn_name, "module_name": module_name})
            continue
    return {"ok": False, "skipped": True, "reason": "risk_service_unavailable"}


def _is_geo_payload_complete(payload: dict[str, Any] | None) -> bool:
    data = dict(payload or {})
    return bool(
        data.get("lat") is not None
        and data.get("lng") is not None
        and _nonblank(data.get("normalized_address"))
        and _nonblank(data.get("geocode_source"))
    )


def _is_risk_payload_complete(payload: dict[str, Any] | None) -> bool:
    data = dict(payload or {})
    return bool(data.get("crime_score") is not None and data.get("offender_count") is not None)


def _is_rent_payload_complete(payload: dict[str, Any] | None) -> bool:
    data = dict(payload or {})
    return bool(
        data.get("market_rent_estimate") is not None
        or data.get("approved_rent_ceiling") is not None
        or data.get("section8_fmr") is not None
    )


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
        "rent_deferred": False,
        "evaluate_ok": False,
        "state_ok": False,
        "workflow_ok": False,
        "next_actions_ok": False,
        "partial": False,
        "errors": [],
        "timings_ms": {},
        "step_status": {},
    }

    def _record_step(step: str, ok: bool, payload: Any = None, *, skipped: bool = False) -> None:
        result["step_status"][step] = {"ok": bool(ok), "skipped": bool(skipped)}
        if payload is not None:
            result[step] = payload

    try:
        from app.services.geo_enrichment import enrich_property_geo

        geo_res = _timed_step(
            result,
            step_key="geo",
            fn=lambda: _run_maybe_async(
                enrich_property_geo(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_id),
                    google_api_key=get_google_maps_api_key(),
                    force=False,
                )
            ),
        )
        geo_res = geo_res if isinstance(geo_res, dict) else {"ok": bool(geo_res)}
        result["geo_ok"] = _is_geo_payload_complete(geo_res)
        geo_force_retry_remaining = _get_property_retry_budget(
            db,
            property_id=int(property_id),
            retry_type="force_retry",
        )
        if not result["geo_ok"] and geo_res.get("geocode_attempted") and geo_force_retry_remaining > 0:
            forced_geo_res = _timed_step(
                result,
                step_key="geo_force_retry",
                fn=lambda: _run_maybe_async(
                    enrich_property_geo(
                        db,
                        org_id=int(org_id),
                        property_id=int(property_id),
                        google_api_key=get_google_maps_api_key(),
                        force=True,
                    )
                ),
            )
            forced_geo_res = forced_geo_res if isinstance(forced_geo_res, dict) else {"ok": bool(forced_geo_res)}
            if _is_geo_payload_complete(forced_geo_res):
                geo_res = forced_geo_res
                geo_res["forced_retry"] = True
                result["geo_ok"] = True
            else:
                geo_res["forced_retry"] = True
                geo_res["forced_retry_result"] = forced_geo_res
        elif not result["geo_ok"] and geo_res.get("geocode_attempted"):
            geo_res["forced_retry_skipped"] = True
            geo_res["forced_retry_skip_reason"] = "retry_limit_reached"
        geo_res["ok"] = result["geo_ok"]
        _record_step("geo", result["geo_ok"], geo_res, skipped=bool(geo_res.get("skipped")))
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"geo:{type(e).__name__}:{e}")
        _record_step("geo", False, {"ok": False, "error": str(e)})

    try:
        risk_res = _timed_step(result, step_key="risk", fn=lambda: _try_risk_refresh(db, org_id=int(org_id), property_id=int(property_id)))
        risk_res = risk_res if isinstance(risk_res, dict) else {"ok": bool(risk_res)}
        result["risk_ok"] = _is_risk_payload_complete(risk_res)
        risk_res["ok"] = result["risk_ok"]
        _record_step("risk", result["risk_ok"], risk_res, skipped=bool(risk_res.get("skipped")))
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"risk:{type(e).__name__}:{e}")
        _record_step("risk", False, {"ok": False, "error": str(e)})

    try:
        if should_run_inline_rent_refresh():
            rent_res = _timed_step(
                result,
                step_key="rent",
                fn=lambda: refresh_property_rent_assumptions(db, org_id=int(org_id), property_id=int(property_id)),
            )
            rent_res = rent_res if isinstance(rent_res, dict) else {"ok": bool(rent_res)}
            result["rent_ok"] = _is_rent_payload_complete(rent_res)
            rent_res["ok"] = result["rent_ok"]
            _record_step("rent", result["rent_ok"], rent_res, skipped=bool(rent_res.get("skipped")))
        else:
            result["rent_ok"] = False
            result["rent_deferred"] = True
            result["timings_ms"]["rent"] = 0.0
            rent_payload = {"ok": False, "skipped": True, "reason": "deferred_budgeted_refresh", "publish_without_rent": publish_without_rent()}
            _record_step("rent", False, rent_payload, skipped=True)
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"rent:{type(e).__name__}:{e}")
        _record_step("rent", False, {"ok": False, "error": str(e)})

    try:
        from .routers.evaluate import evaluate_property_core
        from .routers.rent import explain_rent

        principal_shim = type("PrincipalShim", (), {"org_id": int(org_id), "user_id": actor_user_id})()

        def _evaluate_step():
            step_result: dict[str, Any] = {}
            if result.get("rent_ok"):
                explain_res = explain_rent(
                    property_id=int(property_id),
                    strategy="section8",
                    payment_standard_pct=None,
                    persist=True,
                    db=db,
                    p=principal_shim,
                )
                step_result["rent_explain"] = {
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
            step_result["evaluate"] = eval_res
            return step_result

        evaluate_res = _timed_step(result, step_key="evaluate", fn=_evaluate_step)
        if isinstance(evaluate_res, dict) and "rent_explain" in evaluate_res:
            result["rent_explain"] = evaluate_res["rent_explain"]
        eval_payload = evaluate_res.get("evaluate", {}) if isinstance(evaluate_res, dict) else {}
        result["evaluate_ok"] = bool(eval_payload.get("ok"))
        _record_step("evaluate", result["evaluate_ok"], eval_payload)
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"evaluate:{type(e).__name__}:{e}")
        _record_step("evaluate", False, {"ok": False, "error": str(e)})

    try:
        from products.ops.backend.src.services.properties.state_machine import sync_property_state

        _timed_step(result, step_key="state", fn=lambda: sync_property_state(db, org_id=int(org_id), property_id=int(property_id)))
        result["state_ok"] = True
        _record_step("state", True, {"ok": True})
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"state:{type(e).__name__}:{e}")
        _record_step("state", False, {"ok": False, "error": str(e)})

    try:
        from products.compliance.backend.src.services import build_workflow_summary

        workflow_res = _timed_step(
            result,
            step_key="workflow",
            fn=lambda: build_workflow_summary(db, org_id=int(org_id), property_id=int(property_id), recompute=False),
        )
        result["workflow_ok"] = True
        _record_step("workflow", True, {"ok": True, "summary": workflow_res})
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"workflow:{type(e).__name__}:{e}")
        _record_step("workflow", False, {"ok": False, "error": str(e)})

    try:
        next_actions_res = _timed_step(
            result,
            step_key="next_actions",
            fn=lambda: _seed_next_actions_if_available(db, org_id=int(org_id), property_id=int(property_id)),
        )
        result["next_actions_ok"] = bool(next_actions_res.get("ok"))
        _record_step("next_actions", result["next_actions_ok"], next_actions_res, skipped=bool(next_actions_res.get("skipped")))
    except Exception as e:
        _safe_rollback(db)
        result["errors"].append(f"next_actions:{type(e).__name__}:{e}")
        _record_step("next_actions", False, {"ok": False, "error": str(e)})

    oks = [
        bool(result.get("geo_ok")),
        bool(result.get("risk_ok")) or bool(result.get("risk", {}).get("skipped")),
        bool(result.get("rent_ok")) or bool(result.get("rent_deferred")),
        bool(result.get("evaluate_ok")),
        bool(result.get("state_ok")),
        bool(result.get("workflow_ok")),
    ]
    result["partial"] = any(oks) and not all(oks)
    result["needs_followup"] = not all(oks)

    try:
        result["completeness_status"] = _update_property_acquisition_completeness(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            pipeline_result=result,
        )
    except Exception as e:
        result["errors"].append(f"completeness:{type(e).__name__}:{e}")

    log.info(
        "post_ingestion_pipeline_complete",
        extra={
            "org_id": int(org_id),
            "property_id": int(property_id),
            "geo_ok": result.get("geo_ok"),
            "risk_ok": result.get("risk_ok"),
            "rent_ok": result.get("rent_ok"),
            "rent_deferred": result.get("rent_deferred"),
            "evaluate_ok": result.get("evaluate_ok"),
            "state_ok": result.get("state_ok"),
            "workflow_ok": result.get("workflow_ok"),
            "next_actions_ok": result.get("next_actions_ok"),
            "partial": result.get("partial"),
            "error_count": len(result.get("errors") or []),
            "timings_ms": result.get("timings_ms"),
        },
    )
    return result


def apply_pipeline_summary(summary: dict[str, Any], pipeline_res: dict[str, Any], property_id: int) -> None:
    summary["post_import_pipeline_attempted"] = int(summary.get("post_import_pipeline_attempted", 0) or 0) + 1
    if pipeline_res.get("geo_ok"):
        summary["geo_enriched"] = int(summary.get("geo_enriched", 0) or 0) + 1
    if pipeline_res.get("risk_ok"):
        summary["risk_scored"] = int(summary.get("risk_scored", 0) or 0) + 1
    if pipeline_res.get("rent_ok"):
        summary["rent_refreshed"] = int(summary.get("rent_refreshed", 0) or 0) + 1
    if pipeline_res.get("rent_deferred"):
        summary["rent_deferred"] = int(summary.get("rent_deferred", 0) or 0) + 1
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
                "timings_ms": dict(pipeline_res.get("timings_ms") or {}),
                "step_status": dict(pipeline_res.get("step_status") or {}),
            }
        )
        summary["post_import_errors"] = post_import_errors

    step_stats = dict(summary.get("post_import_step_stats") or {})
    for step_name, step_payload in dict(pipeline_res.get("step_status") or {}).items():
        bucket = dict(step_stats.get(step_name) or {})
        bucket["attempted"] = int(bucket.get("attempted", 0) or 0) + 1
        if step_payload.get("ok"):
            bucket["ok"] = int(bucket.get("ok", 0) or 0) + 1
        if step_payload.get("skipped"):
            bucket["skipped"] = int(bucket.get("skipped", 0) or 0) + 1
        if not step_payload.get("ok") and not step_payload.get("skipped"):
            bucket["failed"] = int(bucket.get("failed", 0) or 0) + 1
        step_stats[step_name] = bucket
    summary["post_import_step_stats"] = step_stats

    timing_agg = dict(summary.get("post_import_timing_ms_total") or {})
    for step_name, duration in dict(pipeline_res.get("timings_ms") or {}).items():
        try:
            timing_agg[step_name] = round(float(timing_agg.get(step_name, 0.0) or 0.0) + float(duration or 0.0), 2)
        except Exception:
            continue
    summary["post_import_timing_ms_total"] = timing_agg
    