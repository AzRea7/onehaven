from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.auth import get_principal, require_operator
from onehaven_platform.backend.src.db import get_db
from onehaven_platform.backend.src.models import IngestionRun, Property
from onehaven_platform.backend.src.schemas import (
    IngestionOverviewOut,
    IngestionRunListItem,
    IngestionSourceCreate,
    IngestionSourceOut,
    IngestionSourceUpdate,
)
from products.acquire.backend.src.services.ingestion_run_execute import execute_source_sync
from products.acquire.backend.src.services.ingestion_run_service import get_ingestion_overview, list_runs
from products.acquire.backend.src.services.ingestion_scheduler_service import (
    collapse_dispatches_to_primary_source,
    list_default_daily_markets,
)
from products.acquire.backend.src.services.ingestion_source_service import (
    create_source,
    ensure_default_manual_sources,
    ensure_market_slug_on_sources,
    ensure_sources_for_supported_markets,
    get_source,
    list_sources,
    update_source,
)
from products.intelligence.backend.src.services.market_catalog_service import list_active_supported_markets
from products.intelligence.backend.src.services.market_sync_service import build_supported_market_sync_plan_for_db
from products.intelligence.backend.src.services.portfolio_watchlist_service import (
    delete_search_preset,
    delete_watchlist,
    list_search_presets,
    list_watchlists,
    upsert_search_preset,
    upsert_watchlist,
)
from onehaven_platform.backend.src.jobs.ingestion_tasks import (
    daily_market_refresh_task,
    sync_due_sources_task,
    sync_source_task,
)
from products.intelligence.backend.src.services.property_tax_enrichment_service import enrich_property_tax
from products.intelligence.backend.src.services.property_insurance_enrichment_service import enrich_property_insurance

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


class WatchlistIn(BaseModel):
    name: str
    description: str | None = None
    filters_json: dict[str, Any] = Field(default_factory=dict)
    sort_json: dict[str, Any] = Field(default_factory=dict)
    is_default: bool = False


class SearchPresetIn(BaseModel):
    name: str
    filters_json: dict[str, Any] = Field(default_factory=dict)
    sort_json: dict[str, Any] = Field(default_factory=dict)


def _principal_user_id(p: Any) -> int | None:
    for attr in ("user_id", "id"):
        value = getattr(p, attr, None)
        if value is not None:
            try:
                return int(value)
            except Exception:
                return None
    return None


def _resolve_market_slug_or_400(slug: str) -> str:
    slug = (slug or "").strip().lower()

    markets = list_active_supported_markets()
    valid_slugs = {str(m["slug"]).strip().lower() for m in markets}

    if slug in valid_slugs:
        return slug

    for m in markets:
        candidate = str(m.get("slug") or "").strip().lower()
        if slug and slug in candidate:
            return candidate

    raise HTTPException(
        status_code=400,
        detail={
            "error": "invalid_market_slug",
            "message": f"Invalid market_slug '{slug}'",
            "valid_options": sorted(valid_slugs),
        },
    )


def _normalize_optional_text(value: Any) -> str | None:
    s = str(value or "").strip()
    if not s:
        return None
    if s.lower() in {"any", "all", "none", "null"}:
        return None
    return s


def _normalize_zip_codes(value: Any) -> list[str] | None:
    if value is None:
        return None

    raw_list: list[str] = []

    if isinstance(value, str):
        raw_list = [x.strip() for x in value.split(",") if x.strip()]
    elif isinstance(value, list):
        for item in value:
            s = str(item or "").strip()
            if s:
                raw_list.append(s)

    out: list[str] = []
    seen: set[str] = set()
    for z in raw_list:
        if z not in seen:
            out.append(z)
            seen.add(z)

    return out or None


def _normalize_price_buckets(value: Any) -> list[list[float]] | None:
    if value is None or not isinstance(value, list):
        return None

    out: list[list[float]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        try:
            lo = float(item[0]) if item[0] is not None else None
            hi = float(item[1]) if item[1] is not None else None
        except Exception:
            continue

        if lo is None or hi is None:
            continue
        if lo > hi:
            raise ValueError("Each price bucket must have min <= max")

        out.append([lo, hi])

    return out or None


def _pipeline_outcome(summary_json: dict[str, Any] | None) -> dict[str, Any]:
    summary = dict(summary_json or {})
    return {
        "records_seen": int(summary.get("records_seen", 0) or 0),
        "records_imported": int(summary.get("records_imported", 0) or 0),
        "properties_created": int(summary.get("properties_created", 0) or 0),
        "properties_updated": int(summary.get("properties_updated", 0) or 0),
        "deals_created": int(summary.get("deals_created", 0) or 0),
        "deals_updated": int(summary.get("deals_updated", 0) or 0),
        "rent_rows_upserted": int(summary.get("rent_rows_upserted", 0) or 0),
        "photos_upserted": int(summary.get("photos_upserted", 0) or 0),
        "duplicates_skipped": int(summary.get("duplicates_skipped", 0) or 0),
        "invalid_rows": int(summary.get("invalid_rows", 0) or 0),
        "filtered_out": int(summary.get("filtered_out", 0) or 0),
        "location_automation_enabled": bool(summary.get("location_automation_enabled", False)),
        "enrichments_completed": {
            "geo": int(summary.get("geo_enriched", 0) or 0),
            "risk": int(summary.get("risk_scored", 0) or 0),
            "rent": int(summary.get("rent_refreshed", 0) or 0),
        },
        "evaluations_completed": int(summary.get("evaluated", 0) or 0),
        "workflow": {
            "state_synced": int(summary.get("state_synced", 0) or 0),
            "workflow_synced": int(summary.get("workflow_synced", 0) or 0),
            "next_actions_seeded": int(summary.get("next_actions_seeded", 0) or 0),
        },
        "failures": int(summary.get("post_import_failures", 0) or 0),
        "partials": int(summary.get("post_import_partials", 0) or 0),
        "errors": list(summary.get("post_import_errors") or []),
        "filter_reason_counts": dict(summary.get("filter_reason_counts") or {}),
        "normal_path": bool(summary.get("normal_path", True)),
    }


def _run_response(row: IngestionRun) -> dict[str, Any]:
    summary = dict(getattr(row, "summary_json", None) or {})
    return {
        "ok": True,
        "run_id": getattr(row, "id", None),
        "status": getattr(row, "status", None),
        "source_id": getattr(row, "source_id", None),
        "trigger_type": getattr(row, "trigger_type", None),
        "summary_json": summary,
        "pipeline_outcome": _pipeline_outcome(summary),
        "started_at": getattr(row, "started_at", None),
        "finished_at": getattr(row, "finished_at", None),
        "created_at": getattr(row, "created_at", None),
        "updated_at": getattr(row, "updated_at", None),
    }


def _must_get_run(db: Session, *, org_id: int, run_id: int) -> IngestionRun:
    row = db.scalar(
        select(IngestionRun).where(
            IngestionRun.id == int(run_id),
            IngestionRun.org_id == int(org_id),
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="Run not found")
    return row


def _source_debug_snapshot(row: Any) -> dict[str, Any]:
    config_json = dict(getattr(row, "config_json", None) or {})
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "provider": str(getattr(row, "provider", "") or ""),
        "slug": str(getattr(row, "slug", "") or ""),
        "display_name": str(getattr(row, "display_name", "") or ""),
        "is_enabled": bool(getattr(row, "is_enabled", False)),
        "status": str(getattr(row, "status", "") or ""),
        "market_slug": str(config_json.get("market_slug") or "").strip().lower() or None,
        "config_city": str(config_json.get("city") or "").strip() or None,
        "config_county": str(config_json.get("county") or "").strip() or None,
        "sync_interval_minutes": int(getattr(row, "sync_interval_minutes", 0) or 0),
    }


def _market_dispatch_debug(
    db: Session,
    *,
    org_id: int,
    market_slug: str,
) -> dict[str, Any]:
    normalized_market_slug = str(market_slug or "").strip().lower()

    all_sources = list_sources(db, org_id=org_id)
    enabled_sources = [s for s in all_sources if bool(getattr(s, "is_enabled", False))]

    matched_enabled = []
    matched_any = []

    for row in all_sources:
        snap = _source_debug_snapshot(row)
        slug = snap["slug"]
        row_market_slug = snap["market_slug"]

        matched = (
            row_market_slug == normalized_market_slug
            or (normalized_market_slug and normalized_market_slug in slug)
        )
        if not matched:
            continue

        matched_any.append(snap)
        if snap["is_enabled"]:
            matched_enabled.append(snap)

    return {
        "market_slug": normalized_market_slug,
        "total_sources": len(all_sources),
        "enabled_sources": len(enabled_sources),
        "matched_sources_any_state": matched_any,
        "matched_sources_enabled": matched_enabled,
    }


class IngestionSyncLaunchRequest(BaseModel):
    trigger_type: str = "manual"
    state: str | None = "MI"
    county: str | None = None
    city: str | None = None
    zip_code: str | None = None
    zip_codes: list[str] | str | None = None
    min_price: float | None = None
    max_price: float | None = None
    min_bedrooms: int | None = None
    min_bathrooms: float | None = None
    property_type: str | None = None
    price_buckets: list[list[float]] | None = None
    pages_per_shard: int | None = Field(default=1, ge=1, le=3)
    limit: int = Field(default=100, ge=1, le=500)
    execute_inline: bool = False

    @model_validator(mode="after")
    def validate_ranges(self):
        self.state = _normalize_optional_text(self.state)
        self.county = _normalize_optional_text(self.county)
        self.city = _normalize_optional_text(self.city)
        self.zip_code = _normalize_optional_text(self.zip_code)
        self.property_type = _normalize_optional_text(self.property_type)

        normalized_zip_codes = _normalize_zip_codes(self.zip_codes)
        self.zip_codes = normalized_zip_codes if normalized_zip_codes is not None else None

        normalized_price_buckets = _normalize_price_buckets(self.price_buckets)
        self.price_buckets = normalized_price_buckets if normalized_price_buckets is not None else None

        if self.min_price is not None and self.max_price is not None and self.min_price > self.max_price:
            raise ValueError("min_price cannot be greater than max_price")

        if self.zip_code and not self.zip_codes:
            self.zip_codes = [self.zip_code]
        elif self.zip_code and self.zip_codes:
            combined = [self.zip_code, *list(self.zip_codes)]
            self.zip_codes = _normalize_zip_codes(combined)

        return self

    def runtime_config(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        payload["limit"] = max(1, int(payload.get("limit") or 100))
        payload["trigger_type"] = str(payload.get("trigger_type") or "manual")
        payload["execute_inline"] = bool(payload.get("execute_inline", False))

        if payload.get("pages_per_shard") is not None:
            payload["pages_per_shard"] = max(1, min(3, int(payload["pages_per_shard"])))

        zip_codes = payload.get("zip_codes")
        if isinstance(zip_codes, list) and not zip_codes:
            payload.pop("zip_codes", None)

        price_buckets = payload.get("price_buckets")
        if isinstance(price_buckets, list) and not price_buckets:
            payload.pop("price_buckets", None)

        return payload


class SupportedMarketSyncRequest(BaseModel):
    market_slug: str
    limit: int | None = Field(default=None, ge=1, le=500)
    execute_inline: bool = False

    @model_validator(mode="after")
    def validate_target(self):
        self.market_slug = _normalize_optional_text(self.market_slug)
        if not self.market_slug:
            raise ValueError("market_slug is required")
        return self


@router.get("/overview", response_model=IngestionOverviewOut)
def overview(db: Session = Depends(get_db), p=Depends(get_principal)):
    ensure_default_manual_sources(db, org_id=p.org_id)
    ensure_market_slug_on_sources(db, org_id=p.org_id)
    payload = get_ingestion_overview(db, org_id=p.org_id)
    payload["daily_markets"] = list_default_daily_markets()
    payload["ui_mode"] = "consolidated"
    payload["normal_path"] = "property_first_pipeline"
    payload["legacy_snapshot_flow_enabled"] = False
    return payload


@router.get("/sources", response_model=list[IngestionSourceOut])
def sources(db: Session = Depends(get_db), p=Depends(get_principal)):
    ensure_default_manual_sources(db, org_id=p.org_id)
    ensure_market_slug_on_sources(db, org_id=p.org_id)
    return list_sources(db, org_id=p.org_id)


@router.post("/sources", response_model=IngestionSourceOut)
def create_ingestion_source(
    payload: IngestionSourceCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    row = create_source(db, org_id=p.org_id, payload=payload)
    db.commit()
    db.refresh(row)
    return row


@router.patch("/sources/{source_id}", response_model=IngestionSourceOut)
def patch_ingestion_source(
    source_id: int,
    payload: IngestionSourceUpdate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    row = get_source(db, org_id=p.org_id, source_id=source_id)
    if not row:
        raise HTTPException(status_code=404, detail="Source not found")

    row = update_source(
        db,
        org_id=int(p.org_id),
        source_id=int(source_id),
        payload=payload,
    )
    db.commit()
    db.refresh(row)
    return row


@router.post("/sources/{source_id}/sync", response_model=dict)
def sync_now(
    source_id: int,
    payload: IngestionSyncLaunchRequest,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    row = get_source(db, org_id=p.org_id, source_id=source_id)
    if not row:
        raise HTTPException(status_code=404, detail="Source not found")

    runtime_config = payload.runtime_config()
    trigger_type = str(runtime_config.pop("trigger_type", "manual") or "manual")
    execute_inline = bool(runtime_config.pop("execute_inline", False))

    if execute_inline:
        run = execute_source_sync(
            db,
            org_id=int(p.org_id),
            source=row,
            trigger_type=trigger_type,
            runtime_config=runtime_config,
        )
        db.commit()
        out = _run_response(run)
        out["queued"] = False
        out["normal_path"] = True
        return out

    db.commit()
    job = sync_source_task.delay(int(p.org_id), int(row.id), trigger_type, runtime_config)
    return {
        "ok": True,
        "queued": True,
        "task_id": str(job.id),
        "source_id": int(row.id),
        "runtime_config": runtime_config,
        "normal_path": True,
    }


@router.post("/sync-market", response_model=dict)
def sync_supported_market(
    payload: SupportedMarketSyncRequest,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    market_slug = _resolve_market_slug_or_400(payload.market_slug)

    ensure_default_manual_sources(db, org_id=p.org_id)
    ensure_market_slug_on_sources(db, org_id=p.org_id)
    supported_result = ensure_sources_for_supported_markets(db, org_id=p.org_id)

    # Persist source creation/repair/adoption before plan generation and queueing.
    db.flush()
    db.commit()

    plan = build_supported_market_sync_plan_for_db(
        db,
        org_id=int(p.org_id),
        market_slug=market_slug,
        limit=payload.limit,
        sync_mode="refresh",
    )

    raw_dispatches = list(plan.get("dispatches") or [])
    dispatches = collapse_dispatches_to_primary_source(raw_dispatches)

    seen_market_slugs: set[str] = set()
    final_dispatches: list[dict[str, Any]] = []
    for dispatch in dispatches:
        market = dict(dispatch.get("market") or {})
        slug = str(market.get("slug") or "").strip().lower()
        if not slug or slug in seen_market_slugs:
            continue
        seen_market_slugs.add(slug)
        final_dispatches.append(dispatch)

    dispatches = final_dispatches

    if not dispatches:
        debug = _market_dispatch_debug(
            db,
            org_id=int(p.org_id),
            market_slug=market_slug,
        )
        raise HTTPException(
            status_code=409,
            detail={
                "error": "no_dispatchable_sources",
                "message": f"No enabled sources found for market '{market_slug}'",
                "hint": "Ensure a rentcast source exists with matching market_slug",
                "market": plan.get("market"),
                "repair_summary": {
                    "created": [str(getattr(s, "slug", "") or "") for s in supported_result.get("created", [])],
                    "repaired": [str(getattr(s, "slug", "") or "") for s in supported_result.get("repaired", [])],
                    "adopted": [str(getattr(s, "slug", "") or "") for s in supported_result.get("adopted", [])],
                    "touched": [str(getattr(s, "slug", "") or "") for s in supported_result.get("touched", [])],
                },
                "dispatch_debug": debug,
            },
        )

    if payload.execute_inline:
        runs: list[dict[str, Any]] = []

        for dispatch in dispatches:
            row = get_source(
                db,
                org_id=p.org_id,
                source_id=int(dispatch["source_id"]),
            )
            if not row:
                continue

            run = execute_source_sync(
                db,
                org_id=int(p.org_id),
                source=row,
                trigger_type=str(dispatch["trigger_type"]),
                runtime_config=dict(dispatch["runtime_config"]),
            )
            runs.append(_run_response(run))

        db.commit()
        return {
            "ok": True,
            "mode": "inline",
            "market": plan.get("market"),
            "runs": runs,
            "dispatches": dispatches,
            "dispatches_seen": len(raw_dispatches),
            "dispatches_executed": len(dispatches),
            "supported_sources_created": [str(getattr(s, "slug", "") or "") for s in supported_result.get("created", [])],
            "supported_sources_repaired": [str(getattr(s, "slug", "") or "") for s in supported_result.get("repaired", [])],
            "supported_sources_adopted": [str(getattr(s, "slug", "") or "") for s in supported_result.get("adopted", [])],
        }

    # Commit again so sync state rows created by plan building are visible to the worker.
    db.commit()

    task_ids: list[str] = []

    for dispatch in dispatches:
        job = sync_source_task.delay(
            int(p.org_id),
            int(dispatch["source_id"]),
            str(dispatch["trigger_type"]),
            dict(dispatch["runtime_config"]),
        )
        task_ids.append(str(job.id))

    return {
        "ok": True,
        "mode": "queued",
        "market": plan.get("market"),
        "queued_count": len(task_ids),
        "task_ids": task_ids,
        "dispatches": dispatches,
        "dispatches_seen": len(raw_dispatches),
        "dispatches_executed": len(dispatches),
        "supported_sources_created": [str(getattr(s, "slug", "") or "") for s in supported_result.get("created", [])],
        "supported_sources_repaired": [str(getattr(s, "slug", "") or "") for s in supported_result.get("repaired", [])],
        "supported_sources_adopted": [str(getattr(s, "slug", "") or "") for s in supported_result.get("adopted", [])],
    }


@router.post("/sync-defaults", response_model=dict)
def sync_default_sources_now(
    payload: IngestionSyncLaunchRequest | None = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    ensure_default_manual_sources(db, org_id=p.org_id)
    ensure_market_slug_on_sources(db, org_id=p.org_id)

    rows = [x for x in list_sources(db, org_id=p.org_id) if bool(x.is_enabled)]
    runtime = (payload or IngestionSyncLaunchRequest()).runtime_config()

    execute_inline = bool(runtime.pop("execute_inline", False))
    trigger_type = str(runtime.pop("trigger_type", "manual") or "manual")

    if execute_inline:
        runs: list[dict[str, Any]] = []
        for row in rows:
            run = execute_source_sync(
                db,
                org_id=int(p.org_id),
                source=row,
                trigger_type=trigger_type,
                runtime_config=runtime,
            )
            runs.append(_run_response(run))
        db.commit()
        return {
            "ok": True,
            "queued": False,
            "runs": runs,
            "source_ids": [int(x.id) for x in rows],
            "normal_path": True,
        }

    db.commit()

    source_ids: list[int] = []
    task_ids: list[str] = []

    for row in rows:
        job = sync_source_task.delay(int(p.org_id), int(row.id), trigger_type, runtime)
        task_ids.append(str(job.id))
        source_ids.append(int(row.id))

    return {
        "ok": True,
        "queued": True,
        "task_ids": task_ids,
        "source_ids": source_ids,
        "normal_path": True,
    }


@router.post("/refresh-daily", response_model=dict)
def refresh_daily_markets(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    db.commit()
    job = daily_market_refresh_task.delay(int(p.org_id))
    return {
        "ok": True,
        "queued": True,
        "task_id": str(job.id),
    }


@router.post("/sync-due", response_model=dict)
def sync_due_sources(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    db.commit()
    job = sync_due_sources_task.delay(int(p.org_id))
    return {
        "ok": True,
        "queued": True,
        "task_id": str(job.id),
    }


@router.get("/runs", response_model=list[IngestionRunListItem])
def runs(
    limit: int = 25,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return list_runs(db, org_id=p.org_id, limit=max(1, min(limit, 200)))


@router.get("/runs/{run_id}", response_model=dict)
def run_detail(
    run_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = _must_get_run(db, org_id=int(p.org_id), run_id=int(run_id))
    return _run_response(row)


@router.get("/watchlists", response_model=list[dict])
def get_watchlists(db: Session = Depends(get_db), p=Depends(get_principal)):
    return list_watchlists(db, org_id=int(p.org_id), user_id=_principal_user_id(p))


@router.post("/watchlists", response_model=dict)
def save_watchlist(payload: WatchlistIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    result = upsert_watchlist(
        db,
        org_id=int(p.org_id),
        user_id=_principal_user_id(p),
        name=payload.name,
        description=payload.description,
        filters_json=payload.filters_json,
        sort_json=payload.sort_json,
        is_default=payload.is_default,
    )
    db.commit()
    return result


@router.delete("/watchlists/{name}", response_model=dict)
def remove_watchlist(name: str, db: Session = Depends(get_db), p=Depends(get_principal)):
    result = delete_watchlist(db, org_id=int(p.org_id), user_id=_principal_user_id(p), name=name)
    db.commit()
    return result


@router.get("/search-presets", response_model=list[dict])
def get_search_presets(db: Session = Depends(get_db), p=Depends(get_principal)):
    return list_search_presets(db, org_id=int(p.org_id), user_id=_principal_user_id(p))


@router.post("/search-presets", response_model=dict)
def save_search_preset(payload: SearchPresetIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    result = upsert_search_preset(
        db,
        org_id=int(p.org_id),
        user_id=_principal_user_id(p),
        name=payload.name,
        filters_json=payload.filters_json,
        sort_json=payload.sort_json,
    )
    db.commit()
    return result


@router.delete("/search-presets/{name}", response_model=dict)
def remove_search_preset(name: str, db: Session = Depends(get_db), p=Depends(get_principal)):
    result = delete_search_preset(db, org_id=int(p.org_id), user_id=_principal_user_id(p), name=name)
    db.commit()
    return result


@router.post("/sources/ensure-supported", response_model=dict)
def ensure_supported_sources(
    db: Session = Depends(get_db),
    principal: Any = Depends(get_principal),
    _op=Depends(require_operator),
):
    result = ensure_sources_for_supported_markets(db, org_id=int(principal.org_id))
    db.commit()

    return {
        "ok": True,
        "created": len(result.get("created", [])),
        "repaired": len(result.get("repaired", [])),
        "adopted": len(result.get("adopted", [])),
        "touched": len(result.get("touched", [])),
        "sources": [str(s.slug) for s in result.get("touched", [])],
    }


class ReEnrichFinancialsIn(BaseModel):
    property_ids: list[int] = Field(default_factory=list)
    force: bool = True


@router.post("/re-enrich/financials", response_model=dict)
def re_enrich_financials(
    payload: ReEnrichFinancialsIn | None = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    request = payload or ReEnrichFinancialsIn()
    property_ids = [int(x) for x in request.property_ids]
    if not property_ids:
        property_ids = list(db.scalars(select(Property.id).where(Property.org_id == int(p.org_id)).order_by(Property.id)).all())

    rows: list[dict[str, Any]] = []
    for property_id in property_ids:
        rows.append({
            "property_id": property_id,
            "tax": enrich_property_tax(db, org_id=int(p.org_id), property_id=property_id, force=bool(request.force)),
            "insurance": enrich_property_insurance(db, org_id=int(p.org_id), property_id=property_id, force=bool(request.force)),
        })
    db.commit()
    return {"ok": True, "count": len(rows), "rows": rows}
