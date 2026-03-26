from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.orm import Session

from ..auth import get_principal, require_operator
from ..db import get_db
from ..models import IngestionRun
from ..schemas import (
    IngestionOverviewOut,
    IngestionRunListItem,
    IngestionSourceCreate,
    IngestionSourceOut,
    IngestionSourceUpdate,
    IngestionWebhookIn,
)
from ..services.ingestion_run_execute import execute_source_sync
from ..services.ingestion_run_service import get_ingestion_overview, list_runs
from ..services.ingestion_scheduler_service import list_default_daily_markets
from ..services.ingestion_source_service import (
    create_source,
    ensure_default_manual_sources,
    get_source,
    list_sources,
    update_source,
)
from ..services.market_sync_service import build_supported_market_sync_plan_for_db
from ..services.portfolio_watchlist_service import (
    delete_search_preset,
    delete_watchlist,
    list_search_presets,
    list_watchlists,
    upsert_search_preset,
    upsert_watchlist,
)
from ..tasks.ingestion_tasks import (
    daily_market_refresh_task,
    sync_due_sources_task,
    sync_source_task,
)

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
    market_slug: str | None = None
    city: str | None = None
    state: str = "MI"
    execute_inline: bool = False

    @model_validator(mode="after")
    def validate_target(self):
        self.market_slug = _normalize_optional_text(self.market_slug)
        self.city = _normalize_optional_text(self.city)
        self.state = _normalize_optional_text(self.state) or "MI"

        if not self.market_slug and not self.city:
            raise ValueError("market_slug or city is required")

        return self


@router.get("/overview", response_model=IngestionOverviewOut)
def overview(db: Session = Depends(get_db), p=Depends(get_principal)):
    ensure_default_manual_sources(db, org_id=p.org_id)
    payload = get_ingestion_overview(db, org_id=p.org_id)
    payload["daily_markets"] = list_default_daily_markets()
    payload["ui_mode"] = "consolidated"
    payload["normal_path"] = "property_first_pipeline"
    payload["legacy_snapshot_flow_enabled"] = False
    return payload


@router.get("/sources", response_model=list[IngestionSourceOut])
def sources(db: Session = Depends(get_db), p=Depends(get_principal)):
    ensure_default_manual_sources(db, org_id=p.org_id)
    return list_sources(db, org_id=p.org_id)


@router.post("/sources", response_model=IngestionSourceOut)
def create_ingestion_source(
    payload: IngestionSourceCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    return create_source(db, org_id=p.org_id, payload=payload)


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
    return update_source(db, row=row, payload=payload)


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
        out = _run_response(run)
        out["queued"] = False
        out["normal_path"] = True
        return out

    job = sync_source_task.delay(p.org_id, row.id, trigger_type, runtime_config)
    return {
        "ok": True,
        "queued": True,
        "task_id": job.id,
        "source_id": row.id,
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
    plan = build_supported_market_sync_plan_for_db(
        db,
        org_id=int(p.org_id),
        market_slug=payload.market_slug,
        city=payload.city,
        state=payload.state or "MI",
    )

    if not plan["covered"]:
        raise HTTPException(status_code=404, detail="Supported market not found")

    dispatches = list(plan["dispatches"] or [])
    if not dispatches:
        raise HTTPException(status_code=409, detail="No enabled sources available for supported market")

    if payload.execute_inline:
        runs: list[dict[str, Any]] = []
        for dispatch in dispatches:
            row = get_source(db, org_id=p.org_id, source_id=int(dispatch["source_id"]))
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

        return {
            "ok": True,
            "covered": True,
            "queued": False,
            "market": plan["market"],
            "runs": runs,
            "dispatches": dispatches,
            "queued_count": len(runs),
        }

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
        "covered": True,
        "queued": True,
        "market": plan["market"],
        "queued_count": len(task_ids),
        "task_ids": task_ids,
        "dispatches": dispatches,
    }


@router.post("/sync-defaults", response_model=dict)
def sync_default_sources_now(
    payload: IngestionSyncLaunchRequest | None = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    ensure_default_manual_sources(db, org_id=p.org_id)
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
        return {
            "ok": True,
            "queued": False,
            "runs": runs,
            "source_ids": [int(x.id) for x in rows],
            "normal_path": True,
        }

    source_ids: list[int] = []
    for row in rows:
        sync_source_task.delay(int(p.org_id), int(row.id), trigger_type, runtime)
        source_ids.append(int(row.id))

    return {
        "ok": True,
        "queued": len(source_ids),
        "source_ids": source_ids,
        "normal_path": True,
    }


@router.post("/refresh-daily", response_model=dict)
def refresh_daily_markets(
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    job = daily_market_refresh_task.delay(int(p.org_id))
    return {
        "ok": True,
        "queued": True,
        "task_id": str(job.id),
    }


@router.post("/sync-due", response_model=dict)
def sync_due_sources(
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
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


@router.get("/watchlists", response_model=list[dict])
def get_watchlists(db: Session = Depends(get_db), p=Depends(get_principal)):
    return list_watchlists(db, org_id=int(p.org_id), user_id=_principal_user_id(p))


@router.post("/watchlists", response_model=dict)
def save_watchlist(payload: WatchlistIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    return upsert_watchlist(
        db,
        org_id=int(p.org_id),
        user_id=_principal_user_id(p),
        name=payload.name,
        description=payload.description,
        filters_json=payload.filters_json,
        sort_json=payload.sort_json,
        is_default=payload.is_default,
    )


@router.delete("/watchlists/{name}", response_model=dict)
def remove_watchlist(name: str, db: Session = Depends(get_db), p=Depends(get_principal)):
    return delete_watchlist(db, org_id=int(p.org_id), user_id=_principal_user_id(p), name=name)


@router.get("/search-presets", response_model=list[dict])
def get_search_presets(db: Session = Depends(get_db), p=Depends(get_principal)):
    return list_search_presets(db, org_id=int(p.org_id), user_id=_principal_user_id(p))


@router.post("/search-presets", response_model=dict)
def save_search_preset(payload: SearchPresetIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    return upsert_search_preset(
        db,
        org_id=int(p.org_id),
        user_id=_principal_user_id(p),
        name=payload.name,
        filters_json=payload.filters_json,
        sort_json=payload.sort_json,
    )


@router.delete("/search-presets/{name}", response_model=dict)
def remove_search_preset(name: str, db: Session = Depends(get_db), p=Depends(get_principal)):
    return delete_search_preset(db, org_id=int(p.org_id), user_id=_principal_user_id(p), name=name)
