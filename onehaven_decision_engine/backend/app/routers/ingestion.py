from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request
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
from ..tasks.ingestion_tasks import (
    daily_market_refresh_task,
    sync_due_sources_task,
    sync_source_task,
)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


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
    if value is None:
        return None
    if not isinstance(value, list):
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


@router.get("/overview", response_model=IngestionOverviewOut)
def overview(db: Session = Depends(get_db), p=Depends(get_principal)):
    ensure_default_manual_sources(db, org_id=p.org_id)
    payload = get_ingestion_overview(db, org_id=p.org_id)
    payload["daily_markets"] = list_default_daily_markets()
    payload["ui_mode"] = "consolidated"
    return payload


@router.get("/sources", response_model=list[IngestionSourceOut])
def sources(db: Session = Depends(get_db), p=Depends(get_principal)):
    ensure_default_manual_sources(db, org_id=p.org_id)
    rows = list_sources(db, org_id=p.org_id)
    return rows


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
        return out

    job = sync_source_task.delay(p.org_id, row.id, trigger_type, runtime_config)
    return {
        "ok": True,
        "queued": True,
        "task_id": job.id,
        "source_id": row.id,
        "runtime_config": runtime_config,
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
    runtime.pop("trigger_type", None)

    if execute_inline:
        runs: list[dict[str, Any]] = []
        for row in rows:
            run = execute_source_sync(
                db,
                org_id=int(p.org_id),
                source=row,
                trigger_type="manual",
                runtime_config=runtime,
            )
            runs.append(_run_response(run))
        return {"ok": True, "queued": False, "runs": runs}

    queued: list[int] = []
    for row in rows:
        sync_source_task.delay(int(p.org_id), int(row.id), "manual", runtime)
        queued.append(int(row.id))
    return {"ok": True, "queued": len(queued), "source_ids": queued, "runtime_config": runtime}


@router.post("/sync-due", response_model=dict)
def queue_due_sources(_op=Depends(require_operator)):
    job = sync_due_sources_task.delay()
    return {"ok": True, "queued": True, "task_id": job.id}


@router.post("/daily-refresh", response_model=dict)
def queue_daily_market_refresh(_op=Depends(require_operator)):
    job = daily_market_refresh_task.delay()
    return {
        "ok": True,
        "queued": True,
        "task_id": job.id,
        "markets": list_default_daily_markets(),
    }


@router.get("/runs", response_model=list[IngestionRunListItem])
def runs(limit: int = 50, db: Session = Depends(get_db), p=Depends(get_principal)):
    return list_runs(db, org_id=p.org_id, limit=limit)


@router.get("/runs/{run_id}", response_model=dict)
def run_detail(run_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.get(IngestionRun, int(run_id))
    if not row or int(row.org_id) != int(p.org_id):
        raise HTTPException(status_code=404, detail="Run not found")

    summary = dict(row.summary_json or {})
    return {
        "id": row.id,
        "source_id": row.source_id,
        "trigger_type": row.trigger_type,
        "status": row.status,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "records_seen": row.records_seen,
        "records_imported": row.records_imported,
        "properties_created": row.properties_created,
        "properties_updated": row.properties_updated,
        "deals_created": row.deals_created,
        "deals_updated": row.deals_updated,
        "rent_rows_upserted": row.rent_rows_upserted,
        "photos_upserted": row.photos_upserted,
        "duplicates_skipped": row.duplicates_skipped,
        "invalid_rows": row.invalid_rows,
        "retry_count": row.retry_count,
        "error_summary": row.error_summary,
        "error_json": row.error_json,
        "summary_json": summary,
        "pipeline_outcome": _pipeline_outcome(summary),
    }


@router.post("/webhooks/{provider}/{source_slug}", response_model=dict)
async def webhook_ingest(
    provider: str,
    source_slug: str,
    payload: IngestionWebhookIn,
    request: Request,
    x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
    db: Session = Depends(get_db),
):
    rows = list_sources(db, org_id=1)
    source = next((x for x in rows if x.provider == provider and x.slug == source_slug), None)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")

    if source.webhook_secret_hint and not x_webhook_secret:
        raise HTTPException(status_code=401, detail="Missing webhook secret")

    source.config_json = source.config_json or {}
    source.config_json["sample_rows"] = [payload.payload]
    db.add(source)
    db.commit()
    db.refresh(source)

    run = execute_source_sync(
        db,
        org_id=int(source.org_id),
        source=source,
        trigger_type="webhook",
    )
    out = _run_response(run)
    return out
