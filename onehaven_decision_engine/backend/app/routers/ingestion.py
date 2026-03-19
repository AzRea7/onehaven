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
from ..services.ingestion_source_service import (
    create_source,
    ensure_default_manual_sources,
    get_source,
    list_sources,
    update_source,
)
from ..tasks.ingestion_tasks import daily_market_refresh_task, sync_due_sources_task, sync_source_task

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


class IngestionSyncLaunchRequest(BaseModel):
    trigger_type: str = "manual"
    state: str | None = "MI"
    county: str | None = None
    city: str | None = None
    min_price: float | None = None
    max_price: float | None = None
    min_bedrooms: int | None = None
    min_bathrooms: float | None = None
    property_type: str | None = None
    limit: int = Field(default=100, ge=1)

    @model_validator(mode="after")
    def validate_ranges(self):
        if (
            self.min_price is not None
            and self.max_price is not None
            and self.min_price > self.max_price
        ):
            raise ValueError("min_price cannot be greater than max_price")
        return self

    def runtime_config(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        payload["limit"] = max(1, int(payload.get("limit") or 100))
        payload["trigger_type"] = str(payload.get("trigger_type") or "manual")
        return payload


@router.get("/overview", response_model=IngestionOverviewOut)
def overview(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    ensure_default_manual_sources(db, org_id=p.org_id)
    return get_ingestion_overview(db, org_id=p.org_id)


@router.get("/sources", response_model=list[IngestionSourceOut])
def sources(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
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

    job = sync_source_task.delay(
        p.org_id,
        row.id,
        trigger_type,
        runtime_config,
    )
    return {
        "ok": True,
        "queued": True,
        "task_id": job.id,
        "source_id": row.id,
    }


@router.post("/sync-defaults", response_model=dict)
def sync_default_sources_now(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    """
    Convenience endpoint for the cleaner ingestion UI:
    queue syncs for all default warm-market sources at once.
    """
    ensure_default_manual_sources(db, org_id=p.org_id)
    rows = list_sources(db, org_id=p.org_id)

    queued: list[int] = []
    for row in rows:
        if not bool(row.is_enabled):
            continue
        sync_source_task.delay(int(p.org_id), int(row.id), "manual", {})
        queued.append(int(row.id))

    return {"ok": True, "queued": len(queued), "source_ids": queued}


@router.post("/sync-due", response_model=dict)
def queue_due_sources(
    _op=Depends(require_operator),
):
    job = sync_due_sources_task.delay()
    return {"ok": True, "queued": True, "task_id": job.id}


@router.post("/daily-refresh", response_model=dict)
def queue_daily_market_refresh(
    _op=Depends(require_operator),
):
    job = daily_market_refresh_task.delay()
    return {"ok": True, "queued": True, "task_id": job.id}


@router.get("/runs", response_model=list[IngestionRunListItem])
def runs(
    limit: int = 50,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return list_runs(db, org_id=p.org_id, limit=limit)


@router.get("/runs/{run_id}", response_model=dict)
def run_detail(
    run_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.get(IngestionRun, int(run_id))
    if not row or int(row.org_id) != int(p.org_id):
        raise HTTPException(status_code=404, detail="Run not found")
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
        "summary_json": row.summary_json,
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

    run = execute_source_sync(db, org_id=int(source.org_id), source=source, trigger_type="webhook")
    return {"ok": True, "run_id": run.id, "status": run.status}
