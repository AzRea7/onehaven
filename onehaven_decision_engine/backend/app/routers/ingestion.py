from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Request
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
    IngestionSyncRequest,
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
from ..tasks.ingestion_tasks import sync_source_task

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


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
    payload: IngestionSyncRequest,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    row = get_source(db, org_id=p.org_id, source_id=source_id)
    if not row:
        raise HTTPException(status_code=404, detail="Source not found")

    async_enabled = True
    if async_enabled:
        job = sync_source_task.delay(p.org_id, row.id, payload.trigger_type or "manual")
        return {"ok": True, "queued": True, "task_id": job.id, "source_id": row.id}

    run = execute_source_sync(db, org_id=p.org_id, source=row, trigger_type=payload.trigger_type or "manual")
    return {"ok": True, "queued": False, "run_id": run.id, "status": run.status}


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
    # Intentionally not using principal auth here because providers call this.
    # In production, validate signature/HMAC. This is the seam.
    rows = list_sources(db, org_id=1)  # replace with org lookup by route/secret in production
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