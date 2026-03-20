# backend/app/routers/automation.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_operator

router = APIRouter(prefix="/automation", tags=["automation"])


@router.post("/ingest/run", response_model=dict, deprecated=True)
def ingest_run(_op=Depends(require_operator)):
    """
    Legacy snapshot/manual importer path intentionally disabled.

    Normal ingestion must go through:
      - /ingestion/sources/{source_id}/sync
      - /ingestion/sync-defaults
      - scheduled ingestion tasks

    This prevents the app from using the old snapshot-centric execution path.
    """
    raise HTTPException(
        status_code=410,
        detail={
            "code": "legacy_ingest_route_removed",
            "message": "Use /ingestion/* property-first sync routes instead of /automation/ingest/run.",
        },
    )