# backend/app/routers/automation.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..domain.importers.zillow import ZillowImporter
from ..domain.importers.investorlift import InvestorLiftImporter

router = APIRouter(prefix="/automation", tags=["automation"])


@router.post("/ingest/run", response_model=dict)
def ingest_run(
    source: str = Query(..., description="zillow|investorlift"),
    snapshot_id: Optional[int] = Query(default=None, description="If omitted, importer should create a new snapshot"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Manual trigger for ingestion.
    Phase 6 intent: later run this daily via scheduler, but keep it manual until you’ve used this on a real deal.
    """
    s = source.strip().lower()

    if s == "zillow":
        imp = ZillowImporter(db=db)
        res = imp.run(snapshot_id=snapshot_id, limit=limit)
        return {"source": "zillow", "result": res}

    if s == "investorlift":
        imp = InvestorLiftImporter(db=db)
        res = imp.run(snapshot_id=snapshot_id, limit=limit)
        return {"source": "investorlift", "result": res}

    return {"error": f"unknown source={source}"}
