from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from .imports import import_status

router = APIRouter(prefix="/imports", tags=["import"])


@router.get("/status")
def imports_status(
    run_id: int | None = Query(default=None, description="Preferred normal-path ingestion run id"),
    snapshot_id: int | None = Query(default=None, description="Legacy only; manual CSV audit/status"),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    return import_status(
        run_id=run_id,
        snapshot_id=snapshot_id,
        db=db,
        principal=principal,
    )