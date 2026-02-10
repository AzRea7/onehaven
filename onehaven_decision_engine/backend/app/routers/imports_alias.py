# onehaven_decision_engine/backend/app/routers/imports_alias.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from .imports import import_status

router = APIRouter(prefix="/imports", tags=["import"])

@router.get("/status")
def imports_status(snapshot_id: int = Query(...), db: Session = Depends(get_db)):
    return import_status(snapshot_id=snapshot_id, db=db)
