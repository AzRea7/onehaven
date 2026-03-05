# onehaven_decision_engine/backend/app/routers/photos.py
from __future__ import annotations

import os
from datetime import datetime
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.auth import get_principal, require_operator
from app.db import get_db
from app.models import Property, PropertyPhoto

router = APIRouter(prefix="/photos", tags=["photos"])


def _now() -> datetime:
    return datetime.utcnow()


@router.post("/upload", response_model=dict)
async def upload_photo(
    property_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    prop = db.scalar(select(Property).where(Property.org_id == p.org_id, Property.id == int(property_id)))
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    # Simple local storage (dev). Swap to S3 later.
    base = os.getenv("PHOTO_UPLOAD_DIR", "/tmp/onehaven_uploads")
    os.makedirs(base, exist_ok=True)
    fn = f"p{property_id}_{int(_now().timestamp())}_{file.filename}"
    path = os.path.join(base, fn)

    content = await file.read()
    with open(path, "wb") as f:
        f.write(content)

    row = PropertyPhoto(
        org_id=p.org_id,
        property_id=int(property_id),
        storage_key=fn,
        url=f"/api/photos/raw/{fn}",
        created_at=_now(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return {"ok": True, "photo": {"id": int(row.id), "url": row.url}}


@router.get("/raw/{storage_key}")
def raw(storage_key: str):
    base = os.getenv("PHOTO_UPLOAD_DIR", "/tmp/onehaven_uploads")
    path = os.path.join(base, storage_key)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Not found")
    with open(path, "rb") as f:
        b = f.read()
    return b