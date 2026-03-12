# backend/app/routers/photos.py
from __future__ import annotations

import os
from datetime import datetime

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import get_principal, require_operator
from app.db import get_db
from app.models import Property, PropertyPhoto
from app.schemas import PropertyPhotoCreate, PropertyPhotoOut
from app.services.property_photo_service import (
    create_uploaded_photo,
    ensure_property_exists,
    list_property_photos,
    upsert_zillow_photos,
)
from app.services.zillow_photo_source import classify_photo_kind

router = APIRouter(prefix="/photos", tags=["photos"])


def _now() -> datetime:
    return datetime.utcnow()


def _upload_dir() -> str:
    base = os.getenv("PHOTO_UPLOAD_DIR", "/tmp/onehaven_uploads")
    os.makedirs(base, exist_ok=True)
    return base


@router.get("/{property_id}", response_model=list[PropertyPhotoOut])
def get_property_photos(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    ensure_property_exists(db, org_id=p.org_id, property_id=property_id)
    return list_property_photos(db, org_id=p.org_id, property_id=property_id)


@router.post("/sync-zillow/{property_id}", response_model=dict)
def sync_zillow_photos(
    property_id: int,
    payload: PropertyPhotoCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    urls = [payload.url] if payload.url else []
    result = upsert_zillow_photos(
        db,
        org_id=p.org_id,
        property_id=property_id,
        urls=urls,
    )
    return {"ok": True, **result}


@router.post("/upload", response_model=PropertyPhotoOut)
async def upload_photo(
    property_id: int = Form(...),
    kind: str = Form("unknown"),
    label: str | None = Form(default=None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    prop = db.scalar(
        select(Property).where(
            Property.org_id == p.org_id,
            Property.id == int(property_id),
        )
    )
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    upload_dir = _upload_dir()
    safe_name = f"p{property_id}_{int(_now().timestamp())}_{file.filename}"
    path = os.path.join(upload_dir, safe_name)

    content = await file.read()
    with open(path, "wb") as f:
        f.write(content)

    row = create_uploaded_photo(
        db,
        org_id=p.org_id,
        property_id=int(property_id),
        url=f"/api/photos/raw/{safe_name}",
        storage_key=safe_name,
        kind=kind or classify_photo_kind(file.filename or ""),
        label=label,
        content_type=file.content_type,
    )
    return row


@router.get("/raw/{storage_key}")
def raw_photo(storage_key: str):
    path = os.path.join(_upload_dir(), storage_key)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Photo not found")
    return FileResponse(path)


@router.delete("/{photo_id}", response_model=dict)
def delete_photo(
    photo_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    row = db.scalar(
        select(PropertyPhoto).where(
            PropertyPhoto.id == photo_id,
            PropertyPhoto.org_id == p.org_id,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="Photo not found")

    if row.storage_key:
        path = os.path.join(_upload_dir(), row.storage_key)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

    db.delete(row)
    db.commit()
    return {"ok": True, "id": photo_id}
