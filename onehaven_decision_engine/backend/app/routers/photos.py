from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import get_principal, require_operator
from app.db import get_db
from app.models import Property, PropertyPhoto
from app.schemas import PropertyPhotoCreate, PropertyPhotoOut
from app.products.compliance.services.compliance_document_service import create_compliance_document_from_path
from app.products.compliance.services.compliance_photo_analysis_service import (
    analyze_property_photos_for_compliance,
    create_compliance_tasks_from_photo_analysis,
)
from app.services.property_photo_service import (
    create_uploaded_photo,
    ensure_property_exists,
    list_property_photos,
    summarize_property_photo_inventory,
    upsert_zillow_photos,
)
from app.services.virus_scanning_service import scan_file
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


@router.get("/{property_id}/summary", response_model=dict)
def get_property_photo_summary(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    ensure_property_exists(db, org_id=p.org_id, property_id=property_id)
    return summarize_property_photo_inventory(db, org_id=p.org_id, property_id=property_id)


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


@router.post("/upload", response_model=dict)
async def upload_photo(
    property_id: int = Form(...),
    kind: str = Form("unknown"),
    label: str | None = Form(default=None),
    inspection_id: int | None = Form(default=None),
    checklist_item_id: int | None = Form(default=None),
    evidence_category: str | None = Form(default="photo_evidence"),
    attach_to_compliance: bool = Form(default=False),
    auto_analyze_for_compliance: bool = Form(default=False),
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

    scan = scan_file(path)
    if bool(scan.get("infected")):
        try:
            os.remove(path)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="Uploaded photo failed virus scan")

    row = create_uploaded_photo(
        db,
        org_id=p.org_id,
        property_id=int(property_id),
        url=f"/api/photos/raw/{safe_name}",
        storage_key=safe_name,
        kind=kind or classify_photo_kind(file.filename or ""),
        label=label,
        content_type=file.content_type,
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
    )

    compliance_document = None
    if attach_to_compliance:
        compliance_document = create_compliance_document_from_path(
            db,
            org_id=int(p.org_id),
            actor_user_id=int(p.user_id),
            property_id=int(property_id),
            category=str(evidence_category or "photo_evidence"),
            absolute_path=path,
            original_filename=file.filename or safe_name,
            content_type=file.content_type,
            inspection_id=int(inspection_id) if inspection_id is not None else None,
            checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
            label=label or "Photo evidence",
            notes=f"Mirrored from property photo upload (photo_id pending url={row.url})",
            parse_document=False,
            existing_storage_key=safe_name,
            public_url=f"/api/photos/raw/{safe_name}",
            scan_result=scan,
        )
        db.commit()

    compliance_preview: dict[str, Any] | None = None
    if auto_analyze_for_compliance:
        compliance_preview = analyze_property_photos_for_compliance(
            db,
            org_id=int(p.org_id),
            property_id=int(property_id),
            inspection_id=int(inspection_id) if inspection_id is not None else None,
            checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
        )

    return {
        "ok": True,
        "photo": row,
        "compliance_document": compliance_document,
        "scan": scan,
        "compliance_preview": compliance_preview,
    }


@router.post("/{property_id}/compliance-preview", response_model=dict)
def preview_compliance_findings_from_photos(
    property_id: int,
    inspection_id: int | None = Form(default=None),
    checklist_item_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    ensure_property_exists(db, org_id=p.org_id, property_id=property_id)
    return analyze_property_photos_for_compliance(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
    )


@router.post("/{property_id}/compliance-tasks", response_model=dict)
def create_compliance_tasks_from_photos(
    property_id: int,
    confirmed_codes: str | None = Form(default=None),
    inspection_id: int | None = Form(default=None),
    checklist_item_id: int | None = Form(default=None),
    mark_blocking: bool = Form(default=False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    ensure_property_exists(db, org_id=p.org_id, property_id=property_id)
    analysis = analyze_property_photos_for_compliance(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
    )

    selected_codes = None
    if confirmed_codes:
        selected_codes = [c.strip() for c in confirmed_codes.split(",") if c.strip()]

    return create_compliance_tasks_from_photo_analysis(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        analysis=analysis,
        confirmed_codes=selected_codes,
        mark_blocking=bool(mark_blocking),
    )


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
