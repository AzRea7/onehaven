from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..schemas.acquisition import AcquisitionDocumentCreate, AcquisitionRecordUpdate
from ..services.acquisition_service import (
    add_acquisition_document,
    get_acquisition_detail,
    get_document_file_response,
    list_acquisition_queue,
    update_acquisition_record,
    upload_acquisition_document_file,
)

router = APIRouter(prefix="/acquisition", tags=["acquisition"])


@router.get("/queue")
def acquisition_queue(
    q: str | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return list_acquisition_queue(db, org_id=p.org_id, q=q, limit=limit, offset=offset)


@router.get("/properties/{property_id}")
def acquisition_property_detail(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    payload = get_acquisition_detail(db, org_id=p.org_id, property_id=property_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Property not found.")
    return payload


@router.put("/properties/{property_id}")
def acquisition_property_update(
    property_id: int,
    payload: AcquisitionRecordUpdate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    updated = update_acquisition_record(
        db,
        org_id=p.org_id,
        property_id=property_id,
        payload=payload.model_dump(exclude_unset=True),
    )
    return {"ok": True, "acquisition": updated}


@router.post("/properties/{property_id}/documents")
def acquisition_property_add_document(
    property_id: int,
    payload: AcquisitionDocumentCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    created = add_acquisition_document(
        db,
        org_id=p.org_id,
        property_id=property_id,
        payload=payload.model_dump(exclude_unset=True),
    )
    return {"ok": True, "document": created}


@router.post("/properties/{property_id}/documents/upload")
def acquisition_property_upload_document(
    property_id: int,
    kind: Annotated[str, Form(...)],
    file: Annotated[UploadFile, File(...)],
    name: Annotated[str | None, Form()] = None,
    notes: Annotated[str | None, Form()] = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    created = upload_acquisition_document_file(
        db,
        org_id=p.org_id,
        property_id=property_id,
        kind=kind,
        name=name,
        notes=notes,
        upload=file,
    )
    return {"ok": True, "document": created}


@router.get("/properties/{property_id}/documents/{document_id}/preview")
def acquisition_document_preview(
    property_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return get_document_file_response(
        db,
        org_id=p.org_id,
        property_id=property_id,
        document_id=document_id,
        disposition="inline",
    )


@router.get("/properties/{property_id}/documents/{document_id}/download")
def acquisition_document_download(
    property_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return get_document_file_response(
        db,
        org_id=p.org_id,
        property_id=property_id,
        document_id=document_id,
        disposition="attachment",
    )