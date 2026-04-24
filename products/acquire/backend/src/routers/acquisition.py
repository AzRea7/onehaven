from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.auth import get_principal
from onehaven_platform.backend.src.db import get_db
from onehaven_platform.backend.src.schemas import (
    AcquisitionDeadlineUpsert,
    AcquisitionDocumentCreate,
    AcquisitionFieldOverrideIn,
    AcquisitionParticipantUpsert,
    AcquisitionRecordUpdate,
)
from products.acquire.backend.src.services.acquisition_deadline_service import (
    delete_deadline,
    list_deadlines,
    upsert_deadline_by_code,
)
from products.acquire.backend.src.services.acquisition_document_review_service import (
    accept_field_value,
    list_document_field_values,
    override_field_value,
    reject_field_value,
)
from products.acquire.backend.src.services.acquisition_participant_service import (
    delete_participant,
    list_participants,
    seed_listing_contacts_from_property,
    upsert_participant,
)
from products.acquire.backend.src.services.acquisition_service import (
    ACQUISITION_DOCUMENT_KIND_LABELS,
    ALLOWED_ACQUISITION_DOCUMENT_KINDS,
    add_acquisition_document,
    delete_acquisition_document,
    get_acquisition_detail,
    get_document_file_response,
    list_acquisition_queue,
    promote_property_to_acquisition,
    remove_property_from_acquisition,
    replace_acquisition_document,
    update_acquisition_record,
    upload_acquisition_document_file,
)

router = APIRouter(prefix="/acquisition", tags=["acquisition"])


class PromoteToAcquisitionIn(AcquisitionRecordUpdate):
    pass


@router.get("/document-kinds")
def acquisition_document_kinds():
    return {
        "items": [
            {"value": kind, "label": ACQUISITION_DOCUMENT_KIND_LABELS.get(kind, kind.replace("_", " ").title())}
            for kind in ALLOWED_ACQUISITION_DOCUMENT_KINDS
        ]
    }


@router.get("/queue")
def acquisition_queue(
    q: str | None = Query(default=None),
    status: str | None = Query(default=None),
    waiting_on: str | None = Query(default=None),
    has_overdue_deadlines: bool | None = Query(default=None),
    has_missing_required_docs: bool | None = Query(default=None),
    needs_review: bool | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return list_acquisition_queue(
        db,
        org_id=p.org_id,
        q=q,
        status=status,
        waiting_on=waiting_on,
        has_overdue_deadlines=has_overdue_deadlines,
        has_missing_required_docs=has_missing_required_docs,
        needs_review=needs_review,
        limit=limit,
        offset=offset,
    )


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


@router.post("/properties/{property_id}/promote")
def acquisition_property_promote(
    property_id: int,
    payload: PromoteToAcquisitionIn,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return promote_property_to_acquisition(
        db,
        org_id=p.org_id,
        property_id=property_id,
        actor_user_id=getattr(p, "user_id", None),
        payload=payload.model_dump(exclude_unset=True),
    )


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


@router.post("/properties/{property_id}/remove")
def acquisition_property_remove(
    property_id: int,
    payload: dict | None = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return remove_property_from_acquisition(
        db,
        org_id=p.org_id,
        property_id=property_id,
        actor_user_id=getattr(p, "user_id", None),
        payload=dict(payload or {}),
    )


@router.get("/properties/{property_id}/participants")
def acquisition_property_participants(
    property_id: int,
    include_listing_seed: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    if include_listing_seed:
        seed_listing_contacts_from_property(
            db,
            org_id=p.org_id,
            property_id=property_id,
            mark_primary=True,
        )
    return {"items": list_participants(db, org_id=p.org_id, property_id=property_id)}


@router.post("/properties/{property_id}/participants/seed-listing")
def acquisition_property_seed_listing_participants(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    items = seed_listing_contacts_from_property(
        db,
        org_id=p.org_id,
        property_id=property_id,
        mark_primary=True,
    )
    return {"ok": True, "items": items}


@router.post("/properties/{property_id}/participants")
def acquisition_property_upsert_participant(
    property_id: int,
    payload: AcquisitionParticipantUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = upsert_participant(
        db,
        org_id=p.org_id,
        property_id=property_id,
        role=payload.role,
        name=payload.name,
        email=payload.email,
        phone=payload.phone,
        company=payload.company,
        notes=payload.notes,
        source_document_id=payload.source_document_id,
        confidence=payload.confidence,
        extraction_version=payload.extraction_version,
        manually_overridden=payload.manually_overridden,
        is_primary=payload.is_primary,
        waiting_on=payload.waiting_on,
        source_type=payload.source_type,
    )
    return {"ok": True, "participant": row}


@router.delete("/properties/{property_id}/participants/{participant_id}")
def acquisition_property_delete_participant(
    property_id: int,
    participant_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = delete_participant(
        db,
        org_id=p.org_id,
        property_id=property_id,
        participant_id=participant_id,
    )
    return {"ok": True, "participant": row}


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
    replace_document_id: Annotated[int | None, Form()] = None,
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
        replace_document_id=replace_document_id,
    )
    return {"ok": True, "document": created}


@router.post("/properties/{property_id}/documents/{document_id}/replace")
def acquisition_document_replace(
    property_id: int,
    document_id: int,
    replacement_document_id: int,
    reason: str | None = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    updated = replace_acquisition_document(
        db,
        org_id=p.org_id,
        property_id=property_id,
        document_id=document_id,
        replacement_document_id=replacement_document_id,
        reason=reason,
    )
    return {"ok": True, "document": updated}


@router.delete("/properties/{property_id}/documents/{document_id}")
def acquisition_document_delete(
    property_id: int,
    document_id: int,
    reason: str | None = Query(default=None),
    hard_delete_file: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    deleted = delete_acquisition_document(
        db,
        org_id=p.org_id,
        property_id=property_id,
        document_id=document_id,
        reason=reason,
        hard_delete_file=hard_delete_file,
    )
    return {"ok": True, "document": deleted}


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


@router.get("/properties/{property_id}/field-values")
def acquisition_field_values(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return {"items": list_document_field_values(db, org_id=p.org_id, property_id=property_id)}


@router.post("/properties/{property_id}/field-values/{field_value_id}/accept")
def acquisition_accept_field_value(
    property_id: int,
    field_value_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = accept_field_value(
        db,
        org_id=p.org_id,
        property_id=property_id,
        field_value_id=field_value_id,
    )
    return {"ok": True, "field_value": row}


@router.post("/properties/{property_id}/field-values/{field_value_id}/reject")
def acquisition_reject_field_value(
    property_id: int,
    field_value_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = reject_field_value(
        db,
        org_id=p.org_id,
        property_id=property_id,
        field_value_id=field_value_id,
    )
    return {"ok": True, "field_value": row}


@router.post("/properties/{property_id}/field-values/override")
def acquisition_override_field_value(
    property_id: int,
    payload: AcquisitionFieldOverrideIn,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = override_field_value(
        db,
        org_id=p.org_id,
        property_id=property_id,
        field_name=payload.field_name,
        value=payload.value,
        source_document_id=payload.source_document_id,
        extraction_version=payload.extraction_version,
    )
    return {"ok": True, "field_value": row}


@router.get("/properties/{property_id}/deadlines")
def acquisition_property_deadlines(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return {"items": list_deadlines(db, org_id=p.org_id, property_id=property_id)}


@router.post("/properties/{property_id}/deadlines")
def acquisition_property_upsert_deadline(
    property_id: int,
    payload: AcquisitionDeadlineUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = upsert_deadline_by_code(
        db,
        org_id=p.org_id,
        property_id=property_id,
        code=payload.code,
        label=payload.label,
        due_at=payload.due_at,
        status=payload.status,
        notes=payload.notes,
    )
    return {"ok": True, "deadline": row}


@router.delete("/properties/{property_id}/deadlines/{deadline_id}")
def acquisition_property_delete_deadline(
    property_id: int,
    deadline_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = delete_deadline(
        db,
        org_id=p.org_id,
        property_id=property_id,
        deadline_id=deadline_id,
    )
    return {"ok": True, "deadline": row}