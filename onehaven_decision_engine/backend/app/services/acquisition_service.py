from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from .acquisition_deadline_service import list_deadlines
from .acquisition_document_review_service import create_field_suggestions_from_document, list_document_field_values
from .acquisition_participant_service import list_participants
from .document_parsing_service import parse_document
from .virus_scanning_service import scan_file

DEFAULT_REQUIRED_DOCS = [
    {"kind": "purchase_agreement", "label": "Purchase agreement"},
    {"kind": "loan_documents", "label": "Loan documents"},
    {"kind": "loan_estimate", "label": "Loan estimate"},
    {"kind": "closing_disclosure", "label": "Closing disclosure"},
    {"kind": "title_documents", "label": "Title / escrow"},
    {"kind": "insurance_binder", "label": "Insurance binder"},
    {"kind": "inspection_report", "label": "Inspection / due diligence"},
]

ALLOWED_UPLOAD_EXTENSIONS = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".txt": "text/plain",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
}

ALLOWED_UPLOAD_CONTENT_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/plain",
    "image/png",
    "image/jpeg",
}

MAX_UPLOAD_BYTES = 15 * 1024 * 1024


def _rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            out.append(dict(row._mapping))
        except Exception:
            out.append(dict(row))
    return out


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        return dict(row._mapping)
    except Exception:
        return dict(row)


def _safe_json_load(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return fallback
        try:
            return json.loads(s)
        except Exception:
            return fallback
    return fallback


def _days_to_close(target_close_date: str | None) -> int | None:
    if not target_close_date:
        return None
    try:
        dt = datetime.strptime(target_close_date, "%Y-%m-%d").date()
        return (dt - date.today()).days
    except Exception:
        return None


def _acquisition_upload_root() -> Path:
    raw = os.getenv("ACQUISITION_UPLOAD_DIR", "/app/data/acquisition_uploads")
    root = Path(raw).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sanitize_filename(filename: str | None) -> str:
    raw = (filename or "upload").strip()
    raw = raw.replace("\\", "/").split("/")[-1]
    raw = re.sub(r"[^A-Za-z0-9._\- ]+", "_", raw).strip()
    raw = re.sub(r"\s+", "_", raw)
    if not raw:
        raw = "upload"
    if len(raw) > 180:
        stem, ext = os.path.splitext(raw)
        raw = stem[:140] + ext[:20]
    return raw


def _validate_extension_and_content_type(filename: str, content_type: str | None) -> str:
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"File type '{ext or 'unknown'}' is not allowed.")
    if content_type:
        normalized = content_type.strip().lower()
        if normalized not in ALLOWED_UPLOAD_CONTENT_TYPES:
            raise HTTPException(status_code=400, detail=f"Content type '{normalized}' is not allowed.")
    return ext


def _sniff_magic_bytes(ext: str, file_head: bytes) -> None:
    if ext == ".pdf" and not file_head.startswith(b"%PDF-"):
        raise HTTPException(status_code=400, detail="Uploaded PDF failed signature validation.")
    if ext == ".png" and not file_head.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=400, detail="Uploaded PNG failed signature validation.")
    if ext in {".jpg", ".jpeg"} and not file_head.startswith(b"\xff\xd8\xff"):
        raise HTTPException(status_code=400, detail="Uploaded JPEG failed signature validation.")
    if ext == ".docx" and not file_head.startswith(b"PK"):
        raise HTTPException(status_code=400, detail="Uploaded DOCX failed signature validation.")


def _validate_docx_safely(path: Path) -> None:
    try:
        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
            for name in names:
                lowered = name.lower()
                if name.startswith("/") or ".." in name.replace("\\", "/").split("/"):
                    raise HTTPException(status_code=400, detail="DOCX contains invalid archive paths.")
                if lowered.endswith("vbaproject.bin"):
                    raise HTTPException(status_code=400, detail="Macro-enabled Office files are not allowed.")
            if "[Content_Types].xml" not in names:
                raise HTTPException(status_code=400, detail="DOCX structure is invalid.")
            if not any(name.startswith("word/") for name in names):
                raise HTTPException(status_code=400, detail="DOCX structure is invalid.")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="DOCX validation failed.")


def _stream_upload_to_disk(upload: UploadFile, target_path: Path) -> dict[str, Any]:
    sha256 = hashlib.sha256()
    total = 0
    first_chunk = b""

    target_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with target_path.open("wb") as out:
            while True:
                chunk = upload.file.read(1024 * 1024)
                if not chunk:
                    break
                if not first_chunk:
                    first_chunk = chunk[:64]
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=400, detail="Upload exceeds maximum size of 15 MB.")
                sha256.update(chunk)
                out.write(chunk)
    except HTTPException:
        target_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        target_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {exc}")

    return {
        "size_bytes": total,
        "sha256": sha256.hexdigest(),
        "head": first_chunk,
    }


def ensure_acquisition_record(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    existing = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_records
                where org_id = :org_id and property_id = :property_id
                limit 1
                """
            ),
            {"org_id": org_id, "property_id": property_id},
        ).fetchone()
    )
    if existing:
        return existing

    db.execute(
        text(
            """
            insert into acquisition_records (
                org_id,
                property_id,
                status,
                waiting_on,
                next_step,
                contacts_json,
                milestones_json,
                created_at,
                updated_at
            )
            values (
                :org_id,
                :property_id,
                'needs_setup',
                'Purchase agreement',
                'Import or enter acquisition data',
                :contacts_json,
                :milestones_json,
                now(),
                now()
            )
            """
        ),
        {
            "org_id": org_id,
            "property_id": property_id,
            "contacts_json": json.dumps([]),
            "milestones_json": json.dumps([]),
        },
    )
    db.commit()
    return _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_records
                where org_id = :org_id and property_id = :property_id
                limit 1
                """
            ),
            {"org_id": org_id, "property_id": property_id},
        ).fetchone()
    ) or {}


def update_acquisition_record(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    allowed_fields = {
        "status",
        "waiting_on",
        "next_step",
        "contract_date",
        "target_close_date",
        "closing_date",
        "purchase_price",
        "earnest_money",
        "loan_amount",
        "loan_type",
        "interest_rate",
        "cash_to_close",
        "closing_costs",
        "seller_credits",
        "title_company",
        "escrow_officer",
        "notes",
        "contacts_json",
        "milestones_json",
    }

    updates: list[str] = []
    params: dict[str, Any] = {"org_id": org_id, "property_id": property_id}

    for key, value in payload.items():
        if key not in allowed_fields:
            continue
        if key in {"contacts_json", "milestones_json"} and not isinstance(value, str):
            value = json.dumps(value)
        updates.append(f"{key} = :{key}")
        params[key] = value

    if not updates:
        return ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    updates.append("updated_at = now()")
    db.execute(
        text(
            f"""
            update acquisition_records
            set {", ".join(updates)}
            where org_id = :org_id and property_id = :property_id
            """
        ),
        params,
    )
    db.commit()
    return ensure_acquisition_record(db, org_id=org_id, property_id=property_id)


def add_acquisition_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    extracted_text = (payload.get("extracted_text") or "").strip()
    extracted_fields = payload.get("extracted_fields") or {}

    db.execute(
        text(
            """
            insert into acquisition_documents (
                org_id,
                property_id,
                kind,
                name,
                original_filename,
                storage_path,
                storage_url,
                content_type,
                file_size_bytes,
                sha256,
                upload_status,
                scan_status,
                scan_result,
                parse_status,
                parser_version,
                preview_text,
                status,
                source_url,
                extracted_text,
                extracted_fields_json,
                notes,
                replaced_by_document_id,
                deleted_at,
                deleted_reason,
                created_at,
                updated_at
            )
            values (
                :org_id,
                :property_id,
                :kind,
                :name,
                :original_filename,
                :storage_path,
                :storage_url,
                :content_type,
                :file_size_bytes,
                :sha256,
                :upload_status,
                :scan_status,
                :scan_result,
                :parse_status,
                :parser_version,
                :preview_text,
                :status,
                :source_url,
                :extracted_text,
                :extracted_fields_json,
                :notes,
                :replaced_by_document_id,
                :deleted_at,
                :deleted_reason,
                now(),
                now()
            )
            """
        ),
        {
            "org_id": org_id,
            "property_id": property_id,
            "kind": payload.get("kind") or "other",
            "name": payload.get("name") or "Imported document",
            "original_filename": payload.get("original_filename"),
            "storage_path": payload.get("storage_path"),
            "storage_url": payload.get("storage_url"),
            "content_type": payload.get("content_type"),
            "file_size_bytes": payload.get("file_size_bytes"),
            "sha256": payload.get("sha256"),
            "upload_status": payload.get("upload_status") or "received",
            "scan_status": payload.get("scan_status"),
            "scan_result": payload.get("scan_result"),
            "parse_status": payload.get("parse_status"),
            "parser_version": payload.get("parser_version"),
            "preview_text": payload.get("preview_text"),
            "status": payload.get("status") or "received",
            "source_url": payload.get("source_url"),
            "extracted_text": extracted_text or None,
            "extracted_fields_json": json.dumps(extracted_fields),
            "notes": payload.get("notes"),
            "replaced_by_document_id": payload.get("replaced_by_document_id"),
            "deleted_at": payload.get("deleted_at"),
            "deleted_reason": payload.get("deleted_reason"),
        },
    )
    db.commit()

    doc = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_documents
                where org_id = :org_id and property_id = :property_id
                order by id desc
                limit 1
                """
            ),
            {"org_id": org_id, "property_id": property_id},
        ).fetchone()
    ) or {}
    doc["extracted_fields"] = _safe_json_load(doc.get("extracted_fields_json"), {})
    if doc.get("id") and extracted_fields:
        create_field_suggestions_from_document(
            db,
            org_id=org_id,
            property_id=property_id,
            document_id=int(doc["id"]),
            extracted_fields=extracted_fields,
            extraction_version=payload.get("parser_version") or "v1",
        )
    return doc


def upload_acquisition_document_file(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    kind: str,
    name: str | None,
    notes: str | None,
    upload: UploadFile,
    replace_document_id: int | None = None,
) -> dict[str, Any]:
    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    original_filename = _sanitize_filename(upload.filename)
    ext = _validate_extension_and_content_type(original_filename, upload.content_type)

    base_dir = _acquisition_upload_root() / f"org_{org_id}" / f"property_{property_id}"
    base_dir.mkdir(parents=True, exist_ok=True)

    file_token = uuid.uuid4().hex
    stored_filename = f"{file_token}_{original_filename}"
    stored_path = (base_dir / stored_filename).resolve()

    if base_dir not in stored_path.parents:
        raise HTTPException(status_code=400, detail="Invalid upload path.")

    streamed = _stream_upload_to_disk(upload, stored_path)
    _sniff_magic_bytes(ext, streamed["head"])
    if ext == ".docx":
        _validate_docx_safely(stored_path)

    scan = scan_file(stored_path)
    if scan.get("infected"):
        stored_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="Upload blocked: file failed malware scan.")

    parse = parse_document(stored_path, upload.content_type)
    relative_storage_path = str(stored_path.relative_to(_acquisition_upload_root()))

    created = add_acquisition_document(
        db,
        org_id=org_id,
        property_id=property_id,
        payload={
            "kind": kind or "other",
            "name": (name or os.path.splitext(original_filename)[0] or "Uploaded document").strip(),
            "original_filename": original_filename,
            "storage_path": relative_storage_path,
            "storage_url": None,
            "content_type": (upload.content_type or ALLOWED_UPLOAD_EXTENSIONS.get(ext) or "").strip().lower() or None,
            "file_size_bytes": streamed["size_bytes"],
            "sha256": streamed["sha256"],
            "upload_status": "stored",
            "scan_status": scan.get("scan_status"),
            "scan_result": scan.get("scan_result"),
            "parse_status": parse.get("parse_status"),
            "parser_version": parse.get("parser_version"),
            "preview_text": parse.get("preview_text"),
            "status": "received",
            "source_url": None,
            "notes": (notes or "").strip() or None,
            "extracted_text": parse.get("extracted_text"),
            "extracted_fields": parse.get("extracted_fields") or {},
        },
    )

    if replace_document_id:
        replace_acquisition_document(
            db,
            org_id=org_id,
            property_id=property_id,
            document_id=int(replace_document_id),
            replacement_document_id=int(created["id"]),
            reason="replaced_by_new_upload",
        )
    return created


def replace_acquisition_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    replacement_document_id: int,
    reason: str | None = None,
) -> dict[str, Any]:
    existing = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_documents
                where id = :document_id and org_id = :org_id and property_id = :property_id
                """
            ),
            {"document_id": int(document_id), "org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Document not found.")

    db.execute(
        text(
            """
            update acquisition_documents
            set status = 'replaced',
                replaced_by_document_id = :replacement_document_id,
                deleted_reason = :reason,
                updated_at = now()
            where id = :document_id and org_id = :org_id and property_id = :property_id
            """
        ),
        {
            "document_id": int(document_id),
            "replacement_document_id": int(replacement_document_id),
            "reason": (reason or "").strip() or "replaced",
            "org_id": int(org_id),
            "property_id": int(property_id),
        },
    )
    db.commit()
    return _row_to_dict(
        db.execute(text("select * from acquisition_documents where id = :id"), {"id": int(document_id)}).fetchone()
    ) or {}


def delete_acquisition_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    reason: str | None = None,
    hard_delete_file: bool = False,
) -> dict[str, Any]:
    row = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_documents
                where id = :document_id and org_id = :org_id and property_id = :property_id
                """
            ),
            {"document_id": int(document_id), "org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found.")

    db.execute(
        text(
            """
            update acquisition_documents
            set status = 'deleted',
                deleted_at = now(),
                deleted_reason = :reason,
                updated_at = now()
            where id = :document_id and org_id = :org_id and property_id = :property_id
            """
        ),
        {
            "document_id": int(document_id),
            "org_id": int(org_id),
            "property_id": int(property_id),
            "reason": (reason or "").strip() or "deleted",
        },
    )
    db.commit()

    if hard_delete_file:
        rel = row.get("storage_path")
        if rel:
            root = _acquisition_upload_root()
            path = (root / rel).resolve()
            if root in path.parents and path.exists():
                path.unlink(missing_ok=True)

    return _row_to_dict(
        db.execute(text("select * from acquisition_documents where id = :id"), {"id": int(document_id)}).fetchone()
    ) or {}


def get_acquisition_detail(db: Session, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    property_row = _row_to_dict(
        db.execute(
            text(
                """
                select
                    p.id as property_id,
                    p.address,
                    p.city,
                    p.state,
                    p.zip,
                    p.county,
                    p.bedrooms,
                    p.bathrooms,
                    p.square_feet,
                    p.year_built,
                    p.property_type,
                    ps.current_stage
                from properties p
                left join property_states ps
                    on ps.org_id = p.org_id and ps.property_id = p.id
                where p.org_id = :org_id and p.id = :property_id
                limit 1
                """
            ),
            {"org_id": org_id, "property_id": property_id},
        ).fetchone()
    )
    if not property_row:
        return None

    acquisition_row = ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    documents = _rows_to_dicts(
        db.execute(
            text(
                """
                select
                    id,
                    property_id,
                    kind,
                    name,
                    original_filename,
                    storage_path,
                    storage_url,
                    content_type,
                    file_size_bytes,
                    sha256,
                    upload_status,
                    scan_status,
                    scan_result,
                    parse_status,
                    parser_version,
                    preview_text,
                    status,
                    source_url,
                    extracted_text,
                    extracted_fields_json,
                    notes,
                    replaced_by_document_id,
                    deleted_at,
                    deleted_reason,
                    created_at,
                    updated_at
                from acquisition_documents
                where org_id = :org_id and property_id = :property_id
                order by created_at desc, id desc
                """
            ),
            {"org_id": org_id, "property_id": property_id},
        ).fetchall()
    )

    for doc in documents:
        doc["extracted_fields"] = _safe_json_load(doc.get("extracted_fields_json"), {})

    contacts = _safe_json_load(acquisition_row.get("contacts_json"), [])
    milestones = _safe_json_load(acquisition_row.get("milestones_json"), [])
    target_close = acquisition_row.get("target_close_date") or acquisition_row.get("closing_date")
    present_doc_kinds = {
        str(d.get("kind") or "").strip()
        for d in documents
        if str(d.get("status") or "").lower() not in {"deleted", "replaced"}
    }

    required_docs = [{**doc, "present": doc["kind"] in present_doc_kinds} for doc in DEFAULT_REQUIRED_DOCS]
    deadlines = list_deadlines(db, org_id=org_id, property_id=property_id)
    participants = list_participants(db, org_id=org_id, property_id=property_id)
    field_values = list_document_field_values(db, org_id=org_id, property_id=property_id)

    has_overdue = any(bool(x.get("is_overdue")) for x in deadlines)
    suggested_count = sum(1 for x in field_values if str(x.get("review_state") or "") == "suggested")

    return {
        "property": property_row,
        "acquisition": {
            **acquisition_row,
            "contacts": contacts,
            "milestones": milestones,
            "days_to_close": _days_to_close(target_close),
        },
        "documents": documents,
        "participants": participants,
        "deadlines": deadlines,
        "field_values": field_values,
        "required_documents": required_docs,
        "summary": {
            "days_to_close": _days_to_close(target_close),
            "document_count": len(documents),
            "required_documents_total": len(required_docs),
            "required_documents_present": sum(1 for d in required_docs if d["present"]),
            "overdue_deadline_count": sum(1 for d in deadlines if d.get("is_overdue")),
            "suggested_field_count": suggested_count,
            "has_overdue_deadlines": has_overdue,
            "has_missing_required_docs": any(not d["present"] for d in required_docs),
        },
    }


def list_acquisition_queue(
    db: Session,
    *,
    org_id: int,
    q: str | None = None,
    status: str | None = None,
    waiting_on: str | None = None,
    has_overdue_deadlines: bool | None = None,
    has_missing_required_docs: bool | None = None,
    needs_review: bool | None = None,
    limit: int = 250,
    offset: int = 0,
) -> dict[str, Any]:
    q = (q or "").strip().lower()
    status = (status or "").strip().lower() or None
    waiting_on = (waiting_on or "").strip().lower() or None

    rows = db.execute(
        text(
            """
            select
                p.id as property_id,
                p.address,
                p.city,
                p.state,
                p.zip,
                p.county,
                p.bedrooms,
                p.bathrooms,
                p.square_feet,
                ps.current_stage,
                ar.status,
                ar.waiting_on,
                ar.next_step,
                ar.contract_date,
                ar.target_close_date,
                ar.closing_date,
                ar.purchase_price,
                ar.loan_amount,
                ar.cash_to_close,
                ar.closing_costs,
                ar.updated_at as acquisition_updated_at,
                (
                    select count(*)
                    from acquisition_documents ad
                    where ad.org_id = p.org_id and ad.property_id = p.id
                      and coalesce(ad.status, 'received') not in ('deleted', 'replaced')
                ) as document_count,
                (
                    select count(*)
                    from acquisition_field_values fv
                    where fv.org_id = p.org_id and fv.property_id = p.id
                      and fv.review_state = 'suggested'
                ) as suggested_field_count,
                (
                    select count(*)
                    from acquisition_deadlines dl
                    where dl.org_id = p.org_id and dl.property_id = p.id
                      and dl.status not in ('completed', 'waived')
                      and dl.due_at < now()
                ) as overdue_deadline_count
            from properties p
            left join property_states ps
                on ps.org_id = p.org_id and ps.property_id = p.id
            left join acquisition_records ar
                on ar.org_id = p.org_id and ar.property_id = p.id
            where p.org_id = :org_id
              and (
                ar.id is not null
                or lower(coalesce(ps.current_stage, '')) in ('offer', 'under_contract', 'acquisition', 'closing', 'escrow')
              )
            order by
                (
                    select min(dl2.due_at)
                    from acquisition_deadlines dl2
                    where dl2.org_id = p.org_id and dl2.property_id = p.id and dl2.status not in ('completed', 'waived')
                ) asc nulls last,
                coalesce(ar.target_close_date, ar.closing_date) asc nulls last,
                coalesce(ar.updated_at, now()) desc
            limit :limit
            offset :offset
            """
        ),
        {"org_id": org_id, "limit": limit, "offset": offset},
    ).fetchall()

    items = _rows_to_dicts(rows)
    filtered: list[dict[str, Any]] = []

    for item in items:
        haystack = " ".join(
            [
                str(item.get("address") or ""),
                str(item.get("city") or ""),
                str(item.get("state") or ""),
                str(item.get("zip") or ""),
                str(item.get("county") or ""),
                str(item.get("status") or ""),
                str(item.get("waiting_on") or ""),
                str(item.get("next_step") or ""),
            ]
        ).lower()

        if q and q not in haystack:
            continue
        if status and str(item.get("status") or "").lower() != status:
            continue
        if waiting_on and waiting_on not in str(item.get("waiting_on") or "").lower():
            continue
        if has_overdue_deadlines is True and int(item.get("overdue_deadline_count") or 0) <= 0:
            continue
        if needs_review is True and int(item.get("suggested_field_count") or 0) <= 0:
            continue

        target_close = item.get("target_close_date") or item.get("closing_date")
        item["days_to_close"] = _days_to_close(str(target_close) if target_close else None)

        active_doc_rows = _rows_to_dicts(
            db.execute(
                text(
                    """
                    select kind, status
                    from acquisition_documents
                    where org_id = :org_id and property_id = :property_id
                      and coalesce(status, 'received') not in ('deleted', 'replaced')
                    """
                ),
                {"org_id": int(org_id), "property_id": int(item["property_id"])},
            ).fetchall()
        )
        present_kinds = {str(x.get("kind") or "") for x in active_doc_rows}
        missing_required = [doc for doc in DEFAULT_REQUIRED_DOCS if doc["kind"] not in present_kinds]
        item["missing_required_document_count"] = len(missing_required)
        item["missing_required_document_kinds"] = [doc["kind"] for doc in missing_required]
        item["has_missing_required_docs"] = bool(missing_required)

        if has_missing_required_docs is True and not missing_required:
            continue

        filtered.append(item)

    return {"items": filtered, "count": len(filtered)}


def get_document_file_response(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    disposition: str = "inline",
) -> FileResponse:
    row = _row_to_dict(
        db.execute(
            text(
                """
                select
                    id,
                    org_id,
                    property_id,
                    original_filename,
                    storage_path,
                    content_type,
                    upload_status,
                    scan_status,
                    status
                from acquisition_documents
                where org_id = :org_id
                  and property_id = :property_id
                  and id = :document_id
                limit 1
                """
            ),
            {
                "org_id": org_id,
                "property_id": property_id,
                "document_id": document_id,
            },
        ).fetchone()
    )

    if not row:
        raise HTTPException(status_code=404, detail="Document not found.")
    if row.get("upload_status") != "stored":
        raise HTTPException(status_code=400, detail="Document file is not available.")
    if row.get("scan_status") not in {"clean", "skipped", "error"}:
        raise HTTPException(status_code=400, detail="Document is not cleared for access.")
    if str(row.get("status") or "").lower() == "deleted":
        raise HTTPException(status_code=400, detail="Document has been deleted.")

    rel = row.get("storage_path")
    if not rel:
        raise HTTPException(status_code=400, detail="Document storage path is missing.")

    root = _acquisition_upload_root()
    path = (root / rel).resolve()
    if root not in path.parents:
        raise HTTPException(status_code=400, detail="Invalid storage path.")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Stored file not found.")

    filename = row.get("original_filename") or path.name
    media_type = row.get("content_type") or "application/octet-stream"

    return FileResponse(
        path=str(path),
        media_type=media_type,
        filename=filename,
        content_disposition_type=disposition,
    )
