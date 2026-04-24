from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.services.compliance_projection_service import (
    build_property_projection_snapshot,
    rebuild_property_projection,
    sync_document_evidence_for_property,
)
from onehaven_platform.backend.src.services.virus_scanning_service import scan_file


_ALLOWED_CATEGORIES = {
    "inspection_report",
    "pass_certificate",
    "reinspection_notice",
    "repair_invoice",
    "utility_confirmation",
    "smoke_detector_proof",
    "lead_based_paint_paperwork",
    "local_jurisdiction_document",
    "approval_letter",
    "denial_letter",
    "photo_evidence",
    "registration_certificate",
    "other_evidence",
}


class DocumentResult(dict):
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


def _now() -> datetime:
    return datetime.utcnow()


def _rollback_quietly(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        pass


def _storage_dir() -> Path:
    base = Path(os.getenv("COMPLIANCE_DOCUMENT_UPLOAD_DIR", "/tmp/onehaven_compliance_uploads"))
    base.mkdir(parents=True, exist_ok=True)
    return base


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, default=str)
    except Exception:
        return "{}"


def _json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _normalize_category(value: str | None) -> str:
    raw = str(value or "other_evidence").strip().lower().replace(" ", "_")
    return raw if raw in _ALLOWED_CATEGORIES else "other_evidence"


def ensure_compliance_document_table(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS compliance_documents (
                id BIGSERIAL PRIMARY KEY,
                org_id BIGINT NOT NULL,
                property_id BIGINT NOT NULL,
                inspection_id BIGINT NULL,
                checklist_item_id BIGINT NULL,
                category VARCHAR(120) NOT NULL,
                label VARCHAR(255) NULL,
                notes TEXT NULL,
                source VARCHAR(60) NOT NULL DEFAULT 'upload',
                storage_key VARCHAR(255) NULL,
                public_url TEXT NULL,
                original_filename VARCHAR(255) NULL,
                content_type VARCHAR(255) NULL,
                file_size_bytes BIGINT NULL,
                parse_status VARCHAR(60) NULL,
                extracted_text_preview TEXT NULL,
                parser_meta_json TEXT NULL,
                scan_status VARCHAR(60) NULL,
                scan_result TEXT NULL,
                metadata_json TEXT NULL,
                created_by_user_id BIGINT NULL,
                created_at TIMESTAMP NULL,
                updated_at TIMESTAMP NULL,
                deleted_at TIMESTAMP NULL
            )
            """
        )
    )
    db.flush()


def _row_to_dict(row: Any) -> DocumentResult:
    mapping = row._mapping if hasattr(row, "_mapping") else row
    item = DocumentResult(dict(mapping))
    item["parse_meta"] = _json_loads(item.get("parser_meta_json"), {})
    item["metadata"] = _json_loads(item.get("metadata_json"), {})
    item["absolute_path"] = None
    storage_key = item.get("storage_key")
    if storage_key:
        item["absolute_path"] = str(_storage_dir() / storage_key)
    return item


def list_compliance_documents(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    ensure_compliance_document_table(db)

    clauses = ["org_id = :org_id", "property_id = :property_id", "deleted_at IS NULL"]
    params: dict[str, Any] = {"org_id": int(org_id), "property_id": int(property_id)}

    if inspection_id is not None:
        clauses.append("inspection_id = :inspection_id")
        params["inspection_id"] = int(inspection_id)
    if checklist_item_id is not None:
        clauses.append("checklist_item_id = :checklist_item_id")
        params["checklist_item_id"] = int(checklist_item_id)
    if category:
        clauses.append("category = :category")
        params["category"] = _normalize_category(category)

    sql = f"""
        SELECT *
        FROM compliance_documents
        WHERE {' AND '.join(clauses)}
        ORDER BY id DESC
    """
    rows = db.execute(text(sql), params).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_compliance_document(
    db: Session,
    *,
    org_id: int,
    document_id: int,
) -> dict[str, Any]:
    ensure_compliance_document_table(db)
    row = db.execute(
        text(
            """
            SELECT *
            FROM compliance_documents
            WHERE id = :document_id
              AND org_id = :org_id
              AND deleted_at IS NULL
            """
        ),
        {"document_id": int(document_id), "org_id": int(org_id)},
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Compliance document not found")
    return _row_to_dict(row)


def _extract_text_preview(path: Path, content_type: str | None) -> tuple[str, str, dict[str, Any]]:
    suffix = path.suffix.lower()
    parser_meta: dict[str, Any] = {"suffix": suffix, "content_type": content_type}
    try:
        if suffix in {".txt", ".md", ".csv", ".json"} or (content_type or "").startswith("text/"):
            content = path.read_text(encoding="utf-8", errors="replace")
            return "parsed", content[:4000], parser_meta
        if suffix in {".pdf", ".doc", ".docx"}:
            parser_meta["note"] = "placeholder_text_extraction"
            return "queued", "", parser_meta
        return "skipped", "", parser_meta
    except Exception as exc:
        parser_meta["error"] = str(exc)
        return "error", "", parser_meta


async def create_compliance_document_from_upload(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    property_id: int,
    category: str,
    upload: UploadFile,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    label: str | None = None,
    notes: str | None = None,
    parse_document: bool = True,
) -> dict[str, Any]:
    storage_dir = _storage_dir()
    timestamp = int(_now().timestamp())
    safe_name = f"org{int(org_id)}_p{int(property_id)}_{timestamp}_{upload.filename or 'upload.bin'}"
    path = storage_dir / safe_name

    with path.open("wb") as out:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)

    scan = scan_file(path)

    return create_compliance_document_from_path(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        property_id=property_id,
        category=category,
        absolute_path=path,
        original_filename=upload.filename or safe_name,
        content_type=upload.content_type,
        inspection_id=inspection_id,
        checklist_item_id=checklist_item_id,
        label=label,
        notes=notes,
        parse_document=parse_document,
        existing_storage_key=safe_name,
        public_url=None,
        scan_result=scan,
    )


def create_compliance_document_from_path(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    property_id: int,
    category: str,
    absolute_path: str | Path,
    original_filename: str,
    content_type: str | None,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    label: str | None = None,
    notes: str | None = None,
    parse_document: bool = True,
    existing_storage_key: str | None = None,
    public_url: str | None = None,
    scan_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_compliance_document_table(db)

    path = Path(absolute_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Uploaded file missing on disk")

    safe_category = _normalize_category(category)
    scan = scan_result or scan_file(path)
    if bool(scan.get("infected")):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="Uploaded document failed virus scan")

    parse_status = "skipped"
    extracted_text_preview = ""
    parser_meta: dict[str, Any] = {}
    if parse_document:
        parse_status, extracted_text_preview, parser_meta = _extract_text_preview(path, content_type)

    if existing_storage_key:
        storage_key = existing_storage_key
        stored_path = path
    else:
        storage_key = f"{int(_now().timestamp())}_{original_filename}"
        stored_path = _storage_dir() / storage_key
        if stored_path != path:
            shutil.copyfile(path, stored_path)

    params = {
        "org_id": int(org_id),
        "property_id": int(property_id),
        "inspection_id": int(inspection_id) if inspection_id is not None else None,
        "checklist_item_id": int(checklist_item_id) if checklist_item_id is not None else None,
        "category": safe_category,
        "label": label,
        "notes": notes,
        "source": "upload",
        "storage_key": storage_key,
        "public_url": public_url,
        "original_filename": original_filename,
        "content_type": content_type,
        "file_size_bytes": int(stored_path.stat().st_size) if stored_path.exists() else None,
        "parse_status": parse_status,
        "extracted_text_preview": extracted_text_preview,
        "parser_meta_json": _json_dumps(parser_meta),
        "scan_status": scan.get("scan_status"),
        "scan_result": scan.get("scan_result"),
        "metadata_json": _json_dumps(
            {
                "category": safe_category,
                "uploaded_for": "compliance_execution",
                "inspection_id": inspection_id,
                "checklist_item_id": checklist_item_id,
            }
        ),
        "created_by_user_id": int(actor_user_id) if actor_user_id is not None else None,
        "created_at": _now(),
        "updated_at": _now(),
    }

    row = db.execute(
        text(
            """
            INSERT INTO compliance_documents (
                org_id,
                property_id,
                inspection_id,
                checklist_item_id,
                category,
                label,
                notes,
                source,
                storage_key,
                public_url,
                original_filename,
                content_type,
                file_size_bytes,
                parse_status,
                extracted_text_preview,
                parser_meta_json,
                scan_status,
                scan_result,
                metadata_json,
                created_by_user_id,
                created_at,
                updated_at
            ) VALUES (
                :org_id,
                :property_id,
                :inspection_id,
                :checklist_item_id,
                :category,
                :label,
                :notes,
                :source,
                :storage_key,
                :public_url,
                :original_filename,
                :content_type,
                :file_size_bytes,
                :parse_status,
                :extracted_text_preview,
                :parser_meta_json,
                :scan_status,
                :scan_result,
                :metadata_json,
                :created_by_user_id,
                :created_at,
                :updated_at
            )
            RETURNING *
            """
        ),
        params,
    ).fetchone()
    db.flush()

    created = _row_to_dict(row)

    # Commit the document insert first so later evidence/projection failures do not
    # make the upload appear successful while rolling the document back.
    db.commit()

    try:
        sync_document_evidence_for_property(
            db,
            org_id=org_id,
            property_id=property_id,
            document_id=int(created["id"]),
        )
        db.commit()
    except Exception:
        _rollback_quietly(db)
        created["evidence_sync_error"] = True

    try:
        rebuild_property_projection(
            db,
            org_id=org_id,
            property_id=property_id,
        )
        db.commit()
    except Exception:
        _rollback_quietly(db)
        created["projection_rebuild_error"] = True

    persisted = get_compliance_document(db, org_id=org_id, document_id=int(created["id"]))
    if isinstance(persisted, dict):
        persisted.update(
            {
                "evidence_sync_error": created.get("evidence_sync_error", False),
                "projection_rebuild_error": created.get("projection_rebuild_error", False),
            }
        )
    return persisted


def delete_compliance_document(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    document_id: int,
) -> dict[str, Any]:
    row = get_compliance_document(db, org_id=org_id, document_id=document_id)
    db.execute(
        text(
            """
            UPDATE compliance_documents
            SET deleted_at = :deleted_at,
                updated_at = :updated_at
            WHERE id = :document_id
              AND org_id = :org_id
              AND deleted_at IS NULL
            """
        ),
        {
            "deleted_at": _now(),
            "updated_at": _now(),
            "document_id": int(document_id),
            "org_id": int(org_id),
        },
    )
    path = row.get("absolute_path")
    if path:
        try:
            Path(path).unlink(missing_ok=True)
        except Exception:
            pass
    db.commit()
    try:
        rebuild_property_projection(
            db,
            org_id=org_id,
            property_id=int(row["property_id"]),
        )
        db.commit()
    except Exception:
        _rollback_quietly(db)
    return {"document_id": int(document_id), "deleted": True}


def build_property_document_stack(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    rows = list_compliance_documents(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    by_category: dict[str, list[dict[str, Any]]] = {}
    by_inspection: dict[str, list[dict[str, Any]]] = {}
    by_checklist_item: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        category = str(row.get("category") or "other_evidence")
        by_category.setdefault(category, []).append(row)

        inspection_key = str(row.get("inspection_id")) if row.get("inspection_id") is not None else "unassigned"
        by_inspection.setdefault(inspection_key, []).append(row)

        checklist_key = str(row.get("checklist_item_id")) if row.get("checklist_item_id") is not None else "unassigned"
        by_checklist_item.setdefault(checklist_key, []).append(row)

    try:
        projection = build_property_projection_snapshot(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    except Exception:
        _rollback_quietly(db)
        projection = {"projection": None, "items": [], "blockers": [], "proof_counts": {}, "proof_obligations": []}

    proof_summary: dict[str, list[dict[str, Any]]] = {}
    for item in projection.get("items") or []:
        rule_key = str(item.get("rule_key") or "")
        if not rule_key:
            continue
        proof_summary.setdefault(rule_key, []).append(
            {
                "evaluation_status": item.get("evaluation_status"),
                "evidence_status": item.get("evidence_status"),
                "evidence_summary": item.get("evidence_summary"),
                "evidence_gap": item.get("evidence_gap"),
            }
        )

    return {
        "ok": True,
        "property_id": int(property_id),
        "count": len(rows),
        "rows": rows,
        "by_category": by_category,
        "by_inspection": by_inspection,
        "by_checklist_item": by_checklist_item,
        "proof_summary": proof_summary,
        "proof_obligations": projection.get("proof_obligations") or ((projection.get("projection") or {}).get("proof_obligations") if isinstance(projection.get("projection"), dict) else []),
        "proof_counts": projection.get("proof_counts") or ((projection.get("projection") or {}).get("proof_counts") if isinstance(projection.get("projection"), dict) else {}),
        "projection": projection.get("projection"),
        "projection_items": projection.get("items") or [],
        "blockers": projection.get("blockers") or [],
    }
