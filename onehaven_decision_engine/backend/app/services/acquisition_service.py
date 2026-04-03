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
from .acquisition_document_review_service import (
    create_field_suggestions_from_document,
    list_document_field_values,
)
from .acquisition_participant_service import (
    build_document_contact_directory,
    list_participants,
    seed_listing_contacts_from_property,
)
from .document_parsing_service import parse_document
from .virus_scanning_service import scan_file
from .acquisition_tag_service import (
    DEFAULT_INVESTOR_PRESERVE_TAGS,
    list_property_tags,
    normalize_preserve_tags,
    replace_property_tags,
)
from .property_state_machine import get_state_payload, sync_property_state

DEFAULT_REQUIRED_DOCS = [
    {"kind": "purchase_agreement", "label": "Purchase agreement"},
    {"kind": "loan_documents", "label": "Loan documents"},
    {"kind": "loan_estimate", "label": "Loan estimate"},
    {"kind": "closing_disclosure", "label": "Closing disclosure"},
    {"kind": "title_documents", "label": "Title / escrow"},
    {"kind": "insurance_binder", "label": "Insurance binder"},
    {"kind": "inspection_report", "label": "Inspection / due diligence"},
]


ALLOWED_ACQUISITION_DOCUMENT_KINDS: tuple[str, ...] = (
    "purchase_agreement",
    "loan_estimate",
    "loan_documents",
    "closing_disclosure",
    "title_documents",
    "insurance_binder",
    "inspection_report",
)

ACQUISITION_DOCUMENT_KIND_LABELS: dict[str, str] = {
    "purchase_agreement": "Purchase agreement",
    "loan_estimate": "Loan estimate",
    "loan_documents": "Loan documents",
    "closing_disclosure": "Closing disclosure",
    "title_documents": "Title documents",
    "insurance_binder": "Insurance binder",
    "inspection_report": "Inspection report",
}

PROMOTION_REQUIRED_FIELDS: tuple[str, ...] = ()

DEFAULT_ACQUISITION_STATUS = "pursuing"
DEFAULT_ACQUISITION_WAITING_ON = "Operator review / pre-offer pursuit"
DEFAULT_ACQUISITION_NEXT_STEP = "Start pre-offer acquisition work"

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

HIGHLIGHT_PATTERNS: list[tuple[str, str]] = [
    ("seller_credit", r"\bseller\s+credit[s]?\b"),
    ("commission_split", r"\b(?:buyer(?:'s)?\s+agent|selling\s+agent|co-?op)\b.*?\b(?:0\.5%|0\.50%|1%|1\.0%)\b"),
    ("commission_reduction", r"\bcommission\b.*?\breduc"),
    ("as_is", r"\bas[- ]is\b"),
    ("inspection_deadline", r"\binspection\b.*?\b(?:deadline|within \d+ days|contingency)\b"),
    ("financing_contingency", r"\bfinancing contingency\b"),
    ("appraisal_gap", r"\bappraisal gap\b"),
    ("earnest_money", r"\bearnest money\b"),
    ("occupancy", r"\boccupancy\b"),
    ("closing_date", r"\bclosing\b.*?\bdate\b"),
    ("title_issue", r"\btitle\b.*?\b(issue|objection|commitment)\b"),
]

MAX_UPLOAD_BYTES = 15 * 1024 * 1024


def extract_document_highlights(text: str | None) -> list[dict[str, Any]]:
    body = str(text or "").strip()
    if not body:
        return []

    normalized = re.sub(r"\s+", " ", body)
    highlights: list[dict[str, Any]] = []

    for code, pattern in HIGHLIGHT_PATTERNS:
        for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
            start = max(0, match.start() - 90)
            end = min(len(normalized), match.end() + 140)
            excerpt = normalized[start:end].strip()
            if excerpt:
                highlights.append({"code": code, "excerpt": excerpt})

    deduped: list[dict[str, Any]] = []
    seen = set()
    for item in highlights:
        key = (item["code"], item["excerpt"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped[:20]



def _safe_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _safe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None

def _col_exists(db: Session, table_name: str, column_name: str) -> bool:
    try:
        result = db.execute(
            text(
                """
                select 1
                from information_schema.columns
                where table_name = :table_name
                  and column_name = :column_name
                limit 1
                """
            ),
            {
                "table_name": table_name,
                "column_name": column_name,
            },
        ).fetchone()
        return result is not None
    except Exception:
        return False

def _doc_json_payload(doc: dict[str, Any]) -> dict[str, Any]:
    raw = _safe_json_load(doc.get("extracted_fields_json"), {})
    if not isinstance(raw, dict):
        raw = {}
    return raw


def _build_document_actionable_payload(doc: dict[str, Any], acquisition_row: dict[str, Any]) -> dict[str, Any]:
    payload = _doc_json_payload(doc)
    facts = payload.get("facts") if isinstance(payload.get("facts"), dict) else {}
    if not facts and isinstance(doc.get("extracted_fields"), dict):
        facts = {k: {"field_name": k, "value": v, "confidence": 0.75, "excerpt": None} for k, v in doc.get("extracted_fields", {}).items()}

    canonical_pairs = {
        "purchase_price": acquisition_row.get("purchase_price"),
        "earnest_money": acquisition_row.get("earnest_money"),
        "loan_amount": acquisition_row.get("loan_amount"),
        "loan_type": acquisition_row.get("loan_type"),
        "cash_to_close": acquisition_row.get("cash_to_close"),
        "closing_costs": acquisition_row.get("closing_costs"),
        "seller_credits": acquisition_row.get("seller_credits"),
        "title_company": acquisition_row.get("title_company"),
        "escrow_officer": acquisition_row.get("escrow_officer"),
        "target_close_date": acquisition_row.get("target_close_date") or acquisition_row.get("closing_date"),
    }

    mismatch_indicators: list[dict[str, Any]] = []
    for field_name, current_value in canonical_pairs.items():
        fact = facts.get(field_name)
        if not fact:
            continue
        parsed_value = fact.get("value")
        if parsed_value in (None, "", []):
            continue
        if current_value in (None, "", []):
            continue
        if str(parsed_value).strip().lower() != str(current_value).strip().lower():
            mismatch_indicators.append(
                {
                    "field_name": field_name,
                    "parsed_value": parsed_value,
                    "current_value": current_value,
                    "excerpt": fact.get("excerpt"),
                    "confidence": fact.get("confidence"),
                }
            )

    return {
        "normalized_document_type": payload.get("normalized_document_type") or doc.get("kind"),
        "facts": facts,
        "recommended_next_actions": payload.get("recommended_next_actions") or [],
        "who_to_contact_next": payload.get("who_to_contact_next") or [],
        "deadline_candidates": payload.get("deadline_candidates") or [],
        "risk_flags": payload.get("risk_flags") or [],
        "mismatch_indicators": mismatch_indicators,
        "warnings": payload.get("warnings") or [x.get("label") for x in (payload.get("risk_flags") or []) if x.get("label")],
    }


def _rollup_actionable_summary(documents: list[dict[str, Any]]) -> dict[str, Any]:
    actions: list[str] = []
    contacts: list[dict[str, Any]] = []
    deadlines: list[dict[str, Any]] = []
    flags: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for doc in documents:
        actionable = doc.get("actionable_intelligence") or {}
        for item in actionable.get("recommended_next_actions") or []:
            if item and item not in actions:
                actions.append(str(item))
        for item in actionable.get("who_to_contact_next") or []:
            if item and item not in contacts:
                contacts.append(item)
        for item in actionable.get("deadline_candidates") or []:
            if item and item not in deadlines:
                deadlines.append(item)
        for item in actionable.get("risk_flags") or []:
            if item and item not in flags:
                flags.append(item)
        for item in actionable.get("mismatch_indicators") or []:
            if item and item not in mismatches:
                mismatches.append(item)
    return {
        "recommended_next_actions": actions[:20],
        "who_to_contact_next": contacts[:20],
        "deadline_candidates": deadlines[:20],
        "risk_flags": flags[:20],
        "mismatch_indicators": mismatches[:20],
    }


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


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None



def _normalize_document_kind(kind: Any) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized not in ALLOWED_ACQUISITION_DOCUMENT_KINDS:
        allowed = ", ".join(ALLOWED_ACQUISITION_DOCUMENT_KINDS)
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported acquisition document kind '{normalized or 'unknown'}'. Allowed kinds: {allowed}.",
        )
    return normalized


def _get_document_by_id(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
) -> dict[str, Any] | None:
    return _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_documents
                where id = :document_id
                  and org_id = :org_id
                  and property_id = :property_id
                limit 1
                """
            ),
            {
                "document_id": int(document_id),
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
        ).fetchone()
    )


def _get_duplicate_document_by_sha256(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    sha256: str,
) -> dict[str, Any] | None:
    return _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_documents
                where org_id = :org_id
                  and property_id = :property_id
                  and sha256 = :sha256
                  and coalesce(status, 'received') not in ('deleted', 'replaced')
                order by id desc
                limit 1
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "sha256": str(sha256),
            },
        ).fetchone()
    )

def _acquisition_upload_root() -> Path:
    raw = os.getenv("ACQUISITION_UPLOAD_DIR", "/app/data/acquisition_uploads")
    root = Path(raw).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sanitize_filename(filename: str | None) -> str:
    raw = (filename or "upload").strip()
    raw = raw.replace("\\", "/").split("/")[-1]
    raw = re.sub(r"[^A-Za-z0-9._ -]+", "_", raw).strip()
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

    return {"size_bytes": total, "sha256": sha256.hexdigest(), "head": first_chunk}


def _default_waiting_on(value: str | None) -> str:
    return value or DEFAULT_ACQUISITION_WAITING_ON


def _default_next_step(value: str | None) -> str:
    return value or DEFAULT_ACQUISITION_NEXT_STEP


def _looks_like_legacy_setup_row(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    waiting_on = str(row.get("waiting_on") or "").strip().lower()
    next_step = str(row.get("next_step") or "").strip().lower()
    return (
        status == "needs_setup"
        and waiting_on == "purchase agreement"
        and next_step == "import or enter acquisition data"
    )


def _normalize_legacy_acquisition_record(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    row: dict[str, Any],
) -> dict[str, Any]:
    if not _looks_like_legacy_setup_row(row):
        return row

    db.execute(
        text(
            """
            update acquisition_records
            set
                status = :status,
                waiting_on = :waiting_on,
                next_step = :next_step,
                updated_at = now()
            where org_id = :org_id and property_id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "status": DEFAULT_ACQUISITION_STATUS,
            "waiting_on": DEFAULT_ACQUISITION_WAITING_ON,
            "next_step": DEFAULT_ACQUISITION_NEXT_STEP,
        },
    )
    db.commit()

    refreshed = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_records
                where org_id = :org_id and property_id = :property_id
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    return refreshed or row


def _get_acquisition_record(db: Session, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    row = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_records
                where org_id = :org_id and property_id = :property_id
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not row:
        return None
    return _normalize_legacy_acquisition_record(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
        row=row,
    )


def ensure_acquisition_record(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    existing = _get_acquisition_record(db, org_id=org_id, property_id=property_id)
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
                :status,
                :waiting_on,
                :next_step,
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
            "status": DEFAULT_ACQUISITION_STATUS,
            "waiting_on": DEFAULT_ACQUISITION_WAITING_ON,
            "next_step": DEFAULT_ACQUISITION_NEXT_STEP,
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


def _merge_contacts_json_with_participants(
    *,
    existing_contacts: list[dict[str, Any]],
    participants: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    seen_keys: set[tuple[str, str]] = set()
    for row in existing_contacts:
        role = str(row.get("role") or "").strip().lower()
        name = str(row.get("name") or "").strip().lower()
        seen_keys.add((role, name))
        out.append(row)

    for row in participants:
        role = str(row.get("role") or "").strip().lower()
        name = str(row.get("name") or "").strip().lower()
        if not role or not name:
            continue
        if (role, name) in seen_keys:
            continue
        out.append(
            {
                "role": row.get("role"),
                "name": row.get("name"),
                "email": row.get("email"),
                "phone": row.get("phone"),
                "company": row.get("company"),
                "is_primary": bool(row.get("is_primary") or False),
                "waiting_on": bool(row.get("waiting_on") or False),
                "source_type": row.get("source_type"),
            }
        )
        seen_keys.add((role, name))

    return out


def update_acquisition_record(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    if payload.get("contacts_json") is None:
        existing = ensure_acquisition_record(db, org_id=org_id, property_id=property_id)
        current_contacts = _safe_json_load(existing.get("contacts_json"), [])
        participants = list_participants(db, org_id=org_id, property_id=property_id)
        payload = {
            **payload,
            "contacts_json": _merge_contacts_json_with_participants(
                existing_contacts=current_contacts if isinstance(current_contacts, list) else [],
                participants=participants,
            ),
        }

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


def _as_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid numeric value: {value}") from exc


def _validate_promotion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = {
        "status": _clean_text(payload.get("status")) or DEFAULT_ACQUISITION_STATUS,
        "waiting_on": _default_waiting_on(_clean_text(payload.get("waiting_on"))),
        "next_step": _default_next_step(_clean_text(payload.get("next_step"))),
        "contract_date": _clean_text(payload.get("contract_date")),
        "target_close_date": _clean_text(payload.get("target_close_date")),
        "closing_date": _clean_text(payload.get("closing_date")),
        "purchase_price": _as_float_or_none(payload.get("purchase_price")),
        "earnest_money": _as_float_or_none(payload.get("earnest_money")),
        "loan_amount": _as_float_or_none(payload.get("loan_amount")),
        "loan_type": _clean_text(payload.get("loan_type")) or "dscr",
        "interest_rate": _as_float_or_none(payload.get("interest_rate")),
        "cash_to_close": _as_float_or_none(payload.get("cash_to_close")),
        "closing_costs": _as_float_or_none(payload.get("closing_costs")),
        "seller_credits": _as_float_or_none(payload.get("seller_credits")),
        "title_company": _clean_text(payload.get("title_company")),
        "escrow_officer": _clean_text(payload.get("escrow_officer")),
        "notes": _clean_text(payload.get("notes")),
    }

    purchase_price = cleaned.get("purchase_price")
    if purchase_price is not None and purchase_price <= 0:
        raise HTTPException(status_code=422, detail="purchase_price must be greater than 0.")

    loan_type = str(cleaned.get("loan_type") or "").strip().lower()
    allowed_loan_types = {
        "cash",
        "dscr",
        "conventional",
        "hard_money",
        "private_money",
        "seller_finance",
    }
    if loan_type and loan_type not in allowed_loan_types:
        raise HTTPException(status_code=422, detail="loan_type is invalid.")

    loan_amount = cleaned.get("loan_amount")
    if loan_amount is not None and loan_amount <= 0:
        raise HTTPException(
            status_code=422,
            detail="loan_amount must be greater than 0 when provided.",
        )

    return cleaned


def _property_listing_contact_seed(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> list[dict[str, Any]]:
    try:
        return seed_listing_contacts_from_property(
            db,
            org_id=org_id,
            property_id=property_id,
            mark_primary=True,
        )
    except HTTPException:
        raise
    except Exception:
        return list_participants(db, org_id=org_id, property_id=property_id)


def promote_property_to_acquisition(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    actor_user_id: int | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    prop = _row_to_dict(
        db.execute(
            text(
                """
                select
                    id,
                    org_id,
                    address,
                    city,
                    state,
                    zip,
                    county,
                    listing_price as asking_price,
                    listing_price,
                    listing_agent_name,
                    listing_agent_phone,
                    listing_agent_email,
                    listing_agent_website,
                    listing_office_name,
                    listing_office_phone,
                    listing_office_email
                from properties
                where org_id = :org_id and id = :property_id
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found.")

    state = get_state_payload(db, org_id=int(org_id), property_id=int(property_id), recompute=True)
    decision_bucket = str(
        state.get("decision_bucket") or state.get("normalized_decision") or prop.get("normalized_decision") or "REVIEW"
    ).upper()
    if decision_bucket == "REJECT":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "rejected_property_cannot_enter_acquisition",
                "message": "Rejected properties cannot be moved into acquisition until assumptions change.",
                "decision_bucket": decision_bucket,
                "next_actions": state.get("next_actions") or [],
            },
        )

    cleaned = _validate_promotion_payload(payload)

    _property_listing_contact_seed(db, org_id=org_id, property_id=property_id)

    acquisition = update_acquisition_record(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
        payload=cleaned,
    )

    existing_tags = [
        row.get("tag")
        for row in list_property_tags(db, org_id=int(org_id), property_id=int(property_id))
        if row.get("tag")
    ]
    merged_tags = sorted(set(existing_tags) | {"offer_candidate"})

    replace_property_tags(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
        tags=merged_tags,
        actor_user_id=actor_user_id,
        source="operator",
    )

    sync_property_state(db, org_id=int(org_id), property_id=int(property_id))
    db.commit()

    return {
        "ok": True,
        "property_id": int(property_id),
        "property": prop,
        "acquisition": acquisition,
        "detail": get_acquisition_detail(db, org_id=int(org_id), property_id=int(property_id)),
        "state": get_state_payload(db, org_id=int(org_id), property_id=int(property_id), recompute=True),
        "tags": merged_tags,
    }


def remove_property_from_acquisition(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    actor_user_id: int | None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = payload or {}
    delete_documents = bool(payload.get("delete_documents", True))
    delete_deadlines = bool(payload.get("delete_deadlines", True))
    delete_field_reviews = bool(payload.get("delete_field_reviews", True))
    delete_contacts = bool(payload.get("delete_contacts", True))
    hard_delete_files = bool(payload.get("hard_delete_files", True))
    preserve_tags = normalize_preserve_tags(
        payload.get("preserve_tags"),
        default_to_investor_tags=True,
    )

    prop = _row_to_dict(
        db.execute(
            text(
                """
                select id, org_id, address, city, state, zip
                from properties
                where org_id = :org_id and id = :property_id
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found.")

    existing_tags = [
        str(row.get("tag") or "").strip().lower()
        for row in list_property_tags(db, org_id=int(org_id), property_id=int(property_id))
        if row.get("tag")
    ]
    final_tags = [tag for tag in preserve_tags if tag in existing_tags]

    active_documents = _rows_to_dicts(
        db.execute(
            text(
                """
                select id, storage_path
                from acquisition_documents
                where org_id = :org_id
                  and property_id = :property_id
                  and coalesce(status, 'received') not in ('deleted', 'replaced')
                order by id asc
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchall()
    )

    if delete_documents:
        db.execute(
            text(
                """
                update acquisition_documents
                set status = 'deleted',
                    storage_url = null,
                    deleted_at = now(),
                    deleted_reason = 'removed_from_acquire',
                    updated_at = now()
                where org_id = :org_id
                  and property_id = :property_id
                  and coalesce(status, 'received') not in ('deleted', 'replaced')
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        )

    if delete_field_reviews:
        db.execute(
            text(
                """
                delete from acquisition_field_values
                where org_id = :org_id and property_id = :property_id
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        )

    if delete_deadlines:
        db.execute(
            text(
                """
                delete from acquisition_deadlines
                where org_id = :org_id and property_id = :property_id
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        )

    if delete_contacts and _col_exists(db, "acquisition_contacts", "property_id"):
        db.execute(
            text(
                """
                delete from acquisition_contacts
                where org_id = :org_id and property_id = :property_id
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        )

    db.execute(
        text(
            """
            delete from acquisition_records
            where org_id = :org_id and property_id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    )

    replace_property_tags(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
        tags=final_tags,
        actor_user_id=actor_user_id,
        source="operator",
    )

    sync_property_state(db, org_id=int(org_id), property_id=int(property_id))
    db.commit()

    if delete_documents and hard_delete_files:
        for row in active_documents:
            storage_path = _clean_text(row.get("storage_path"))
            if storage_path:
                try:
                    Path(storage_path).unlink(missing_ok=True)
                except Exception:
                    pass

    state_payload = get_state_payload(db, org_id=int(org_id), property_id=int(property_id), recompute=True)
    return {
        "ok": True,
        "property_id": int(property_id),
        "property": prop,
        "deleted_documents": len(active_documents) if delete_documents else 0,
        "deleted_field_reviews": bool(delete_field_reviews),
        "deleted_deadlines": bool(delete_deadlines),
        "deleted_contacts": bool(delete_contacts),
        "preserved_tags": final_tags,
        "state": state_payload,
        "detail": get_acquisition_detail(db, org_id=int(org_id), property_id=int(property_id)),
    }


def get_acquisition_detail(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any] | None:
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
                    ps.current_stage,

                    p.listing_status,
                    p.listing_days_on_market,
                    p.listing_listed_at,
                    p.listing_last_seen_at,
                    p.listing_removed_at,
                    p.listing_zillow_url,

                    p.listing_agent_name,
                    p.listing_agent_phone,
                    p.listing_agent_email,
                    p.listing_agent_website,

                    p.listing_office_name,
                    p.listing_office_phone,
                    p.listing_office_email
                from properties p
                left join property_states ps
                  on ps.org_id = p.org_id and ps.property_id = p.id
                where p.org_id = :org_id and p.id = :property_id
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not property_row:
        return None

    acquisition_row = _get_acquisition_record(db, org_id=org_id, property_id=property_id) or {}
    if acquisition_row:
        _property_listing_contact_seed(db, org_id=org_id, property_id=property_id)

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
                where org_id = :org_id
                  and property_id = :property_id
                  and coalesce(status, 'received') not in ('deleted', 'replaced')
                  and deleted_at is null
                order by created_at desc, id desc
                """
            ),
            {"org_id": org_id, "property_id": property_id},
        ).fetchall()
    )

    for doc in documents:
        payload = _doc_json_payload(doc)
        extracted_fields = payload.get("extracted_fields") if isinstance(payload.get("extracted_fields"), dict) else payload.get("fields") if isinstance(payload.get("fields"), dict) else payload if isinstance(payload, dict) else {}
        doc["parser_payload"] = payload
        doc["extracted_fields"] = extracted_fields if isinstance(extracted_fields, dict) else {}
        doc["kind_label"] = ACQUISITION_DOCUMENT_KIND_LABELS.get(str(doc.get("kind") or "").strip().lower(), str(doc.get("kind") or "").replace("_", " ").title())
        source_text = doc.get("extracted_text") or doc.get("preview_text") or ""
        doc["highlights"] = extract_document_highlights(source_text)
        doc["actionable_intelligence"] = _build_document_actionable_payload(doc, acquisition_row)

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

    merged_contacts = _merge_contacts_json_with_participants(
        existing_contacts=contacts if isinstance(contacts, list) else [],
        participants=participants,
    )

    queue_waiting = str(acquisition_row.get("waiting_on") or "").strip().lower()
    waiting_on_roles = {
        "lender": {"loan_officer", "lender"},
        "title": {"title_company", "escrow", "escrow_officer"},
        "seller": {"seller"},
        "operator": {"operator", "internal_team", "analyst"},
        "document": {"document_coordinator", "closing_coordinator"},
    }

    normalized_participants: list[dict[str, Any]] = []
    for row in participants:
        role = str(row.get("role") or "").strip().lower()
        waiting = bool(row.get("waiting_on") or False)

        if "lender" in queue_waiting and role in waiting_on_roles["lender"]:
            waiting = True
        elif "title" in queue_waiting and role in waiting_on_roles["title"]:
            waiting = True
        elif "seller" in queue_waiting and role in waiting_on_roles["seller"]:
            waiting = True
        elif "operator" in queue_waiting and role in waiting_on_roles["operator"]:
            waiting = True
        elif "document" in queue_waiting and role in waiting_on_roles["document"]:
            waiting = True

        normalized_participants.append({**row, "waiting_on": waiting})

    document_contact_guide = build_document_contact_directory(
        normalized_participants,
        [doc.get("kind") for doc in documents if str(doc.get("kind") or "").strip()],
    )
    for doc in documents:
        kind_key = str(doc.get("kind") or "").strip().lower()
        contact_card = document_contact_guide.get(kind_key) or {}
        doc["primary_contact_for_document_kind"] = contact_card.get(
            "primary_contact_for_document_kind"
        )
        doc["fallback_contacts_for_document_kind"] = contact_card.get(
            "fallback_contacts_for_document_kind", []
        )
        doc["missing_contact_roles"] = contact_card.get("missing_contact_roles", [])
        doc["document_contact_card"] = contact_card

    listing_contacts = []
    if property_row.get("listing_agent_name") or property_row.get("listing_agent_phone") or property_row.get("listing_agent_email"):
        listing_contacts.append(
            {
                "role": "listing_agent",
                "name": property_row.get("listing_agent_name"),
                "email": property_row.get("listing_agent_email"),
                "phone": property_row.get("listing_agent_phone"),
                "company": property_row.get("listing_office_name"),
                "website": property_row.get("listing_agent_website"),
                "source_type": "listing_import",
            }
        )
    if property_row.get("listing_office_name") or property_row.get("listing_office_phone") or property_row.get("listing_office_email"):
        listing_contacts.append(
            {
                "role": "listing_office",
                "name": property_row.get("listing_office_name"),
                "email": property_row.get("listing_office_email"),
                "phone": property_row.get("listing_office_phone"),
                "company": property_row.get("listing_office_name"),
                "source_type": "listing_import",
            }
        )

    actionable_summary = _rollup_actionable_summary(documents)

    return {
        "property": property_row,
        "acquisition": {
            **acquisition_row,
            "contacts": merged_contacts,
            "listing_contacts": listing_contacts,
            "milestones": milestones,
            "days_to_close": _days_to_close(target_close),
            "deadlines": deadlines,
            "field_values": field_values,
            "actionable_summary": actionable_summary,
            "document_contact_guide": document_contact_guide,
        },
        "documents": documents,
        "participants": normalized_participants,
        "deadlines": deadlines,
        "field_values": field_values,
        "required_documents": required_docs,
        "actionable_summary": actionable_summary,
        "document_contact_guide": document_contact_guide,
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

    rows = _rows_to_dicts(
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

                    p.listing_status,
                    p.listing_days_on_market,
                    p.listing_zillow_url,
                    p.listing_agent_name,
                    p.listing_agent_phone,
                    p.listing_agent_email,
                    p.listing_office_name,
                    p.listing_office_phone,
                    p.listing_office_email,

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
                          and dl.status = 'active'
                          and dl.due_at::date < current_date
                    ) as overdue_deadline_count
                from properties p
                join acquisition_records ar
                  on ar.org_id = p.org_id and ar.property_id = p.id
                left join property_states ps
                  on ps.org_id = p.org_id and ps.property_id = p.id
                where p.org_id = :org_id
                order by ar.updated_at desc nulls last, p.id desc
                limit :limit
                offset :offset
                """
            ),
            {
                "org_id": int(org_id),
                "limit": int(limit),
                "offset": int(offset),
            },
        ).fetchall()
    )

    filtered: list[dict[str, Any]] = []
    for row in rows:
        text_blob = " ".join(
            str(x or "")
            for x in [
                row.get("address"),
                row.get("city"),
                row.get("state"),
                row.get("zip"),
                row.get("county"),
                row.get("waiting_on"),
                row.get("next_step"),
                row.get("listing_agent_name"),
                row.get("listing_office_name"),
            ]
        ).lower()

        if q and q not in text_blob:
            continue

        row_status = str(row.get("status") or "").strip().lower()
        if status and row_status != status:
            continue

        row_waiting = str(row.get("waiting_on") or "").strip().lower()
        if waiting_on and waiting_on not in row_waiting:
            continue

        detail = get_acquisition_detail(db, org_id=org_id, property_id=int(row["property_id"]))
        required_documents = detail.get("required_documents") if detail else []
        field_values = detail.get("field_values") if detail else []
        participants = detail.get("participants") if detail else []

        grouped_values: dict[str, set[str]] = {}
        for fv in field_values:
            key = str(fv.get("field_name") or "").strip().lower()
            value = str(
                fv.get("value_text")
                if fv.get("value_text") is not None
                else fv.get("extracted_value") or fv.get("value_number") or ""
            ).strip().lower()
            if not key or not value:
                continue
            grouped_values.setdefault(key, set()).add(value)
        conflict_count = sum(1 for values in grouped_values.values() if len(values) > 1)

        missing_document_groups = [
            {"kind": doc.get("kind"), "label": doc.get("label")}
            for doc in required_documents
            if not doc.get("present")
        ]

        has_overdue = int(row.get("overdue_deadline_count") or 0) > 0
        has_missing_docs = bool(missing_document_groups)
        suggested_field_count = int(row.get("suggested_field_count") or 0)

        if has_overdue_deadlines is True and not has_overdue:
            continue
        if has_missing_required_docs is True and not has_missing_docs:
            continue
        if needs_review is True and not (suggested_field_count > 0 or conflict_count > 0):
            continue

        readiness = 0
        total_docs = len(required_documents)
        present_docs = total_docs - len(missing_document_groups)
        if total_docs > 0:
            readiness += round((present_docs / total_docs) * 50)
        readiness += min(int(row.get("document_count") or 0) * 4, 20)
        days = _days_to_close(row.get("target_close_date") or row.get("closing_date"))
        if days is not None:
            if days > 14:
                readiness += 20
            elif days >= 7:
                readiness += 14
            elif days >= 0:
                readiness += 8
            else:
                readiness -= 10
        if "document" in row_waiting:
            readiness -= 10
        if conflict_count > 0:
            readiness -= min(conflict_count * 8, 20)
        readiness = max(0, min(100, readiness))

        listing_contacts = []
        if row.get("listing_agent_name") or row.get("listing_agent_phone") or row.get("listing_agent_email"):
            listing_contacts.append(
                {
                    "role": "listing_agent",
                    "name": row.get("listing_agent_name"),
                    "email": row.get("listing_agent_email"),
                    "phone": row.get("listing_agent_phone"),
                    "company": row.get("listing_office_name"),
                    "source_type": "listing_import",
                }
            )
        if row.get("listing_office_name") or row.get("listing_office_phone") or row.get("listing_office_email"):
            listing_contacts.append(
                {
                    "role": "listing_office",
                    "name": row.get("listing_office_name"),
                    "email": row.get("listing_office_email"),
                    "phone": row.get("listing_office_phone"),
                    "company": row.get("listing_office_name"),
                    "source_type": "listing_import",
                }
            )

        filtered.append(
            {
                **row,
                "days_to_close": days,
                "conflict_count": conflict_count,
                "missing_document_groups": missing_document_groups,
                "estimated_close_readiness": readiness,
                "listing_contacts": listing_contacts,
                "participant_count": len(participants),
                "suggested_field_count": suggested_field_count,
            }
        )

    return {"items": filtered, "count": len(filtered)}


def add_acquisition_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)

    kind = _normalize_document_kind(payload.get("kind"))
    name = _clean_text(payload.get("name")) or ACQUISITION_DOCUMENT_KIND_LABELS.get(kind) or kind.replace("_", " ").title()

    db.execute(
        text(
            """
            insert into acquisition_documents (
                org_id,
                property_id,
                kind,
                name,
                status,
                source_url,
                extracted_text,
                extracted_fields_json,
                notes,
                created_at,
                updated_at
            )
            values (
                :org_id,
                :property_id,
                :kind,
                :name,
                :status,
                :source_url,
                :extracted_text,
                :extracted_fields_json,
                :notes,
                now(),
                now()
            )
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "kind": kind,
            "name": name,
            "status": _clean_text(payload.get("status")) or "received",
            "source_url": _clean_text(payload.get("source_url")),
            "extracted_text": _clean_text(payload.get("extracted_text")),
            "extracted_fields_json": json.dumps(payload.get("extracted_fields") or {}),
            "notes": _clean_text(payload.get("notes")),
        },
    )
    db.commit()
    row = _row_to_dict(
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
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    ) or {}
    row["extracted_fields"] = _safe_json_load(row.get("extracted_fields_json"), {})
    return row


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

    normalized_kind = _normalize_document_kind(kind)
    filename = _sanitize_filename(upload.filename or name or "upload")
    ext = _validate_extension_and_content_type(filename, upload.content_type)

    replacement_row: dict[str, Any] | None = None
    if replace_document_id is not None:
        replacement_row = _get_document_by_id(
            db,
            org_id=org_id,
            property_id=property_id,
            document_id=int(replace_document_id),
        )
        if not replacement_row:
            raise HTTPException(status_code=404, detail="Replacement target document not found.")
        if str(replacement_row.get("status") or "").lower() in {"deleted", "replaced"}:
            raise HTTPException(status_code=409, detail="Replacement target is not active.")
        original_kind = str(replacement_row.get("kind") or "").strip().lower()
        if original_kind and original_kind != normalized_kind:
            raise HTTPException(
                status_code=422,
                detail=f"Replacement kind mismatch. Existing document is '{original_kind}' but upload kind is '{normalized_kind}'.",
            )

    root = _acquisition_upload_root()
    property_dir = root / str(org_id) / str(property_id)
    unique_name = f"{uuid.uuid4().hex}_{filename}"
    target_path = property_dir / unique_name

    meta = _stream_upload_to_disk(upload, target_path)
    try:
        _sniff_magic_bytes(ext, meta["head"])
        if ext == ".docx":
            _validate_docx_safely(target_path)

        duplicate_row = _get_duplicate_document_by_sha256(
            db,
            org_id=org_id,
            property_id=property_id,
            sha256=meta["sha256"],
        )
        if duplicate_row:
            duplicate_id = int(duplicate_row["id"])
            if replace_document_id is None or duplicate_id != int(replace_document_id):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "duplicate_document",
                        "message": "An identical file is already attached to this property. Use replace to supersede the existing document.",
                        "existing_document_id": duplicate_id,
                        "existing_kind": duplicate_row.get("kind"),
                        "sha256": meta["sha256"],
                    },
                )

        scan_result = scan_file(target_path)
        scan_status = "clean"
        scan_result_text = "clean"
        if isinstance(scan_result, dict):
            scan_status = str(scan_result.get("status") or "clean")
            scan_result_text = str(scan_result.get("result") or scan_result.get("detail") or scan_status)
        elif isinstance(scan_result, str):
            scan_result_text = scan_result

        parsed = parse_document(
            target_path,
            upload.content_type or "application/octet-stream",
            document_kind=normalized_kind,
            filename=upload.filename or filename,
        )
        preview_text = None
        extracted_text = None
        parser_payload: dict[str, Any] = {}
        extracted_fields = {}
        parser_version = None
        parse_status = "parsed"

        if isinstance(parsed, dict):
            parser_payload = dict(parsed)
            preview_text = parsed.get("preview_text")
            extracted_text = parsed.get("extracted_text") or parsed.get("text")
            extracted_fields = parsed.get("extracted_fields") or parsed.get("fields") or {}
            parser_version = parsed.get("parser_version")
            parse_status = str(parsed.get("parse_status") or parsed.get("status") or "parsed")

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
                    extracted_text,
                    extracted_fields_json,
                    notes,
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
                    :content_type,
                    :file_size_bytes,
                    :sha256,
                    'uploaded',
                    :scan_status,
                    :scan_result,
                    :parse_status,
                    :parser_version,
                    :preview_text,
                    'received',
                    :extracted_text,
                    :extracted_fields_json,
                    :notes,
                    now(),
                    now()
                )
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "kind": normalized_kind,
                "name": _clean_text(name) or ACQUISITION_DOCUMENT_KIND_LABELS.get(normalized_kind) or filename,
                "original_filename": upload.filename or filename,
                "storage_path": str(target_path),
                "content_type": upload.content_type,
                "file_size_bytes": meta["size_bytes"],
                "sha256": meta["sha256"],
                "scan_status": scan_status,
                "scan_result": scan_result_text,
                "parse_status": parse_status,
                "parser_version": parser_version,
                "preview_text": preview_text,
                "extracted_text": extracted_text,
                "extracted_fields_json": json.dumps(parser_payload or extracted_fields or {}),
                "notes": _clean_text(notes),
            },
        )
        db.commit()
    except Exception:
        target_path.unlink(missing_ok=True)
        raise

    row = _row_to_dict(
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
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    ) or {}
    row["extracted_fields"] = _safe_json_load(row.get("extracted_fields_json"), {})

    if replace_document_id is not None and row.get("id"):
        replace_acquisition_document(
            db,
            org_id=org_id,
            property_id=property_id,
            document_id=int(replace_document_id),
            replacement_document_id=int(row["id"]),
            reason="superseded_by_upload",
        )

    if row.get("id") and extracted_fields:
        create_field_suggestions_from_document(
            db,
            org_id=org_id,
            property_id=property_id,
            document_id=int(row["id"]),
            extracted_fields=extracted_fields or {},
            extraction_version=parser_version,
        )

    return row


def replace_acquisition_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    replacement_document_id: int,
    reason: str | None = None,
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
            {
                "document_id": int(document_id),
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found.")

    db.execute(
        text(
            """
            update acquisition_documents
            set status = 'replaced',
                replaced_by_document_id = :replacement_document_id,
                deleted_reason = :reason,
                updated_at = now()
            where id = :document_id
            """
        ),
        {
            "document_id": int(document_id),
            "replacement_document_id": int(replacement_document_id),
            "reason": _clean_text(reason),
        },
    )
    db.commit()

    updated = _row_to_dict(
        db.execute(
            text("select * from acquisition_documents where id = :document_id"),
            {"document_id": int(document_id)},
        ).fetchone()
    ) or {}
    updated["extracted_fields"] = _safe_json_load(updated.get("extracted_fields_json"), {})
    return updated


def delete_acquisition_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    reason: str | None = None,
    hard_delete_file: bool = True,
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
            {
                "document_id": int(document_id),
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found.")

    storage_path = _clean_text(row.get("storage_path"))

    db.execute(
        text(
            """
            update acquisition_documents
            set status = 'deleted',
                storage_url = null,
                deleted_at = now(),
                deleted_reason = :reason,
                updated_at = now()
            where id = :document_id
            """
        ),
        {"document_id": int(document_id), "reason": _clean_text(reason)},
    )

    # Remove all document-derived review rows so deleted documents stop surfacing
    # in parsed field review immediately.
    db.execute(
        text(
            """
            delete from acquisition_field_values
            where org_id = :org_id
              and property_id = :property_id
              and source_document_id = :document_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "document_id": int(document_id),
        },
    )

    # Remove document-derived deadlines and participants that were sourced from
    # this document so operational panels stay in sync with the file stack.
    # Remove document-derived deadlines
    db.execute(
        text(
            """
            delete from acquisition_deadlines
            where org_id = :org_id
            and property_id = :property_id
            and source_document_id = :document_id
            and coalesce(manually_overridden, false) = false
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "document_id": int(document_id),
        },
    )

# Remove document-derived participants (SAFE VERSION)
    if _col_exists(db, "acquisition_contacts", "source_document_id"):
        db.execute(
            text(
                """
                delete from acquisition_contacts
                where org_id = :org_id
                and property_id = :property_id
                and source_document_id = :document_id
                and coalesce(manually_overridden, false) = false
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "document_id": int(document_id),
            },
        )
    db.commit()

    if hard_delete_file and storage_path:
        try:
            Path(storage_path).unlink(missing_ok=True)
        except Exception:
            pass

    updated = _row_to_dict(
        db.execute(
            text("select * from acquisition_documents where id = :document_id"),
            {"document_id": int(document_id)},
        ).fetchone()
    ) or {}
    updated["extracted_fields"] = _safe_json_load(updated.get("extracted_fields_json"), {})
    return updated


def get_document_file_response(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    disposition: str = "inline",
):
    row = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_documents
                where id = :document_id and org_id = :org_id and property_id = :property_id
                """
            ),
            {
                "document_id": int(document_id),
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found.")

    storage_path = _clean_text(row.get("storage_path"))
    if not storage_path or not Path(storage_path).exists():
        raise HTTPException(status_code=404, detail="Stored file not found.")

    filename = _clean_text(row.get("original_filename")) or _clean_text(row.get("name")) or f"document-{document_id}"
    media_type = _clean_text(row.get("content_type")) or "application/octet-stream"
    return FileResponse(
        path=storage_path,
        media_type=media_type,
        filename=filename,
        content_disposition_type="attachment" if disposition == "attachment" else "inline",
    )