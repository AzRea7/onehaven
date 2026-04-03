from __future__ import annotations

import json
import re
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from .acquisition_deadline_service import upsert_deadline_by_code
from .acquisition_participant_service import upsert_participant

REVIEW_STATES = {"suggested", "accepted", "rejected", "superseded"}

FIELD_TO_RECORD_COLUMN = {
    "purchase_price": "purchase_price",
    "earnest_money": "earnest_money",
    "loan_amount": "loan_amount",
    "loan_type": "loan_type",
    "interest_rate": "interest_rate",
    "cash_to_close": "cash_to_close",
    "closing_costs": "closing_costs",
    "seller_credits": "seller_credits",
    "title_company": "title_company",
    "escrow_officer": "escrow_officer",
}

DEADLINE_FIELD_TO_CODE = {
    "inspection_contingency_date": "inspection_contingency",
    "financing_contingency_date": "financing_contingency",
    "appraisal_deadline": "appraisal",
    "earnest_money_deadline": "earnest_money",
    "title_objection_deadline": "title_objection",
    "coverage_effective_date": "insurance_due",
    "closing_datetime": "closing_datetime",
    "closing_date": "closing_datetime",
    "target_close_date": "closing_datetime",
}

CONTACT_FIELD_TO_ROLE = {
    "lender_name": "lender",
    "loan_officer_contact": "loan_officer",
    "title_company": "title_company",
    "escrow_officer": "escrow_officer",
    "listing_agent_name": "listing_agent",
    "buyer_agent_name": "buyer_agent",
    "brokerage_contacts": "listing_office",
    "carrier_name": "insurance_agency",
    "insurance_agent_contact": "insurance_agent",
    "inspection_company": "inspection_company",
    "inspector_name": "inspector",
}

FIELD_LABELS = {
    "purchase_price": "Purchase price",
    "earnest_money": "Earnest money",
    "loan_amount": "Loan amount",
    "loan_type": "Loan type",
    "interest_rate": "Interest rate",
    "cash_to_close": "Cash to close",
    "closing_costs": "Closing costs",
    "seller_credits": "Seller credits",
    "title_company": "Title company",
    "escrow_officer": "Escrow officer",
    "inspection_contingency_date": "Inspection contingency deadline",
    "financing_contingency_date": "Financing contingency deadline",
    "appraisal_deadline": "Appraisal deadline",
    "target_close_date": "Target close date",
    "closing_date": "Closing date",
    "coverage_effective_date": "Coverage effective date",
    "lender_name": "Lender",
    "loan_officer_contact": "Loan officer",
    "listing_agent_name": "Listing agent",
    "buyer_agent_name": "Buyer agent",
    "carrier_name": "Carrier",
    "insurance_agent_contact": "Insurance agent",
    "brokerage_contacts": "Brokerage",
    "inspection_company": "Inspection company",
    "inspector_name": "Inspector",
}


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        return dict(row._mapping)
    except Exception:
        return dict(row)


def _normalize_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def _normalized_value_parts(value: Any) -> tuple[str | None, float | None, str | None]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return None, float(value), None
    if isinstance(value, list):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return (" • ".join(cleaned) or None), None, None
    if isinstance(value, str):
        stripped = value.strip()
        if len(stripped) == 10 and stripped[4] == "-" and stripped[7] == "-":
            return None, None, stripped
        return stripped or None, None, None
    if value is None:
        return None, None, None
    return json.dumps(value), None, None




def _extract_first_email(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", text, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _extract_first_phone(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"(\+?1?[\s\-.]?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4})", text)
    return match.group(1).strip() if match else None


def _clean_contact_name(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = re.sub(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\+?1?[\s\-.]?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}", " ", text)
    text = re.sub(r"(?:phone|ph|cell|mobile|office|email|e-mail|contact|broker|agent|loan officer|loan originator|inspector|inspection company|company|carrier)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" ,;:-")
    return text or None


def _participant_kwargs_for_field(field_name: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    role = CONTACT_FIELD_TO_ROLE.get(field_name)
    if not role:
        return None
    raw_value = payload.get("value")
    if raw_value in (None, "", []):
        return None
    raw_text = str(raw_value).strip()
    email = _extract_first_email(raw_text)
    phone = _extract_first_phone(raw_text)
    cleaned = _clean_contact_name(raw_text)
    name = cleaned or raw_text
    company = None
    if role in {"lender", "title_company", "insurance_agency", "listing_office", "inspection_company"}:
        company = cleaned or raw_text
        name = company
    return {
        "role": role,
        "name": name,
        "email": email,
        "phone": phone,
        "company": company,
        "source_document_id": None,
        "confidence": float(payload.get("confidence") or 0.85),
        "extraction_version": None,
        "manually_overridden": False,
    }


def _flatten_parser_payload(extracted_fields: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    if not extracted_fields:
        return []
    facts = extracted_fields.get("facts") if isinstance(extracted_fields, dict) else None
    if isinstance(facts, dict):
        out = []
        for field_name, payload in facts.items():
            if not isinstance(payload, dict):
                payload = {"value": payload}
            out.append((str(field_name), payload))
        return out
    return [(str(field_name), {"value": value}) for field_name, value in extracted_fields.items()]


def list_document_field_values(db: Session, *, org_id: int, property_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            select
                fv.*,
                ad.name as source_document_name,
                ad.kind as source_document_kind
            from acquisition_field_values fv
            left join acquisition_documents ad
              on ad.id = fv.source_document_id
            where fv.org_id = :org_id and fv.property_id = :property_id
            order by fv.field_name asc, fv.created_at desc, fv.id desc
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        item = _row_to_dict(row) or {}
        normalized = _normalize_json(item.get("normalized_value_json"), None)
        if isinstance(normalized, dict) and "value" in normalized:
            value = normalized.get("value")
            excerpt = normalized.get("excerpt")
            if excerpt is None and isinstance(normalized.get("value"), list) and normalized.get("value"):
                excerpt = str(normalized.get("value")[0])
            item["source_excerpt"] = excerpt
            item["label"] = normalized.get("label") or FIELD_LABELS.get(item.get("field_name"), item.get("field_name"))
        else:
            value = normalized if normalized is not None else item.get("extracted_value")
            item["label"] = FIELD_LABELS.get(item.get("field_name"), item.get("field_name"))
        value_text, value_number, value_date = _normalized_value_parts(value)
        item["value_text"] = value_text
        item["value_number"] = value_number
        item["value_date"] = value_date
        item["conflict"] = False
        out.append(item)

    grouped: dict[str, set[str]] = {}
    for item in out:
        key = str(item.get("field_name") or "").strip().lower()
        value_repr = json.dumps(
            item.get("value_date") if item.get("value_date") is not None else item.get("value_number") if item.get("value_number") is not None else item.get("value_text"),
            sort_keys=True,
            default=str,
        )
        if key:
            grouped.setdefault(key, set()).add(value_repr)
    for item in out:
        key = str(item.get("field_name") or "").strip().lower()
        item["conflict"] = len(grouped.get(key, set())) > 1
    return out


def create_field_suggestions_from_document(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int,
    extracted_fields: dict[str, Any],
    extraction_version: str | None,
) -> list[dict[str, Any]]:
    from .acquisition_service import ensure_acquisition_record

    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)
    payload_rows = _flatten_parser_payload(extracted_fields)
    if not payload_rows:
        return []

    created: list[dict[str, Any]] = []
    for field_name, payload in payload_rows:
        field_value = payload.get("value")
        if field_value in (None, "", []):
            continue

        existing = _row_to_dict(
            db.execute(
                text(
                    """
                    select *
                    from acquisition_field_values
                    where org_id = :org_id
                      and property_id = :property_id
                      and field_name = :field_name
                      and source_document_id = :source_document_id
                      and extracted_value = :extracted_value
                    order by id desc
                    limit 1
                    """
                ),
                {
                    "org_id": int(org_id),
                    "property_id": int(property_id),
                    "field_name": str(field_name),
                    "source_document_id": int(document_id),
                    "extracted_value": str(field_value),
                },
            ).fetchone()
        )
        if existing:
            created.append(existing)
            continue

        normalized_payload = {
            "value": field_value,
            "label": payload.get("label") or FIELD_LABELS.get(field_name, field_name.replace("_", " ").title()),
            "excerpt": payload.get("excerpt"),
            "value_type": payload.get("value_type"),
        }
        db.execute(
            text(
                """
                insert into acquisition_field_values (
                    org_id,
                    property_id,
                    field_name,
                    extracted_value,
                    normalized_value_json,
                    review_state,
                    source_document_id,
                    confidence,
                    extraction_version,
                    manually_overridden,
                    created_at,
                    updated_at
                )
                values (
                    :org_id,
                    :property_id,
                    :field_name,
                    :extracted_value,
                    :normalized_value_json,
                    'suggested',
                    :source_document_id,
                    :confidence,
                    :extraction_version,
                    false,
                    now(),
                    now()
                )
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "field_name": str(field_name),
                "extracted_value": str(field_value),
                "normalized_value_json": json.dumps(normalized_payload),
                "source_document_id": int(document_id),
                "confidence": float(payload.get("confidence") or 0.85),
                "extraction_version": extraction_version or "operator_v2",
            },
        )

        deadline_code = DEADLINE_FIELD_TO_CODE.get(str(field_name))
        if deadline_code and payload.get("value"):
            upsert_deadline_by_code(
                db,
                org_id=org_id,
                property_id=property_id,
                code=deadline_code,
                due_at=str(payload.get("value")),
                source_document_id=int(document_id),
                confidence=float(payload.get("confidence") or 0.85),
                extraction_version=extraction_version or "operator_v2",
                manually_overridden=False,
            )

        participant_kwargs = _participant_kwargs_for_field(str(field_name), payload)
        if participant_kwargs:
            participant_kwargs["source_document_id"] = int(document_id)
            participant_kwargs["extraction_version"] = extraction_version or "operator_v2"
            upsert_participant(
                db,
                org_id=org_id,
                property_id=property_id,
                **participant_kwargs,
            )
    db.commit()
    return list_document_field_values(db, org_id=org_id, property_id=property_id)


def _write_field_to_canonical_record(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    field_name: str,
    normalized_value: Any,
) -> None:
    record_column = FIELD_TO_RECORD_COLUMN.get(field_name)
    if record_column:
        db.execute(
            text(
                f"""
                update acquisition_records
                set {record_column} = :value, updated_at = now()
                where org_id = :org_id and property_id = :property_id
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "value": normalized_value,
            },
        )

    deadline_code = DEADLINE_FIELD_TO_CODE.get(field_name)
    if deadline_code and normalized_value:
        upsert_deadline_by_code(
            db,
            org_id=org_id,
            property_id=property_id,
            code=deadline_code,
            due_at=str(normalized_value),
            source_document_id=None,
            confidence=1.0,
            extraction_version="accepted",
            manually_overridden=False,
        )

    participant_role = CONTACT_FIELD_TO_ROLE.get(field_name)
    if participant_role and normalized_value:
        upsert_participant(
            db,
            org_id=org_id,
            property_id=property_id,
            role=participant_role,
            name=str(normalized_value),
            source_document_id=None,
            confidence=1.0,
            extraction_version="accepted",
            manually_overridden=False,
        )


def accept_field_value(db: Session, *, org_id: int, property_id: int, field_value_id: int) -> dict[str, Any]:
    row = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_field_values
                where id = :field_value_id and org_id = :org_id and property_id = :property_id
                limit 1
                """
            ),
            {"field_value_id": int(field_value_id), "org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Field suggestion not found.")

    db.execute(
        text(
            """
            update acquisition_field_values
            set review_state = 'accepted', updated_at = now()
            where id = :field_value_id and org_id = :org_id and property_id = :property_id
            """
        ),
        {"field_value_id": int(field_value_id), "org_id": int(org_id), "property_id": int(property_id)},
    )
    db.execute(
        text(
            """
            update acquisition_field_values
            set review_state = 'superseded', updated_at = now()
            where org_id = :org_id and property_id = :property_id
              and field_name = :field_name and id <> :field_value_id
              and review_state = 'accepted'
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id), "field_name": row["field_name"], "field_value_id": int(field_value_id)},
    )
    normalized_value = _normalize_json(row.get("normalized_value_json"), row.get("extracted_value"))
    if isinstance(normalized_value, dict) and "value" in normalized_value:
        normalized_value = normalized_value.get("value")
    _write_field_to_canonical_record(db, org_id=org_id, property_id=property_id, field_name=str(row["field_name"]), normalized_value=normalized_value)
    db.commit()
    fresh = list_document_field_values(db, org_id=org_id, property_id=property_id)
    return next((item for item in fresh if int(item.get("id") or 0) == int(field_value_id)), {})


def reject_field_value(db: Session, *, org_id: int, property_id: int, field_value_id: int) -> dict[str, Any]:
    db.execute(
        text(
            """
            update acquisition_field_values
            set review_state = 'rejected', updated_at = now()
            where id = :field_value_id and org_id = :org_id and property_id = :property_id
            """
        ),
        {"field_value_id": int(field_value_id), "org_id": int(org_id), "property_id": int(property_id)},
    )
    db.commit()
    fresh = list_document_field_values(db, org_id=org_id, property_id=property_id)
    row = next((item for item in fresh if int(item.get("id") or 0) == int(field_value_id)), None)
    if not row:
        raise HTTPException(status_code=404, detail="Field suggestion not found.")
    return row


def override_field_value(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    field_name: str,
    value: Any,
    source_document_id: int | None = None,
    extraction_version: str | None = None,
) -> dict[str, Any]:
    from .acquisition_service import ensure_acquisition_record

    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)
    if not field_name.strip():
        raise HTTPException(status_code=422, detail="field_name is required.")

    db.execute(
        text(
            """
            update acquisition_field_values
            set review_state = 'superseded', updated_at = now()
            where org_id = :org_id and property_id = :property_id
              and field_name = :field_name and review_state = 'accepted'
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id), "field_name": field_name.strip()},
    )
    normalized_payload = {"value": value, "label": FIELD_LABELS.get(field_name.strip(), field_name.strip().replace("_", " ").title()), "excerpt": None}
    db.execute(
        text(
            """
            insert into acquisition_field_values (
                org_id,
                property_id,
                field_name,
                extracted_value,
                normalized_value_json,
                review_state,
                source_document_id,
                confidence,
                extraction_version,
                manually_overridden,
                created_at,
                updated_at
            )
            values (
                :org_id,
                :property_id,
                :field_name,
                :extracted_value,
                :normalized_value_json,
                'accepted',
                :source_document_id,
                :confidence,
                :extraction_version,
                true,
                now(),
                now()
            )
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "field_name": field_name.strip(),
            "extracted_value": str(value),
            "normalized_value_json": json.dumps(normalized_payload),
            "source_document_id": source_document_id,
            "confidence": 1.0,
            "extraction_version": extraction_version or "manual_override",
        },
    )
    _write_field_to_canonical_record(db, org_id=org_id, property_id=property_id, field_name=field_name.strip(), normalized_value=value)
    db.commit()
    rows = list_document_field_values(db, org_id=org_id, property_id=property_id)
    return next((row for row in rows if str(row.get("field_name")) == field_name.strip() and str(row.get("review_state")) == "accepted"), {})
