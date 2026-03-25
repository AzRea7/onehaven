from __future__ import annotations

import json
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
    "cash_to_close": "cash_to_close",
    "closing_costs": "closing_costs",
    "title_company": "title_company",
    "escrow_officer": "escrow_officer",
}

DEADLINE_FIELD_TO_CODE = {
    "inspection_contingency_date": "inspection_contingency",
    "financing_contingency_date": "financing_contingency",
    "appraisal_deadline": "appraisal",
    "earnest_money_deadline": "earnest_money",
    "title_objection_deadline": "title_objection",
    "insurance_due_date": "insurance_due",
    "walkthrough_datetime": "walkthrough",
    "closing_datetime": "closing_datetime",
}

CONTACT_FIELD_TO_ROLE = {
    "lender_name": "lender",
    "title_company": "title_company",
    "escrow_officer": "escrow_officer",
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


def list_document_field_values(db: Session, *, org_id: int, property_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            select *
            from acquisition_field_values
            where org_id = :org_id and property_id = :property_id
            order by field_name asc, created_at desc, id desc
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchall()
    return [_row_to_dict(row) or {} for row in rows]


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
    from .acquisition_service import ensure_acquisition_record
    ensure_acquisition_record(db, org_id=org_id, property_id=property_id)
    if not extracted_fields:
        return []

    created: list[dict[str, Any]] = []
    for field_name, field_value in extracted_fields.items():
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
                "normalized_value_json": json.dumps(field_value),
                "source_document_id": int(document_id),
                "confidence": 0.85,
                "extraction_version": extraction_version or "v1",
            },
        )
        row = _row_to_dict(
            db.execute(
                text(
                    """
                    select *
                    from acquisition_field_values
                    where org_id = :org_id and property_id = :property_id
                    order by id desc
                    limit 1
                    """
                ),
                {"org_id": int(org_id), "property_id": int(property_id)},
            ).fetchone()
        )
        if row:
            created.append(row)

    db.commit()
    return created


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


def accept_field_value(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    field_value_id: int,
) -> dict[str, Any]:
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
            {
                "field_value_id": int(field_value_id),
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
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
        {
            "field_value_id": int(field_value_id),
            "org_id": int(org_id),
            "property_id": int(property_id),
        },
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
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "field_name": row["field_name"],
            "field_value_id": int(field_value_id),
        },
    )
    normalized_value = _normalize_json(row.get("normalized_value_json"), row.get("extracted_value"))
    _write_field_to_canonical_record(
        db,
        org_id=org_id,
        property_id=property_id,
        field_name=str(row["field_name"]),
        normalized_value=normalized_value,
    )
    db.commit()
    return _row_to_dict(
        db.execute(
            text("select * from acquisition_field_values where id = :id"),
            {"id": int(field_value_id)},
        ).fetchone()
    ) or {}


def reject_field_value(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    field_value_id: int,
) -> dict[str, Any]:
    db.execute(
        text(
            """
            update acquisition_field_values
            set review_state = 'rejected', updated_at = now()
            where id = :field_value_id and org_id = :org_id and property_id = :property_id
            """
        ),
        {
            "field_value_id": int(field_value_id),
            "org_id": int(org_id),
            "property_id": int(property_id),
        },
    )
    db.commit()
    row = _row_to_dict(
        db.execute(text("select * from acquisition_field_values where id = :id"), {"id": int(field_value_id)}).fetchone()
    )
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
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "field_name": field_name.strip(),
        },
    )
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
            "normalized_value_json": json.dumps(value),
            "source_document_id": source_document_id,
            "confidence": 1.0,
            "extraction_version": extraction_version or "manual_override",
        },
    )
    _write_field_to_canonical_record(
        db,
        org_id=org_id,
        property_id=property_id,
        field_name=field_name.strip(),
        normalized_value=value,
    )
    db.commit()
    return _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_field_values
                where org_id = :org_id and property_id = :property_id and field_name = :field_name
                order by id desc
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id), "field_name": field_name.strip()},
        ).fetchone()
    ) or {}
