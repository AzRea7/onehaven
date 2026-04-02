from __future__ import annotations

from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        return dict(row._mapping)
    except Exception:
        return dict(row)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _clean_role(role: Any) -> str:
    text_value = str(role or "").strip().lower()
    if not text_value:
        raise HTTPException(status_code=422, detail="role is required.")
    return text_value


def _normalize_role(role: str) -> str:
    raw = _clean_role(role)

    aliases = {
        "agent": "listing_agent",
        "listing agent": "listing_agent",
        "listing_agent": "listing_agent",
        "broker": "listing_agent",
        "realtor": "listing_agent",
        "seller_agent": "listing_agent",
        "office": "listing_office",
        "brokerage": "listing_office",
        "listing_office": "listing_office",
        "title": "title_company",
        "title_company": "title_company",
        "loan officer": "loan_officer",
        "loan_officer": "loan_officer",
    }
    return aliases.get(raw, raw.replace(" ", "_"))


def _col_exists(db: Session, table: str, column: str) -> bool:
    row = db.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table_name
              AND column_name = :column_name
            LIMIT 1
            """
        ),
        {"table_name": table, "column_name": column},
    ).first()
    return row is not None


def _participant_sort_rank(row: dict[str, Any]) -> tuple[int, str, int]:
    role = str(row.get("role") or "")
    primary = bool(row.get("is_primary"))
    waiting = bool(row.get("waiting_on"))

    if waiting:
        rank = 0
    elif primary:
        rank = 1
    elif role in {"listing_agent", "listing_office"}:
        rank = 2
    else:
        rank = 3

    return (rank, role, int(row.get("id") or 0))


def list_participants(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> list[dict[str, Any]]:
    has_waiting_on = _col_exists(db, "acquisition_contacts", "waiting_on")
    has_is_primary = _col_exists(db, "acquisition_contacts", "is_primary")

    order_parts: list[str] = []
    if has_waiting_on:
        order_parts.append(
            "case when coalesce(waiting_on, false) then 0 else 1 end asc"
        )
    if has_is_primary:
        order_parts.append(
            "case when coalesce(is_primary, false) then 0 else 1 end asc"
        )
    order_parts.extend(["role asc", "id asc"])
    order_sql = ",\n                ".join(order_parts)

    rows = db.execute(
        text(
            f"""
            select *
            from acquisition_contacts
            where org_id = :org_id and property_id = :property_id
            order by
                {order_sql}
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchall()

    out = [_row_to_dict(row) or {} for row in rows]
    out.sort(key=_participant_sort_rank)
    return out

def delete_participant(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    participant_id: int,
) -> dict[str, Any]:
    row = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_contacts
                where id = :participant_id and org_id = :org_id and property_id = :property_id
                """
            ),
            {
                "participant_id": int(participant_id),
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Participant not found.")

    db.execute(
        text("delete from acquisition_contacts where id = :participant_id"),
        {"participant_id": int(participant_id)},
    )
    db.commit()
    return row

def upsert_participant(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    role: str,
    name: str,
    email: str | None = None,
    phone: str | None = None,
    company: str | None = None,
    notes: str | None = None,
    source_document_id: int | None = None,
    confidence: float | None = None,
    extraction_version: str | None = None,
    manually_overridden: bool = False,
    is_primary: bool | None = None,
    waiting_on: bool | None = None,
    source_type: str | None = None,
) -> dict[str, Any]:
    normalized_role = _normalize_role(role)
    clean_name = _clean_text(name)
    clean_email = _clean_text(email)
    clean_phone = _clean_text(phone)
    clean_company = _clean_text(company)
    clean_notes = _clean_text(notes)
    clean_source_type = _clean_text(source_type)

    if not clean_name:
        raise HTTPException(status_code=422, detail="name is required.")

    has_is_primary = _col_exists(db, "acquisition_contacts", "is_primary")
    has_waiting_on = _col_exists(db, "acquisition_contacts", "waiting_on")
    has_source_type = _col_exists(db, "acquisition_contacts", "source_type")

    existing = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_contacts
                where org_id = :org_id
                  and property_id = :property_id
                  and role = :role
                order by id desc
                limit 1
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "role": normalized_role,
            },
        ).fetchone()
    )

    if existing:
        merged_name = clean_name or _clean_text(existing.get("name"))
        merged_email = clean_email or _clean_text(existing.get("email"))
        merged_phone = clean_phone or _clean_text(existing.get("phone"))
        merged_company = clean_company or _clean_text(existing.get("company"))
        merged_notes = clean_notes or _clean_text(existing.get("notes"))

        set_parts = [
            "name = :name",
            "email = :email",
            "phone = :phone",
            "company = :company",
            "notes = :notes",
            "source_document_id = :source_document_id",
            "confidence = :confidence",
            "extraction_version = :extraction_version",
            "manually_overridden = :manually_overridden",
        ]
        params: dict[str, Any] = {
            "id": int(existing["id"]),
            "name": merged_name,
            "email": merged_email,
            "phone": merged_phone,
            "company": merged_company,
            "notes": merged_notes,
            "source_document_id": source_document_id,
            "confidence": confidence,
            "extraction_version": extraction_version,
            "manually_overridden": bool(manually_overridden),
        }

        if has_is_primary:
            merged_is_primary = (
                bool(is_primary)
                if is_primary is not None
                else bool(existing.get("is_primary"))
            )
            set_parts.append("is_primary = :is_primary")
            params["is_primary"] = merged_is_primary

        if has_waiting_on:
            merged_waiting_on = (
                bool(waiting_on)
                if waiting_on is not None
                else bool(existing.get("waiting_on"))
            )
            set_parts.append("waiting_on = :waiting_on")
            params["waiting_on"] = merged_waiting_on

        if has_source_type:
            merged_source_type = clean_source_type or _clean_text(
                existing.get("source_type")
            )
            set_parts.append("source_type = :source_type")
            params["source_type"] = merged_source_type

        set_parts.append("updated_at = now()")

        db.execute(
            text(
                f"""
                update acquisition_contacts
                set {", ".join(set_parts)}
                where id = :id
                """
            ),
            params,
        )
        db.commit()

        return _row_to_dict(
            db.execute(
                text("select * from acquisition_contacts where id = :id"),
                {"id": int(existing["id"])},
            ).fetchone()
        ) or {}

    columns = [
        "org_id",
        "property_id",
        "role",
        "name",
        "email",
        "phone",
        "company",
        "notes",
        "source_document_id",
        "confidence",
        "extraction_version",
        "manually_overridden",
    ]
    values = [
        ":org_id",
        ":property_id",
        ":role",
        ":name",
        ":email",
        ":phone",
        ":company",
        ":notes",
        ":source_document_id",
        ":confidence",
        ":extraction_version",
        ":manually_overridden",
    ]
    params = {
        "org_id": int(org_id),
        "property_id": int(property_id),
        "role": normalized_role,
        "name": clean_name,
        "email": clean_email,
        "phone": clean_phone,
        "company": clean_company,
        "notes": clean_notes,
        "source_document_id": source_document_id,
        "confidence": confidence,
        "extraction_version": extraction_version,
        "manually_overridden": bool(manually_overridden),
    }

    if has_is_primary:
        columns.append("is_primary")
        values.append(":is_primary")
        params["is_primary"] = bool(is_primary) if is_primary is not None else False

    if has_waiting_on:
        columns.append("waiting_on")
        values.append(":waiting_on")
        params["waiting_on"] = bool(waiting_on) if waiting_on is not None else False

    if has_source_type:
        columns.append("source_type")
        values.append(":source_type")
        params["source_type"] = clean_source_type

    columns.extend(["created_at", "updated_at"])
    values.extend(["now()", "now()"])

    db.execute(
        text(
            f"""
            insert into acquisition_contacts (
                {", ".join(columns)}
            )
            values (
                {", ".join(values)}
            )
            """
        ),
        params,
    )
    db.commit()

    return _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_contacts
                where org_id = :org_id and property_id = :property_id and role = :role
                order by id desc
                limit 1
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "role": normalized_role,
            },
        ).fetchone()
    ) or {}


def seed_listing_contacts_from_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    mark_primary: bool = True,
) -> list[dict[str, Any]]:
    property_row = _row_to_dict(
        db.execute(
            text(
                """
                select
                    id,
                    listing_agent_name,
                    listing_agent_phone,
                    listing_agent_email,
                    listing_agent_website,
                    listing_office_name,
                    listing_office_phone,
                    listing_office_email,
                    listing_status
                from properties
                where org_id = :org_id and id = :property_id
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not property_row:
        raise HTTPException(status_code=404, detail="Property not found.")

    created: list[dict[str, Any]] = []

    agent_name = _clean_text(property_row.get("listing_agent_name"))
    agent_phone = _clean_text(property_row.get("listing_agent_phone"))
    agent_email = _clean_text(property_row.get("listing_agent_email"))
    agent_website = _clean_text(property_row.get("listing_agent_website"))

    if agent_name or agent_phone or agent_email:
        notes_parts = ["Seeded from ingested listing metadata"]
        if agent_website:
            notes_parts.append(f"Website: {agent_website}")

        created.append(
            upsert_participant(
                db,
                org_id=org_id,
                property_id=property_id,
                role="listing_agent",
                name=agent_name or "Listing agent",
                email=agent_email,
                phone=agent_phone,
                company=_clean_text(property_row.get("listing_office_name")),
                notes=" · ".join(notes_parts),
                manually_overridden=False,
                is_primary=mark_primary,
                waiting_on=False,
                source_type="listing_import",
                extraction_version="listing_metadata_v1",
            )
        )

    office_name = _clean_text(property_row.get("listing_office_name"))
    office_phone = _clean_text(property_row.get("listing_office_phone"))
    office_email = _clean_text(property_row.get("listing_office_email"))

    if office_name or office_phone or office_email:
        created.append(
            upsert_participant(
                db,
                org_id=org_id,
                property_id=property_id,
                role="listing_office",
                name=office_name or "Listing office",
                email=office_email,
                phone=office_phone,
                company=office_name,
                notes="Seeded from ingested listing metadata",
                manually_overridden=False,
                is_primary=False,
                waiting_on=False,
                source_type="listing_import",
                extraction_version="listing_metadata_v1",
            )
        )

    return created