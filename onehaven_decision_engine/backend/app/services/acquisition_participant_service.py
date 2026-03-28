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
    text = str(value).strip()
    return text or None


def _clean_role(role: Any) -> str:
    text = str(role or "").strip().lower()
    if not text:
        raise HTTPException(status_code=422, detail="role is required.")
    return text


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
    rows = db.execute(
        text(
            """
            select *
            from acquisition_contacts
            where org_id = :org_id and property_id = :property_id
            order by
                case when coalesce(waiting_on, false) then 0 else 1 end asc,
                case when coalesce(is_primary, false) then 0 else 1 end asc,
                role asc,
                id asc
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchall()

    out = [_row_to_dict(row) or {} for row in rows]
    out.sort(key=_participant_sort_rank)
    return out


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
    if not clean_name:
        raise HTTPException(status_code=422, detail="name is required.")

    clean_email = _clean_text(email)
    clean_phone = _clean_text(phone)
    clean_company = _clean_text(company)
    clean_notes = _clean_text(notes)
    clean_source_type = _clean_text(source_type)

    existing = _row_to_dict(
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
    )

    if existing:
        merged_name = clean_name or existing.get("name")
        merged_email = clean_email if clean_email is not None else existing.get("email")
        merged_phone = clean_phone if clean_phone is not None else existing.get("phone")
        merged_company = clean_company if clean_company is not None else existing.get("company")
        merged_notes = clean_notes if clean_notes is not None else existing.get("notes")
        merged_source_type = (
            clean_source_type if clean_source_type is not None else existing.get("source_type")
        )
        merged_is_primary = (
            bool(is_primary)
            if is_primary is not None
            else bool(existing.get("is_primary"))
        )
        merged_waiting_on = (
            bool(waiting_on)
            if waiting_on is not None
            else bool(existing.get("waiting_on"))
        )

        db.execute(
            text(
                """
                update acquisition_contacts
                set name = :name,
                    email = :email,
                    phone = :phone,
                    company = :company,
                    notes = :notes,
                    source_document_id = :source_document_id,
                    confidence = :confidence,
                    extraction_version = :extraction_version,
                    manually_overridden = :manually_overridden,
                    is_primary = :is_primary,
                    waiting_on = :waiting_on,
                    source_type = :source_type,
                    updated_at = now()
                where id = :id
                """
            ),
            {
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
                "is_primary": merged_is_primary,
                "waiting_on": merged_waiting_on,
                "source_type": merged_source_type,
            },
        )
        db.commit()
        return _row_to_dict(
            db.execute(
                text("select * from acquisition_contacts where id = :id"),
                {"id": int(existing["id"])},
            ).fetchone()
        ) or {}

    db.execute(
        text(
            """
            insert into acquisition_contacts (
                org_id,
                property_id,
                role,
                name,
                email,
                phone,
                company,
                notes,
                source_document_id,
                confidence,
                extraction_version,
                manually_overridden,
                is_primary,
                waiting_on,
                source_type,
                created_at,
                updated_at
            )
            values (
                :org_id,
                :property_id,
                :role,
                :name,
                :email,
                :phone,
                :company,
                :notes,
                :source_document_id,
                :confidence,
                :extraction_version,
                :manually_overridden,
                :is_primary,
                :waiting_on,
                :source_type,
                now(),
                now()
            )
            """
        ),
        {
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
            "is_primary": bool(is_primary) if is_primary is not None else False,
            "waiting_on": bool(waiting_on) if waiting_on is not None else False,
            "source_type": clean_source_type,
        },
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

    return list_participants(db, org_id=org_id, property_id=property_id)


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