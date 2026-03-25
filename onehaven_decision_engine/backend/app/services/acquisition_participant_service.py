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


def list_participants(db: Session, *, org_id: int, property_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            select *
            from acquisition_contacts
            where org_id = :org_id and property_id = :property_id
            order by role asc, id asc
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchall()
    return [_row_to_dict(row) or {} for row in rows]


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
) -> dict[str, Any]:
    if not role.strip():
        raise HTTPException(status_code=422, detail="role is required.")
    if not name.strip():
        raise HTTPException(status_code=422, detail="name is required.")

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
            {"org_id": int(org_id), "property_id": int(property_id), "role": role.strip()},
        ).fetchone()
    )

    if existing:
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
                    updated_at = now()
                where id = :id
                """
            ),
            {
                "id": int(existing["id"]),
                "name": name.strip(),
                "email": (email or "").strip() or None,
                "phone": (phone or "").strip() or None,
                "company": (company or "").strip() or None,
                "notes": (notes or "").strip() or None,
                "source_document_id": source_document_id,
                "confidence": confidence,
                "extraction_version": extraction_version,
                "manually_overridden": bool(manually_overridden),
            },
        )
        db.commit()
        return _row_to_dict(
            db.execute(text("select * from acquisition_contacts where id = :id"), {"id": int(existing["id"])}).fetchone()
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
                now(),
                now()
            )
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "role": role.strip(),
            "name": name.strip(),
            "email": (email or "").strip() or None,
            "phone": (phone or "").strip() or None,
            "company": (company or "").strip() or None,
            "notes": (notes or "").strip() or None,
            "source_document_id": source_document_id,
            "confidence": confidence,
            "extraction_version": extraction_version,
            "manually_overridden": bool(manually_overridden),
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
            {"org_id": int(org_id), "property_id": int(property_id), "role": role.strip()},
        ).fetchone()
    ) or {}


def delete_participant(db: Session, *, org_id: int, property_id: int, participant_id: int) -> dict[str, Any]:
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
