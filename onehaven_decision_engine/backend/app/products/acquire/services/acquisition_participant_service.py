from __future__ import annotations

from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session


DOCUMENT_CONTACT_RULES: dict[str, dict[str, Any]] = {
    "purchase_agreement": {
        "target_roles": ["listing_agent", "buyer_agent", "seller_agent"],
        "fallback_roles": ["listing_office"],
        "why": {
            "listing_agent": "This contact usually controls listing-side offer coordination, access, counters, and contract routing.",
            "buyer_agent": "This contact usually owns buyer-side offer coordination, signatures, and deal positioning.",
            "seller_agent": "This contact usually controls seller-side communication, counters, and execution details.",
            "listing_office": "This brokerage office is the fallback when the individual agent is missing or unreachable.",
        },
    },
    "loan_estimate": {
        "target_roles": ["lender", "loan_officer"],
        "fallback_roles": [],
        "why": {
            "lender": "This contact is responsible for financing terms, disclosures, and underwriting progress.",
            "loan_officer": "This contact is responsible for explaining loan terms, collecting conditions, and moving financing forward.",
        },
    },
    "loan_documents": {
        "target_roles": ["lender", "loan_officer"],
        "fallback_roles": [],
        "why": {
            "lender": "This contact is responsible for financing terms, disclosures, and underwriting progress.",
            "loan_officer": "This contact is responsible for explaining loan terms, collecting conditions, and moving financing forward.",
        },
    },
    "insurance_binder": {
        "target_roles": ["insurance_agent", "insurance_agency"],
        "fallback_roles": [],
        "why": {
            "insurance_agent": "This contact is responsible for the binder, coverage details, and effective-date corrections.",
            "insurance_agency": "This agency is the fallback for binder delivery, declarations, and underwriting questions.",
        },
    },
    "inspection_report": {
        "target_roles": ["inspector", "inspection_company"],
        "fallback_roles": [],
        "why": {
            "inspector": "This contact can clarify findings, severity, repair implications, and reinspection timing.",
            "inspection_company": "This company is the fallback for scheduling, report delivery, and follow-up questions.",
        },
    },
    "title_documents": {
        "target_roles": ["title_company", "escrow_officer"],
        "fallback_roles": [],
        "why": {
            "title_company": "This contact handles title commitment issues, payoff coordination, and closing readiness.",
            "escrow_officer": "This contact handles escrow instructions, settlement coordination, and funds timing.",
        },
    },
    "closing_disclosure": {
        "target_roles": ["title_company", "escrow_officer"],
        "fallback_roles": [],
        "why": {
            "title_company": "This contact handles title-side settlement corrections and final closing coordination.",
            "escrow_officer": "This contact handles closing statement questions, funds, and signature logistics.",
        },
    },
}


ROLE_ALIASES: dict[str, str] = {
    "agent": "listing_agent",
    "listing agent": "listing_agent",
    "listing_agent": "listing_agent",
    "broker": "listing_agent",
    "realtor": "listing_agent",
    "seller agent": "seller_agent",
    "seller_agent": "seller_agent",
    "buyer agent": "buyer_agent",
    "buyers agent": "buyer_agent",
    "buyer_agent": "buyer_agent",
    "office": "listing_office",
    "brokerage": "listing_office",
    "listing_office": "listing_office",
    "title": "title_company",
    "title company": "title_company",
    "title_company": "title_company",
    "escrow": "escrow_officer",
    "escrow officer": "escrow_officer",
    "escrow_officer": "escrow_officer",
    "loan officer": "loan_officer",
    "loan_officer": "loan_officer",
    "lender": "lender",
    "mortgage company": "lender",
    "insurance agent": "insurance_agent",
    "insurance_agent": "insurance_agent",
    "insurance agency": "insurance_agency",
    "insurance_agency": "insurance_agency",
    "inspector": "inspector",
    "inspection company": "inspection_company",
    "inspection_company": "inspection_company",
}


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
    return ROLE_ALIASES.get(raw, raw.replace(" ", "_"))


def _label_for_role(role: str) -> str:
    normalized = _normalize_role(role)
    labels = {
        "listing_agent": "Listing agent",
        "buyer_agent": "Buyer agent",
        "seller_agent": "Seller agent",
        "listing_office": "Listing office",
        "lender": "Lender",
        "loan_officer": "Loan officer",
        "insurance_agent": "Insurance agent",
        "insurance_agency": "Insurance agency",
        "inspector": "Inspector",
        "inspection_company": "Inspection company",
        "title_company": "Title company",
        "escrow_officer": "Escrow officer",
    }
    return labels.get(normalized, normalized.replace("_", " ").title())


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


def _contact_completeness_score(row: dict[str, Any]) -> int:
    score = 0
    if _clean_text(row.get("phone")):
        score += 3
    if _clean_text(row.get("email")):
        score += 3
    if _clean_text(row.get("company")):
        score += 1
    if bool(row.get("waiting_on")):
        score += 3
    if bool(row.get("is_primary")):
        score += 2
    if str(row.get("source_type") or "").strip().lower() == "listing_import":
        score += 1
    return score


def _contact_sort_key(row: dict[str, Any], preferred_role_order: list[str]) -> tuple[int, int, int, int]:
    role = _normalize_role(str(row.get("role") or "other"))
    try:
        role_rank = preferred_role_order.index(role)
    except ValueError:
        role_rank = len(preferred_role_order) + 1
    return (
        role_rank,
        0 if bool(row.get("waiting_on")) else 1,
        0 if bool(row.get("is_primary")) else 1,
        -_contact_completeness_score(row),
    )


def _serialize_document_contact(
    row: dict[str, Any],
    *,
    document_kind: str,
    why_map: dict[str, str],
) -> dict[str, Any]:
    normalized_role = _normalize_role(str(row.get("role") or "other"))
    why_relevant = why_map.get(normalized_role) or f"This { _label_for_role(normalized_role).lower() } is relevant to {document_kind.replace('_', ' ')} follow-up."
    return {
        "id": row.get("id"),
        "role": normalized_role,
        "role_label": _label_for_role(normalized_role),
        "name": row.get("name"),
        "phone": row.get("phone"),
        "email": row.get("email"),
        "company": row.get("company"),
        "notes": row.get("notes"),
        "is_primary": bool(row.get("is_primary") or False),
        "waiting_on": bool(row.get("waiting_on") or False),
        "source_type": row.get("source_type"),
        "why_relevant": why_relevant,
    }


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


def resolve_document_contact_card(
    participants: list[dict[str, Any]] | None,
    document_kind: str | None,
) -> dict[str, Any]:
    kind = str(document_kind or "").strip().lower()
    rule = DOCUMENT_CONTACT_RULES.get(kind, {"target_roles": [], "fallback_roles": [], "why": {}})
    target_roles = [_normalize_role(role) for role in rule.get("target_roles", [])]
    fallback_roles = [_normalize_role(role) for role in rule.get("fallback_roles", [])]
    why_map = dict(rule.get("why") or {})

    participant_rows = list(participants or [])
    normalized_rows = [
        {**row, "role": _normalize_role(str(row.get("role") or "other"))}
        for row in participant_rows
    ]

    target_matches = [row for row in normalized_rows if row.get("role") in set(target_roles)]
    fallback_matches = [row for row in normalized_rows if row.get("role") in set(fallback_roles)]

    ordered_matches = sorted(target_matches, key=lambda row: _contact_sort_key(row, target_roles))
    ordered_fallbacks = sorted(fallback_matches, key=lambda row: _contact_sort_key(row, fallback_roles or target_roles))

    primary_row = ordered_matches[0] if ordered_matches else (ordered_fallbacks[0] if ordered_fallbacks else None)
    fallback_rows: list[dict[str, Any]] = []
    for row in ordered_matches[1:] + ordered_fallbacks:
        if primary_row and int(row.get("id") or 0) == int(primary_row.get("id") or 0):
            continue
        fallback_rows.append(row)

    present_roles = {_normalize_role(str(row.get("role") or "")) for row in ordered_matches}
    missing_roles = [_label_for_role(role) for role in target_roles if role not in present_roles]

    return {
        "document_kind": kind,
        "document_kind_label": kind.replace("_", " ").title() if kind else "Document",
        "target_roles": [_label_for_role(role) for role in target_roles],
        "primary_contact_for_document_kind": _serialize_document_contact(primary_row, document_kind=kind, why_map=why_map) if primary_row else None,
        "fallback_contacts_for_document_kind": [
            _serialize_document_contact(row, document_kind=kind, why_map=why_map)
            for row in fallback_rows[:4]
        ],
        "missing_contact_roles": missing_roles,
    }



def build_document_contact_directory(
    participants: list[dict[str, Any]] | None,
    document_kinds: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    kinds = document_kinds or list(DOCUMENT_CONTACT_RULES.keys())
    return {
        str(kind).strip().lower(): resolve_document_contact_card(participants, kind)
        for kind in kinds
        if str(kind).strip()
    }
