
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property


_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")


@dataclass(frozen=True)
class NormalizedPropertyIdentity:
    address: str
    normalized_address: str
    city: str
    state: str
    zip_code: str | None
    county: str | None
    parcel_id: str | None = None


def _clean_text(value: Any) -> str:
    return _WS_RE.sub(" ", str(value or "").strip())


def _norm_state(value: Any) -> str:
    text = _clean_text(value).upper()
    return text or "MI"


def _norm_lower_text(value: Any) -> str:
    text = _clean_text(value).lower()
    return text


def _norm_zip(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    m = re.search(r"(\d{5}(?:-\d{4})?)", text)
    return m.group(1) if m else text


def normalize_address_line(value: Any) -> str:
    text = _norm_lower_text(value)
    text = _NON_ALNUM_RE.sub(" ", text)
    replacements = {
        " street ": " st ",
        " avenue ": " ave ",
        " road ": " rd ",
        " drive ": " dr ",
        " boulevard ": " blvd ",
        " lane ": " ln ",
        " court ": " ct ",
        " place ": " pl ",
        " apartment ": " apt ",
        " unit ": " unit ",
    }
    text = f" {text} "
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = _WS_RE.sub(" ", text).strip()
    return text


def normalize_county(value: Any) -> str | None:
    text = _norm_lower_text(value)
    if not text:
        return None
    text = text.replace(" county", "").strip()
    return text or None


def build_normalized_property_identity(
    *,
    address: str,
    city: str,
    state: str,
    zip_code: str | None = None,
    county: str | None = None,
    parcel_id: str | None = None,
) -> NormalizedPropertyIdentity:
    address_clean = _clean_text(address)
    city_clean = _clean_text(city)
    state_clean = _norm_state(state)
    zip_clean = _norm_zip(zip_code)
    county_clean = normalize_county(county)
    normalized_address = " | ".join(
        part
        for part in [
            normalize_address_line(address_clean),
            _norm_lower_text(city_clean),
            state_clean.lower(),
            zip_clean or "",
        ]
        if part
    )
    return NormalizedPropertyIdentity(
        address=address_clean,
        normalized_address=normalized_address,
        city=city_clean,
        state=state_clean,
        zip_code=zip_clean,
        county=county_clean,
        parcel_id=_clean_text(parcel_id) or None,
    )


def match_existing_property(
    db: Session,
    *,
    org_id: int,
    normalized_address: str,
    parcel_id: str | None = None,
) -> Property | None:
    stmt = select(Property).where(
        Property.org_id == int(org_id),
        Property.normalized_address == normalized_address,
    )
    row = db.scalars(stmt.limit(1)).first()
    if row is not None:
        return row

    if parcel_id and hasattr(Property, "parcel_id"):
        stmt = select(Property).where(
            Property.org_id == int(org_id),
            Property.parcel_id == parcel_id,
        )
        return db.scalars(stmt.limit(1)).first()
    return None


def property_duplicate_match_summary(
    db: Session,
    *,
    org_id: int,
    address: str,
    city: str,
    state: str,
    zip_code: str | None = None,
    county: str | None = None,
    parcel_id: str | None = None,
) -> dict[str, Any]:
    identity = build_normalized_property_identity(
        address=address,
        city=city,
        state=state,
        zip_code=zip_code,
        county=county,
        parcel_id=parcel_id,
    )
    existing = match_existing_property(
        db,
        org_id=org_id,
        normalized_address=identity.normalized_address,
        parcel_id=identity.parcel_id,
    )
    return {
        "normalized_address": identity.normalized_address,
        "county": identity.county,
        "zip_code": identity.zip_code,
        "parcel_id": identity.parcel_id,
        "matched_property_id": int(existing.id) if existing is not None else None,
        "is_duplicate": existing is not None,
    }
