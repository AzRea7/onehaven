from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


# Canonical normalized categories used by completeness + coverage services.
# Keep these stable because they become persisted values in DB rows.
CATEGORY_RENTAL_LICENSE = "rental_license"
CATEGORY_INSPECTION = "inspection"
CATEGORY_SECTION8 = "section8"
CATEGORY_ZONING = "zoning"
CATEGORY_TAX = "tax"
CATEGORY_UTILITIES = "utilities"
CATEGORY_SAFETY = "safety"
CATEGORY_REGISTRATION = "registration"
CATEGORY_LEAD = "lead"
CATEGORY_OCCUPANCY = "occupancy"
CATEGORY_PERMITS = "permits"
CATEGORY_PROGRAM_OVERLAY = "program_overlay"
CATEGORY_DOCUMENTS = "documents"
CATEGORY_FEES = "fees"
CATEGORY_CONTACTS = "contacts"

CANONICAL_JURISDICTION_CATEGORIES: tuple[str, ...] = (
    CATEGORY_RENTAL_LICENSE,
    CATEGORY_INSPECTION,
    CATEGORY_SECTION8,
    CATEGORY_ZONING,
    CATEGORY_TAX,
    CATEGORY_UTILITIES,
    CATEGORY_SAFETY,
    CATEGORY_REGISTRATION,
    CATEGORY_LEAD,
    CATEGORY_OCCUPANCY,
    CATEGORY_PERMITS,
    CATEGORY_PROGRAM_OVERLAY,
    CATEGORY_DOCUMENTS,
    CATEGORY_FEES,
    CATEGORY_CONTACTS,
)

# Loose aliases so policy extraction / legacy defaults can map into one stable set.
_CATEGORY_ALIASES: dict[str, str] = {
    "license": CATEGORY_RENTAL_LICENSE,
    "licenses": CATEGORY_RENTAL_LICENSE,
    "rental": CATEGORY_RENTAL_LICENSE,
    "rental_license": CATEGORY_RENTAL_LICENSE,
    "rental_licensing": CATEGORY_RENTAL_LICENSE,
    "licensing": CATEGORY_RENTAL_LICENSE,
    "registration": CATEGORY_REGISTRATION,
    "rental_registration": CATEGORY_REGISTRATION,
    "registry": CATEGORY_REGISTRATION,
    "inspection": CATEGORY_INSPECTION,
    "inspections": CATEGORY_INSPECTION,
    "inspection_frequency": CATEGORY_INSPECTION,
    "section8": CATEGORY_SECTION8,
    "section_8": CATEGORY_SECTION8,
    "hqs": CATEGORY_SECTION8,
    "voucher": CATEGORY_SECTION8,
    "housing_choice_voucher": CATEGORY_SECTION8,
    "pha": CATEGORY_PROGRAM_OVERLAY,
    "housing_authority": CATEGORY_PROGRAM_OVERLAY,
    "overlay": CATEGORY_PROGRAM_OVERLAY,
    "program_overlay": CATEGORY_PROGRAM_OVERLAY,
    "documents": CATEGORY_DOCUMENTS,
    "document": CATEGORY_DOCUMENTS,
    "paperwork": CATEGORY_DOCUMENTS,
    "fees": CATEGORY_FEES,
    "fee": CATEGORY_FEES,
    "contacts": CATEGORY_CONTACTS,
    "contact": CATEGORY_CONTACTS,
    "local_contact": CATEGORY_CONTACTS,
    "zoning": CATEGORY_ZONING,
    "land_use": CATEGORY_ZONING,
    "tax": CATEGORY_TAX,
    "taxes": CATEGORY_TAX,
    "property_tax": CATEGORY_TAX,
    "utilities": CATEGORY_UTILITIES,
    "utility": CATEGORY_UTILITIES,
    "water": CATEGORY_UTILITIES,
    "sewer": CATEGORY_UTILITIES,
    "safety": CATEGORY_SAFETY,
    "code_safety": CATEGORY_SAFETY,
    "fire_safety": CATEGORY_SAFETY,
    "lead": CATEGORY_LEAD,
    "lead_paint": CATEGORY_LEAD,
    "occupancy": CATEGORY_OCCUPANCY,
    "occupancy_limit": CATEGORY_OCCUPANCY,
    "certificate_of_occupancy": CATEGORY_OCCUPANCY,
    "certificate": CATEGORY_OCCUPANCY,
    "permits": CATEGORY_PERMITS,
    "permit": CATEGORY_PERMITS,
    "building_permit": CATEGORY_PERMITS,
}

# Minimum operational baseline categories.
_BASE_REQUIRED_CATEGORIES: tuple[str, ...] = (
    CATEGORY_INSPECTION,
    CATEGORY_SAFETY,
)

# Typical city-level rental operations.
_CITY_REQUIRED_CATEGORIES: tuple[str, ...] = (
    CATEGORY_RENTAL_LICENSE,
    CATEGORY_REGISTRATION,
    CATEGORY_DOCUMENTS,
    CATEGORY_FEES,
)

# Voucher / housing authority overlay.
_SECTION8_REQUIRED_CATEGORIES: tuple[str, ...] = (
    CATEGORY_SECTION8,
    CATEGORY_PROGRAM_OVERLAY,
    CATEGORY_CONTACTS,
)

# Older housing / older city stock heuristics.
_LEAD_FOCUSED_CATEGORIES: tuple[str, ...] = (
    CATEGORY_LEAD,
)


@dataclass(frozen=True)
class JurisdictionCategoryCoverage:
    required_categories: list[str]
    covered_categories: list[str]
    missing_categories: list[str]
    completeness_score: float
    completeness_status: str
    coverage_confidence: str


def normalize_category(value: Any) -> str | None:
    if value is None:
        return None

    raw = str(value).strip().lower()
    if not raw:
        return None

    raw = raw.replace("-", "_").replace(" ", "_").replace("/", "_")
    while "__" in raw:
        raw = raw.replace("__", "_")

    if raw in CANONICAL_JURISDICTION_CATEGORIES:
        return raw

    return _CATEGORY_ALIASES.get(raw)


def normalize_categories(values: Iterable[Any] | None) -> list[str]:
    if not values:
        return []

    seen: set[str] = set()
    ordered: list[str] = []

    for value in values:
        normalized = normalize_category(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)

    return ordered


def is_known_category(value: Any) -> bool:
    return normalize_category(value) is not None


def get_required_categories(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    rental_license_required: bool | None = None,
    inspection_authority: str | None = None,
    inspection_frequency: str | None = None,
    tenant_waitlist_depth: str | None = None,
    include_section8: bool = True,
    include_documents: bool = True,
    include_fees: bool = True,
) -> list[str]:
    """
    Deterministic required-category baseline.

    This is intentionally operational, not legal truth.
    It gives the completeness engine a stable minimum category set.
    """
    required: list[str] = list(_BASE_REQUIRED_CATEGORIES)

    normalized_city = (city or "").strip().lower()
    normalized_state = (state or "").strip().upper()
    normalized_county = (county or "").strip().lower()
    normalized_waitlist = (tenant_waitlist_depth or "").strip().lower()
    normalized_freq = (inspection_frequency or "").strip().lower()

    has_city_scope = bool(normalized_city)
    has_mi_scope = normalized_state == "MI" or not normalized_state

    if has_city_scope and has_mi_scope:
        required.extend(_CITY_REQUIRED_CATEGORIES)

    if bool(rental_license_required):
        required.append(CATEGORY_RENTAL_LICENSE)
        required.append(CATEGORY_REGISTRATION)

    if inspection_authority or normalized_freq in {"annual", "biennial", "periodic", "complaint"}:
        required.append(CATEGORY_INSPECTION)

    if include_section8 and (
        normalized_waitlist in {"medium", "high", "very_high"}
        or normalized_city in {
            "detroit",
            "pontiac",
            "inkster",
            "warren",
            "southfield",
            "dearborn",
            "royal oak",
            "hamtramck",
            "highland park",
        }
        or normalized_county in {"wayne", "oakland", "macomb"}
    ):
        required.extend(_SECTION8_REQUIRED_CATEGORIES)

    # Michigan older-housing operational assumption:
    # lead and permit friction matters more in common SE Michigan rental cities.
    if has_mi_scope and normalized_city in {
        "detroit",
        "pontiac",
        "inkster",
        "hamtramck",
        "highland park",
        "river rouge",
        "ecorse",
    }:
        required.extend(_LEAD_FOCUSED_CATEGORIES)
        required.append(CATEGORY_PERMITS)

    if include_documents:
        required.append(CATEGORY_DOCUMENTS)

    if include_fees:
        required.append(CATEGORY_FEES)

    return normalize_categories(required)


def compute_confidence_from_missing(
    required_categories: Iterable[Any] | None,
    covered_categories: Iterable[Any] | None,
) -> str:
    required = normalize_categories(required_categories)
    covered = normalize_categories(covered_categories)

    if not required:
        return "high"

    matched = len(set(required).intersection(set(covered)))
    ratio = matched / float(len(set(required)))

    if ratio >= 0.9:
        return "high"
    if ratio >= 0.5:
        return "medium"
    return "low"


def compute_completeness_score(
    required_categories: Iterable[Any] | None,
    covered_categories: Iterable[Any] | None,
) -> JurisdictionCategoryCoverage:
    required = normalize_categories(required_categories)
    covered = normalize_categories(covered_categories)

    required_set = set(required)
    covered_set = set(covered)

    missing = [category for category in required if category not in covered_set]

    if not required:
        score = 1.0
        status = "complete"
    else:
        matched = len(required_set.intersection(covered_set))
        score = matched / float(len(required_set))

        if score >= 0.999:
            status = "complete"
        elif score > 0.0:
            status = "partial"
        else:
            status = "missing"

    confidence = compute_confidence_from_missing(required, covered)

    return JurisdictionCategoryCoverage(
        required_categories=required,
        covered_categories=covered,
        missing_categories=missing,
        completeness_score=float(round(score, 6)),
        completeness_status=status,
        coverage_confidence=confidence,
    )