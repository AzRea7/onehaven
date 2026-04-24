
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class PolicyCatalogItem:
    url: str
    state: Optional[str] = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None
    publisher: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None
    source_kind: Optional[str] = None
    is_authoritative: bool = True
    priority: int = 100


OFFICIAL_HOST_SUFFIXES = (
    ".gov",
    ".gov.us",
    ".mi.gov",
    ".legislature.mi.gov",
    ".courts.michigan.gov",
    ".michigan.gov",
    ".hud.gov",
    ".ecfr.gov",
    ".federalregister.gov",
)

OFFICIAL_HOST_EXACT = {
    "ecfr.gov",
    "www.ecfr.gov",
    "federalregister.gov",
    "www.federalregister.gov",
    "hud.gov",
    "www.hud.gov",
    "michigan.gov",
    "www.michigan.gov",
    "legislature.mi.gov",
    "www.legislature.mi.gov",
    "courts.michigan.gov",
    "www.courts.michigan.gov",
}


def _host_from_url(url: str) -> str:
    raw = str(url or "").strip().lower()
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    raw = raw.split("/", 1)[0].strip()
    if ":" in raw:
        raw = raw.split(":", 1)[0].strip()
    return raw


def _is_official_catalog_host(url: str) -> bool:
    host = _host_from_url(url)
    if not host:
        return False
    if host in OFFICIAL_HOST_EXACT:
        return True
    if host.endswith(".gov"):
        return True
    if host.endswith(".mi.us"):
        return True
    if host.endswith(".us") and ".gov" in host:
        return True
    return any(host.endswith(suffix) for suffix in OFFICIAL_HOST_SUFFIXES)


def _is_official_catalog_item(item: PolicyCatalogItem) -> bool:
    source_kind = str(item.source_kind or "").strip().lower()
    publisher = str(item.publisher or "").strip().lower()

    if _is_official_catalog_host(item.url):
        return True

    if source_kind in {
        "federal_anchor",
        "state_anchor",
        "municipal_code",
        "municipal_registration",
        "municipal_inspection",
        "municipal_program_page",
        "municipal_certificate",
        "municipal_enforcement",
        "municipal_guidance",
        "municipal_building_anchor",
        "municipal_ordinance",
        "city_program_page",
        "county_program_page",
        "state_program_page",
        "state_hcv_anchor",
        "housing_authority",
        "pha_plan",
        "pha_guidance",
        "pha_notice",
    }:
        return True

    if "housing authority" in publisher or "housing commission" in publisher:
        return True

    return False


def _norm_state(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip().upper()
    return s or None


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip().lower()
    return s or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    return s or None


def _dedupe(items: list[PolicyCatalogItem]) -> list[PolicyCatalogItem]:
    seen: set[str] = set()
    out: list[PolicyCatalogItem] = []
    for item in items:
        key = item.url.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            PolicyCatalogItem(
                url=item.url.strip(),
                state=_norm_state(item.state),
                county=_norm_lower(item.county),
                city=_norm_lower(item.city),
                pha_name=_norm_text(item.pha_name),
                program_type=_norm_text(item.program_type),
                publisher=_norm_text(item.publisher),
                title=_norm_text(item.title),
                notes=_norm_text(item.notes),
                source_kind=_norm_text(item.source_kind),
                is_authoritative=bool(item.is_authoritative),
                priority=int(item.priority or 100),
            )
        )
    return out


def _sorted_then_deduped(items: list[PolicyCatalogItem]) -> list[PolicyCatalogItem]:
    ordered = sorted(
        items,
        key=lambda x: (
            int(x.priority or 100),
            (x.city or ""),
            (x.county or ""),
            x.url.strip(),
        ),
    )
    return _dedupe(ordered)


def filter_official_catalog_items(items: list[PolicyCatalogItem]) -> list[PolicyCatalogItem]:
    return _sorted_then_deduped([item for item in items if _is_official_catalog_item(item)])


def catalog_municipalities(items: list[PolicyCatalogItem]) -> list[dict[str, Optional[str]]]:
    seen: set[tuple[str, Optional[str], Optional[str]]] = set()
    out: list[dict[str, Optional[str]]] = []
    for item in items:
        if not item.city:
            continue
        key = ((_norm_state(item.state) or "MI"), _norm_lower(item.county), _norm_lower(item.city))
        if key in seen:
            continue
        seen.add(key)
        out.append({"state": key[0], "county": key[1], "city": key[2]})
    out.sort(key=lambda x: ((x["county"] or ""), (x["city"] or "")))
    return out


def supported_focuses() -> list[str]:
    return [
        "se_mi",
        "se_mi_extended",
        "all_verified_core",
        "wayne_county_core",
        "oakland_county_core",
        "macomb_county_core",
        *sorted(_city_pack_registry().keys()),
    ]


def supported_markets() -> list[dict[str, Optional[str]]]:
    return catalog_municipalities(catalog_mi_authoritative("se_mi_extended"))


def _federal_and_state_baseline() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.ecfr.gov/current/title-24/subtitle-B/chapter-IX/part-982",
            state="MI",
            program_type="hcv",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 982 — Section 8 Tenant-Based Assistance: Housing Choice Voucher Program",
            notes="Canonical HCV regulations. Primary federal anchor. category_hints=program_overlay,section8",
            source_kind="federal_anchor",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.ecfr.gov/current/title-24/subtitle-A/part-5",
            state="MI",
            program_type="hcv",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 5 — General HUD Program Requirements",
            notes="Program-wide HUD requirements. Includes current NSPIRE structure. category_hints=program_overlay,inspection,lead,section8",
            source_kind="federal_anchor",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.ecfr.gov/current/title-24/subtitle-A/part-5/subpart-G",
            state="MI",
            program_type="hcv",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 5 Subpart G — Physical Inspection of Real Estate",
            notes="Current NSPIRE inspection framework anchor. category_hints=inspection,lead",
            source_kind="federal_anchor",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.federalregister.gov/documents/2023/06/22/2023-13293/national-standards-for-the-physical-inspection-of-real-estate-inspection-standards",
            state="MI",
            program_type="hcv",
            publisher="Federal Register",
            title="National Standards for the Physical Inspection of Real Estate (NSPIRE) Inspection Standards",
            notes="Federal Register standards notice for NSPIRE. category_hints=inspection,lead",
            source_kind="federal_anchor",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.federalregister.gov/documents/2025/09/30/2025-19070/economic-growth-regulatory-relief-and-consumer-protection-act-implementation-of-national-standards",
            state="MI",
            program_type="hcv",
            publisher="Federal Register",
            title="Extension of NSPIRE Compliance Date for HCV, PBV and Mod Rehab (2025 notice)",
            notes="Current later federal timing notice. category_hints=inspection,program_overlay,section8",
            source_kind="federal_anchor",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.hud.gov/program_offices/public_indian_housing/programs/hcv",
            state="MI",
            program_type="hcv",
            publisher="HUD",
            title="HUD Housing Choice Voucher Program",
            notes="Program guidance hub. Use alongside CFR, not instead of CFR. category_hints=program_overlay,section8,contacts",
            source_kind="federal_anchor",
            priority=30,
        ),
        PolicyCatalogItem(
            url="https://www.legislature.mi.gov/Laws/Index?ObjectName=mcl-Act-348-of-1972",
            state="MI",
            publisher="Michigan Legislature",
            title="Michigan Landlord-Tenant Relationships Act (Act 348 of 1972)",
            notes="Primary state landlord-tenant act anchor. category_hints=lead,source_of_income,permits,safety",
            source_kind="state_anchor",
            priority=40,
        ),
        PolicyCatalogItem(
            url="https://www.legislature.mi.gov/Laws/MCL?objectName=mcl-554-602",
            state="MI",
            publisher="Michigan Legislature",
            title="MCL 554.602 (Security Deposit Act section)",
            notes="State deposit/compliance anchor. category_hints=documents,fees,safety",
            source_kind="state_anchor",
            priority=40,
        ),
        PolicyCatalogItem(
            url="https://www.legislature.mi.gov/Laws/Index?ObjectName=mcl-chap554",
            state="MI",
            publisher="Michigan Legislature",
            title="Michigan Compiled Laws Chapter 554",
            notes="Broader landlord-tenant baseline for Michigan. category_hints=safety,source_of_income",
            source_kind="state_anchor",
            priority=50,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda",
            state="MI",
            publisher="MSHDA",
            title="Michigan State Housing Development Authority",
            notes="State housing authority anchor and statewide housing program hub. category_hints=program_overlay,documents,contacts,fees",
            source_kind="state_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/housing-choice-voucher",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="Housing Choice Voucher Program (HCV)",
            notes="Statewide HCV program overview. category_hints=program_overlay,section8",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/hcv-landlords",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="HCV Landlords - MSHDA",
            notes="Statewide landlord hub for HCV operations, forms, inspection notices, and payment info. category_hints=program_overlay,fees,documents,section8",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/hcv-landlords/housing-choice-voucher-landlords",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="Housing Choice Voucher Landlords",
            notes="Operational landlord requirements, HAP contract and tenancy addendum guidance. category_hints=program_overlay,section8",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/hcv-landlords/housing-choice-vouchers-landlords---payment-information",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="Housing Choice Voucher Landlords - Payment Information",
            notes="Operational payment timing/landlord payment process page. category_hints=fees,program_overlay,documents,section8",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/-/media/Project/Websites/mshda/rental/assets/Shared-HCV---PBV/2026-HAP-UAP-Schedule.pdf",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="2026 HAP/UAP Payment Schedule",
            notes="Useful for operational cash timing; not law, but relevant for landlord ops. category_hints=fees,program_overlay",
            source_kind="state_hcv_anchor",
            priority=70,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/-/media/Project/Websites/mshda/rental/assets/PHA-plan/Proposed-Annual-Plan-26-27/MSHDA-Annual-PHA-Plan-2026-27.pdf",
            state="MI",
            program_type="hcv",
            pha_name="MSHDA",
            publisher="MSHDA",
            title="MSHDA Annual PHA Plan 2026-27",
            notes="Statewide PHA operational source for program behavior and landlord-facing operations. category_hints=program_overlay,contacts,documents",
            source_kind="pha_plan",
            priority=70,
        ),
    ]


def _detroit_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property",
            state="MI", county="wayne", city="detroit",
            publisher="City of Detroit BSEED",
            title="Tenant Rental Property",
            notes="Detroit rental property hub. Owners are required to register and complete inspection flow. category_hints=registration,inspection,occupancy",
            source_kind="municipal_registration", priority=10,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property/landlord-rental",
            state="MI", county="wayne", city="detroit",
            publisher="City of Detroit BSEED",
            title="Landlord Rental Requirements",
            notes="Current city rental registration/certification workflow page. category_hints=registration,inspection,occupancy,documents",
            source_kind="municipal_registration", priority=10,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property/rental-certificate",
            state="MI", county="wayne", city="detroit",
            publisher="City of Detroit BSEED",
            title="Rental Certificate of Compliance",
            notes="Certificate of compliance process/inspection workflow anchor. category_hints=occupancy,inspection,registration",
            source_kind="municipal_certificate", priority=10,
        ),
    ]


def _dearborn_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://dearborn.gov/residents/home-property/rental-property-information",
            state="MI", county="wayne", city="dearborn",
            publisher="City of Dearborn",
            title="Rental Property Information",
            notes="Official Dearborn rental property inspection/reporting hub. category_hints=registration,rental_license,documents,contacts,fees",
            source_kind="municipal_registration", priority=10,
        ),
        PolicyCatalogItem(
            url="https://dearborn.gov/residents/home-property/rental-property-information/registering-rental-property",
            state="MI", county="wayne", city="dearborn",
            publisher="City of Dearborn",
            title="Registering a Rental Property",
            notes="Official Dearborn registration flow; certificate of occupancy and inspection requirements. category_hints=registration,rental_license,documents,fees,inspection,occupancy",
            source_kind="municipal_registration", priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.dearborn.gov/index.php/step-1-submit-application-payment-0",
            state="MI", county="wayne", city="dearborn",
            publisher="City of Dearborn",
            title="Step 1: Submit an application with payment",
            notes="Operational application/payment step for requesting a rental home inspection. category_hints=fees,documents,permits,registration",
            source_kind="municipal_inspection", priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.dearborn.gov/index.php/step-6-certificate-occupancy-0",
            state="MI", county="wayne", city="dearborn",
            publisher="City of Dearborn",
            title="Step 6: Certificate of Occupancy",
            notes="Certificate issuance timing, validity period, and renewal cycle anchor. category_hints=occupancy,inspection,rental_license",
            source_kind="municipal_certificate", priority=10,
        ),
        PolicyCatalogItem(
            url="https://dearborn.gov/how-submit-re-occupancy",
            state="MI", county="wayne", city="dearborn",
            publisher="City of Dearborn",
            title="How to Submit for Re-Occupancy",
            notes="Reinspection/correction workflow page that clarifies post-inspection correction requirements. category_hints=inspection,occupancy,documents,contacts",
            source_kind="municipal_inspection", priority=20,
        ),
        PolicyCatalogItem(
            url="https://dearborn.gov/residents/government/city-departments",
            state="MI", county="wayne", city="dearborn",
            publisher="City of Dearborn",
            title="City Departments",
            notes="Department directory for city offices relevant to rental compliance routing. category_hints=contacts",
            source_kind="municipal_guidance", priority=25,
        ),
    ]


def _warren_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/departments/rental-inspections-division/",
            state="MI", county="macomb", city="warren",
            publisher="City of Warren",
            title="Rental Inspections Division",
            notes="Official Warren rental inspections division page. category_hints=inspection,registration",
            source_kind="municipal_inspection", priority=10,
        ),
    ]


def _southfield_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/departments/building-department/building-southfield/rental-housing",
            state="MI", county="oakland", city="southfield",
            publisher="City of Southfield",
            title="Rental Housing",
            notes="Official Southfield rental registration/inspection workflow page. category_hints=registration,inspection,occupancy",
            source_kind="municipal_registration", priority=10,
        ),
    ]


def _pontiac_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.pontiac.mi.us/departments/community_development/property_rentals.php",
            state="MI", county="oakland", city="pontiac",
            publisher="City of Pontiac",
            title="Property Rentals",
            notes="Official Pontiac rental property hub; states rentals must be registered and up to code. category_hints=registration,inspection",
            source_kind="municipal_registration", priority=10,
        ),
    ]


def _livonia_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://livonia.gov/241/Inspection-Building-Enforcement",
            state="MI", county="wayne", city="livonia",
            publisher="City of Livonia",
            title="Inspection (Building & Enforcement)",
            notes="Official Livonia inspection department page. category_hints=inspection,contacts,permits",
            source_kind="municipal_inspection", priority=10,
        ),
    ]


def _westland_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityofwestland.com/203/Residential-Rental-Program",
            state="MI", county="wayne", city="westland",
            publisher="City of Westland",
            title="Residential Rental Program",
            notes="Official Westland rental registration/inspection/certification page. category_hints=registration,inspection,occupancy",
            source_kind="municipal_registration", priority=10,
        ),
    ]


def _taylor_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityoftaylor.com/236/Rental-Department",
            state="MI", county="wayne", city="taylor",
            publisher="City of Taylor",
            title="Rental Department",
            notes="Official Taylor rental inspections and registration page. category_hints=registration,inspection,contacts",
            source_kind="municipal_registration", priority=10,
        ),
    ]


def _city_pack_registry() -> dict[str, Callable[[], list[PolicyCatalogItem]]]:
    return {
        "dearborn": _dearborn_pack,
        "detroit": _detroit_pack,
        "livonia": _livonia_pack,
        "pontiac": _pontiac_pack,
        "southfield": _southfield_pack,
        "taylor": _taylor_pack,
        "warren": _warren_pack,
        "westland": _westland_pack,
    }


def _focus_county_sets() -> dict[str, list[str]]:
    return {
        "wayne_county_core": ["detroit", "dearborn", "livonia", "westland", "taylor"],
        "oakland_county_core": ["southfield", "pontiac"],
        "macomb_county_core": ["warren"],
    }


def _expand_focuses(focus: str) -> list[str]:
    f = (focus or "se_mi_extended").strip().lower()
    city_registry = _city_pack_registry()
    county_sets = _focus_county_sets()

    if f in county_sets:
        return county_sets[f]
    if f in {"all_verified_core", "se_mi_extended"}:
        return ["detroit", "dearborn", "warren", "southfield", "pontiac", "livonia", "westland", "taylor"]
    if f == "se_mi":
        return ["detroit"]
    if f in city_registry:
        return [f]
    return []


def catalog_mi_authoritative(focus: str = "se_mi_extended") -> list[PolicyCatalogItem]:
    items = _federal_and_state_baseline()
    for name in _expand_focuses(focus):
        builder = _city_pack_registry().get(name)
        if builder:
            items.extend(builder())
    return _sorted_then_deduped(items)


# === dataset-first / control-plane helpers retained ===

_SOURCE_KIND_TO_FAMILY: dict[str, str] = {
    "federal_anchor": "legal_primary",
    "state_anchor": "legal_primary",
    "municipal_code": "legal_primary",
    "municipal_registration": "municipal_operations",
    "municipal_inspection": "municipal_operations",
    "municipal_certificate": "municipal_operations",
    "municipal_enforcement": "municipal_operations",
    "municipal_guidance": "municipal_operations",
    "municipal_building_anchor": "municipal_operations",
    "municipal_ordinance": "legal_primary",
    "city_program_page": "municipal_operations",
    "county_program_page": "municipal_operations",
    "state_program_page": "state_program",
    "state_hcv_anchor": "state_program",
    "housing_authority": "program_admin",
    "pha_plan": "program_admin",
    "pha_guidance": "program_admin",
    "pha_notice": "program_admin",
}

_SOURCE_FAMILY_PRIORITY: dict[str, int] = {
    "legal_primary": 10,
    "state_program": 20,
    "municipal_operations": 30,
    "program_admin": 40,
    "supporting_guidance": 50,
    "unknown": 90,
}


def policy_catalog_source_family(item: PolicyCatalogItem) -> str:
    source_kind = str(getattr(item, "source_kind", None) or "").strip().lower()
    if source_kind in _SOURCE_KIND_TO_FAMILY:
        return _SOURCE_KIND_TO_FAMILY[source_kind]
    if getattr(item, "is_authoritative", False):
        return "legal_primary"
    title = str(getattr(item, "title", None) or "").lower()
    if "guide" in title or "faq" in title:
        return "supporting_guidance"
    return "unknown"


def policy_catalog_truth_priority(item: PolicyCatalogItem) -> int:
    family = policy_catalog_source_family(item)
    item_priority = int(getattr(item, "priority", 100) or 100)
    return (_SOURCE_FAMILY_PRIORITY.get(family, 90) * 1000) + item_priority


def policy_catalog_truth_row(item: PolicyCatalogItem) -> dict[str, Any]:
    family = policy_catalog_source_family(item)
    host = _host_from_url(getattr(item, "url", "") or "")
    return {
        "url": item.url,
        "state": _norm_state(item.state) or "MI",
        "county": _norm_lower(item.county),
        "city": _norm_lower(item.city),
        "pha_name": _norm_text(item.pha_name),
        "program_type": _norm_text(item.program_type),
        "publisher": _norm_text(item.publisher),
        "title": _norm_text(item.title),
        "notes": _norm_text(item.notes),
        "source_kind": _norm_text(item.source_kind),
        "source_family": family,
        "truth_priority": policy_catalog_truth_priority(item),
        "is_authoritative": bool(item.is_authoritative),
        "official_host": _is_official_catalog_host(item.url),
        "host": host,
        "truth_model": "dataset_first_catalog",
        "service_role": "coverage_scaffold",
    }


def policy_catalog_dataset_for_market(
    *,
    state: str = "MI",
    county: Optional[str] = None,
    city: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[dict[str, Any]]:
    items = catalog_for_market(state=state, county=county, city=city, focus=focus)
    rows = [policy_catalog_truth_row(item) for item in items]
    rows.sort(key=lambda row: (int(row.get("truth_priority") or 0), str(row.get("title") or ""), str(row.get("url") or "")))
    return rows


def policy_catalog_summary(
    *,
    state: str = "MI",
    county: Optional[str] = None,
    city: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    rows = policy_catalog_dataset_for_market(state=state, county=county, city=city, focus=focus)
    family_counts: dict[str, int] = {}
    kind_counts: dict[str, int] = {}
    authoritative_count = 0
    for row in rows:
        family = str(row.get("source_family") or "unknown")
        kind = str(row.get("source_kind") or "unknown")
        family_counts[family] = family_counts.get(family, 0) + 1
        kind_counts[kind] = kind_counts.get(kind, 0) + 1
        if bool(row.get("is_authoritative")):
            authoritative_count += 1
    return {
        "truth_model": "dataset_first_catalog",
        "service_role": "coverage_scaffold",
        "state": _norm_state(state) or "MI",
        "county": _norm_lower(county),
        "city": _norm_lower(city),
        "focus": focus,
        "item_count": len(rows),
        "authoritative_count": authoritative_count,
        "source_family_counts": family_counts,
        "source_kind_counts": kind_counts,
        "items": rows,
    }


# === integrated category enrichment helpers ===

def _parse_category_hints(notes: Optional[str]) -> list[str]:
    raw = str(notes or "").strip()
    if not raw:
        return []
    for part in raw.split("|"):
        piece = part.strip()
        if piece.lower().startswith("category_hints="):
            hints = piece.split("=", 1)[1]
            return sorted({h.strip().lower() for h in hints.split(",") if h.strip()})
    return []


def _hint_categories_for_text(item: PolicyCatalogItem) -> list[str]:
    text = " ".join(
        [
            str(getattr(item, "title", "") or ""),
            str(getattr(item, "publisher", "") or ""),
            str(getattr(item, "url", "") or ""),
            str(getattr(item, "notes", "") or ""),
        ]
    ).lower()
    hints = set(_parse_category_hints(getattr(item, "notes", None)))
    if any(x in text for x in ["registering a rental property", "rental property information", "license", "registration"]):
        hints.update(["registration", "rental_license"])
    if any(x in text for x in ["application", "submit", "packet", "document"]):
        hints.add("documents")
    if any(x in text for x in ["fee", "payment"]):
        hints.add("fees")
    if any(x in text for x in ["office", "department", "division", "contact"]):
        hints.add("contacts")
    if any(x in text for x in ["permit", "building permit"]):
        hints.add("permits")
    if any(x in text for x in ["source of income", "fair housing", "discrimination"]):
        hints.add("source_of_income")
    if any(x in text for x in ["voucher", "hcv", "hap", "nspire", "pha plan"]):
        hints.add("program_overlay")
    if any(x in text for x in ["lead", "lbp", "lead-safe"]):
        hints.add("lead")
    if any(x in text for x in ["certificate", "occupancy", "re-occupancy"]):
        hints.add("occupancy")
    if "inspection" in text:
        hints.add("inspection")
    return sorted(hints)


def _notes_with_hints(item: PolicyCatalogItem) -> Optional[str]:
    base = str(getattr(item, "notes", "") or "").strip()
    hints = _hint_categories_for_text(item)
    parts = [p.strip() for p in base.split("|") if p.strip() and not p.strip().lower().startswith("category_hints=")]
    if hints:
        parts.append("category_hints=" + ",".join(hints))
    return " | ".join(parts) if parts else None


def _enrich_market_item(item: PolicyCatalogItem) -> PolicyCatalogItem:
    return PolicyCatalogItem(
        url=item.url,
        state=_norm_state(item.state),
        county=_norm_lower(item.county),
        city=_norm_lower(item.city),
        pha_name=_norm_text(item.pha_name),
        program_type=_norm_text(item.program_type),
        publisher=_norm_text(item.publisher),
        title=_norm_text(item.title),
        notes=_notes_with_hints(item),
        source_kind=_norm_text(item.source_kind),
        is_authoritative=bool(item.is_authoritative),
        priority=int(item.priority or 100),
    )


def catalog_for_market(
    *,
    state: str = "MI",
    county: Optional[str] = None,
    city: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicyCatalogItem]:
    st = _norm_state(state) or "MI"
    cnty = _norm_lower(county)
    cty = _norm_lower(city)

    items = filter_official_catalog_items(catalog_mi_authoritative(focus=focus))
    out: list[PolicyCatalogItem] = []

    for item in items:
        if (item.state or "MI") != st:
            continue

        if item.city is None and item.county is None:
            out.append(_enrich_market_item(item))
            continue

        if item.city is None and item.county is not None:
            if cnty and item.county == cnty:
                out.append(_enrich_market_item(item))
            continue

        if item.city is not None:
            if cty and item.city == cty:
                out.append(_enrich_market_item(item))
            continue

    return _sorted_then_deduped(filter_official_catalog_items(out))
