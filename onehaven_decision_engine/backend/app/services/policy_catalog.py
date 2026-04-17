from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


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
        "city_program_page",
        "county_program_page",
        "state_program_page",
        "housing_authority",
        "pha_plan",
        "pha_guidance",
    }:
        return True

    if "housing authority" in publisher or "housing commission" in publisher:
        return True

    return False


def filter_official_catalog_items(items: list[PolicyCatalogItem]) -> list[PolicyCatalogItem]:
    return _sorted_then_deduped([item for item in items if _is_official_catalog_item(item)])

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
        key = item.url.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            PolicyCatalogItem(
                url=key,
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


def catalog_municipalities(
    items: list[PolicyCatalogItem],
) -> list[dict[str, Optional[str]]]:
    seen: set[tuple[str, Optional[str], Optional[str]]] = set()
    out: list[dict[str, Optional[str]]] = []

    for item in items:
        if not item.city:
            continue

        key = (
            _norm_state(item.state) or "MI",
            _norm_lower(item.county),
            _norm_lower(item.city),
        )
        if key in seen:
            continue

        seen.add(key)
        out.append(
            {
                "state": key[0],
                "county": key[1],
                "city": key[2],
            }
        )

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
            out.append(item)
            continue

        if item.city is None and item.county is not None:
            if cnty and item.county == cnty:
                out.append(item)
            continue

        if item.city is not None:
            if cty and item.city == cty:
                out.append(item)
            continue

    return _sorted_then_deduped(out)


def _federal_and_state_baseline() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.ecfr.gov/current/title-24/subtitle-B/chapter-IX/part-982",
            state="MI",
            program_type="hcv",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 982 — Section 8 Tenant-Based Assistance: Housing Choice Voucher Program",
            notes="Canonical HCV regulations. Primary federal anchor.",
            source_kind="federal_anchor",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.ecfr.gov/current/title-24/subtitle-A/part-5",
            state="MI",
            program_type="hcv",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 5 — General HUD Program Requirements",
            notes="Program-wide HUD requirements. Includes current NSPIRE structure.",
            source_kind="federal_anchor",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.ecfr.gov/current/title-24/subtitle-A/part-5/subpart-G",
            state="MI",
            program_type="hcv",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 5 Subpart G — Physical Inspection of Real Estate",
            notes="Current NSPIRE inspection framework anchor.",
            source_kind="federal_anchor",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.federalregister.gov/documents/2023/06/22/2023-13293/national-standards-for-the-physical-inspection-of-real-estate-inspection-standards",
            state="MI",
            program_type="hcv",
            publisher="Federal Register",
            title="National Standards for the Physical Inspection of Real Estate (NSPIRE) Inspection Standards",
            notes="Federal Register standards notice for NSPIRE.",
            source_kind="federal_anchor",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.federalregister.gov/documents/2025/09/30/2025-19070/economic-growth-regulatory-relief-and-consumer-protection-act-implementation-of-national-standards",
            state="MI",
            program_type="hcv",
            publisher="Federal Register",
            title="Extension of NSPIRE Compliance Date for HCV, PBV and Mod Rehab (2025 notice)",
            notes="Current later federal timing notice; operationally more important than older extension notices.",
            source_kind="federal_anchor",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.hud.gov/program_offices/public_indian_housing/programs/hcv",
            state="MI",
            program_type="hcv",
            publisher="HUD",
            title="HUD Housing Choice Voucher Program",
            notes="Program guidance hub. Use alongside CFR, not instead of CFR.",
            source_kind="federal_anchor",
            priority=30,
        ),
        PolicyCatalogItem(
            url="https://www.legislature.mi.gov/Laws/Index?ObjectName=mcl-Act-348-of-1972",
            state="MI",
            publisher="Michigan Legislature",
            title="Michigan Landlord-Tenant Relationships Act (Act 348 of 1972)",
            notes="Primary state landlord-tenant act anchor.",
            source_kind="state_anchor",
            priority=40,
        ),
        PolicyCatalogItem(
            url="https://www.legislature.mi.gov/Laws/MCL?objectName=mcl-554-602",
            state="MI",
            publisher="Michigan Legislature",
            title="MCL 554.602 (Security Deposit Act section)",
            notes="State deposit/compliance anchor.",
            source_kind="state_anchor",
            priority=40,
        ),
        PolicyCatalogItem(
            url="https://www.legislature.mi.gov/Laws/Index?ObjectName=mcl-chap554",
            state="MI",
            publisher="Michigan Legislature",
            title="Michigan Compiled Laws Chapter 554",
            notes="Broader landlord-tenant baseline for Michigan.",
            source_kind="state_anchor",
            priority=50,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda",
            state="MI",
            publisher="MSHDA",
            title="Michigan State Housing Development Authority",
            notes="State housing authority anchor and statewide housing program hub.",
            source_kind="state_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/housing-choice-voucher",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="Housing Choice Voucher Program (HCV)",
            notes="Statewide HCV program overview.",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/hcv-landlords",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="HCV Landlords - MSHDA",
            notes="Statewide landlord hub for HCV operations, forms, inspection notices, and payment info.",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/hcv-landlords/housing-choice-voucher-landlords",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="Housing Choice Voucher Landlords",
            notes="Operational landlord requirements, HAP contract and tenancy addendum guidance.",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/rental/hcv-landlords/housing-choice-vouchers-landlords---payment-information",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="Housing Choice Voucher Landlords - Payment Information",
            notes="Operational payment timing/landlord payment process page.",
            source_kind="state_hcv_anchor",
            priority=60,
        ),
        PolicyCatalogItem(
            url="https://www.michigan.gov/mshda/-/media/Project/Websites/mshda/rental/assets/Shared-HCV---PBV/2026-HAP-UAP-Schedule.pdf",
            state="MI",
            program_type="hcv",
            publisher="MSHDA",
            title="2026 HAP/UAP Payment Schedule",
            notes="Useful for operational cash timing; not law, but relevant for landlord ops.",
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
            notes="Statewide PHA operational source for program behavior and landlord-facing operations.",
            source_kind="pha_plan",
            priority=70,
        ),
    ]


def _detroit_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Tenant Rental Property",
            notes="Detroit rental property hub. Owners are required to register and complete inspection flow.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property/landlord-rental",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Landlord Rental Requirements",
            notes="Current city rental registration/certification workflow page.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property/rental-certificate",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Rental Certificate of Compliance",
            notes="Certificate of compliance process/inspection workflow anchor.",
            source_kind="municipal_certificate",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/node/77656",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Steps for Certificate of Compliance",
            notes="Operational process steps for rental certificate/inspections/lead docs.",
            source_kind="municipal_certificate",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property/rental-certificate-3",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Inspections",
            notes="Inspection request flow and prerequisites for rental properties.",
            source_kind="municipal_inspection",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/departments/buildings-safety-engineering-and-environmental-department-bseed/bseed-divisions/property-maintenance/tenant-rental-property/rental-compliance-map",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Rental Compliance Map",
            notes="Certificate-of-compliance requirement and validity timing.",
            source_kind="municipal_certificate",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/node/1436",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit BSEED",
            title="Rental Requirements FAQ",
            notes="Detroit FAQ with registration, inspection and compliance references.",
            source_kind="municipal_guidance",
            priority=30,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/sites/detroitmi.localhost/files/2021-08/Landlord%20Guide.pdf",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit",
            title="Detroit Landlord Guide",
            notes="Helpful guidance PDF; keep as support, not sole authority.",
            source_kind="municipal_guidance",
            is_authoritative=False,
            priority=50,
        ),
        PolicyCatalogItem(
            url="https://detroitmi.gov/sites/detroitmi.localhost/files/2025-07/Landlord%20Quick%20Reference%20Guide%20%28FV%29%20%281%29%20%281%29.pdf",
            state="MI",
            county="wayne",
            city="detroit",
            publisher="City of Detroit",
            title="Detroit Rental Ordinance / Landlord Quick Reference Guide",
            notes="Recent quick-reference summary of Detroit rental law/process.",
            source_kind="municipal_guidance",
            priority=40,
        ),
        PolicyCatalogItem(
            url="https://dhcmi.org/sites/default/files/upload/MI001jv01-Admin%20Plan.pdf",
            state="MI",
            county="wayne",
            city="detroit",
            program_type="hcv",
            pha_name="Detroit Housing Commission",
            publisher="Detroit Housing Commission",
            title="Detroit Housing Commission HCV Administration Plan",
            notes="Primary PHA operational source for voucher program administration in Detroit.",
            source_kind="pha_plan",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.dhcmi.org/landlord-faqs",
            state="MI",
            county="wayne",
            city="detroit",
            program_type="hcv",
            pha_name="Detroit Housing Commission",
            publisher="Detroit Housing Commission",
            title="Detroit Housing Commission Landlord FAQs",
            notes="Operational landlord FAQ for Detroit PHA workflow.",
            source_kind="pha_guidance",
            priority=30,
        ),
        PolicyCatalogItem(
            url="https://dhcmi.org/sites/default/files/upload/LandlordGuideBook.pdf",
            state="MI",
            county="wayne",
            city="detroit",
            program_type="hcv",
            pha_name="Detroit Housing Commission",
            publisher="Detroit Housing Commission",
            title="Detroit Housing Commission Landlord Guidebook",
            notes="Landlord guidebook/process source for DHC program operations.",
            source_kind="pha_guidance",
            priority=40,
        ),
    ]


def _dearborn_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://dearborn.gov/residents/home-property/rental-property-information",
            state="MI",
            county="wayne",
            city="dearborn",
            publisher="City of Dearborn",
            title="Rental Property Information",
            notes="Official Dearborn rental property inspection/reporting hub.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://dearborn.gov/residents/home-property/rental-property-information/registering-rental-property",
            state="MI",
            county="wayne",
            city="dearborn",
            publisher="City of Dearborn",
            title="Registering a Rental Property",
            notes="Official Dearborn registration flow; certificate of occupancy and inspection requirements.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.dearborn.gov/index.php/step-1-submit-application-payment-0",
            state="MI",
            county="wayne",
            city="dearborn",
            publisher="City of Dearborn",
            title="Step 1: Submit an application with payment",
            notes="Operational application/payment step for requesting a rental home inspection.",
            source_kind="municipal_inspection",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.dearborn.gov/index.php/step-6-certificate-occupancy-0",
            state="MI",
            county="wayne",
            city="dearborn",
            publisher="City of Dearborn",
            title="Step 6: Certificate of Occupancy",
            notes="Certificate issuance timing, validity period, and renewal cycle anchor.",
            source_kind="municipal_certificate",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://dearborn.gov/how-submit-re-occupancy",
            state="MI",
            county="wayne",
            city="dearborn",
            publisher="City of Dearborn",
            title="How to Submit for Re-Occupancy",
            notes="Reinspection/correction workflow page that clarifies post-inspection correction requirements.",
            source_kind="municipal_inspection",
            priority=20,
        ),
    ]


def _warren_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/departments/rental-inspections-division/",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Rental Inspections Division",
            notes="Official Warren rental inspections division page.",
            source_kind="municipal_inspection",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/wp-content/uploads/2024/03/Rental-Application-Paperwork-revised2-Fillable.pdf",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Rental License Application",
            notes="Official Warren rental application packet.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/departments/building-division/",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Building Division",
            notes="Official Warren Building Division page with certificate and city certification application links.",
            source_kind="municipal_building_anchor",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/wp-content/uploads/2019/08/Building_Res_City_Certification_App.pdf",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Application for Residential City Certification",
            notes="Official Warren residential city certification application. Relevant to vacant residential dwellings posted no occupancy.",
            source_kind="municipal_certificate",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/wp-content/uploads/2019/08/Building_Certificate_of_Compliance_Application.pdf",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Certificate of Compliance Application",
            notes="Official Warren certificate of compliance application. Relevant to changed use, altered buildings, and occupancy approvals.",
            source_kind="municipal_certificate",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/departments/property-maintenance-division/",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Property Maintenance Division",
            notes="Operational property maintenance/enforcement page relevant to rental compliance.",
            source_kind="municipal_enforcement",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwarren.org/departments/public-service-department/",
            state="MI",
            county="macomb",
            city="warren",
            publisher="City of Warren",
            title="Public Service Department",
            notes="Operational contacts page with rental inspections/registration and property maintenance contact points.",
            source_kind="municipal_guidance",
            priority=30,
        ),
    ]


def _southfield_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/departments/building-department/building-southfield/rental-housing",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Rental Housing",
            notes="Official Southfield rental registration/inspection workflow page.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/sites/default/files/inline-files/rental_registration_application_1.pdf",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Application for Registration of Rental or Leased Dwelling",
            notes="Official Southfield registration application PDF.",
            source_kind="municipal_registration",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/sites/default/files/inline-files/ordinance_1656_rental_reg_1.pdf",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Ordinance No. 1656 - Rental Registration",
            notes="Primary ordinance anchor for rental certificate validity and compliance rules.",
            source_kind="municipal_ordinance",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/sites/default/files/2023-05/Ordinance%201768.pdf",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Rental Property Ordinance No. 1768",
            notes="Updated rental property ordinance reference for enforcement and registration structure.",
            source_kind="municipal_ordinance",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/departments/building-department/building-southfield/inspections",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Inspections",
            notes="Inspection workflow and certificate-of-occupancy operational page.",
            source_kind="municipal_inspection",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/departments/building-department/building-southfield/certificate-occupancy",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Certificate of Occupancy",
            notes="Certificate-of-occupancy/change-of-occupancy requirements anchor.",
            source_kind="municipal_certificate",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/residents/housing-property/code-enforcement",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            title="Code Enforcement",
            notes="Enforcement/support page that clarifies how property issues are handled.",
            source_kind="municipal_enforcement",
            priority=25,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/residents/housing-property/housing-section-8",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            program_type="hcv",
            pha_name="Southfield Housing Commission",
            title="Housing Section 8",
            notes="Official Southfield HCV page.",
            source_kind="pha_guidance",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.cityofsouthfield.com/sites/default/files/2026-01/Transfer%20letter%202025%20Plymouth%20%281%29.pdf",
            state="MI",
            county="oakland",
            city="southfield",
            publisher="City of Southfield",
            program_type="hcv",
            pha_name="Southfield Housing Commission",
            title="2025 Southfield HCV Transfer Letter",
            notes="Operationally critical: says Southfield HCV administration transfers to Plymouth Housing Commission as of 2025-10-01.",
            source_kind="pha_notice",
            priority=10,
        ),
    ]


def _pontiac_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.pontiac.mi.us/departments/community_development/property_rentals.php",
            state="MI",
            county="oakland",
            city="pontiac",
            publisher="City of Pontiac",
            title="Property Rentals",
            notes="Official Pontiac rental property hub; states rentals must be registered and up to code.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.pontiac.mi.us/departments/community_development/building_safety.php",
            state="MI",
            county="oakland",
            city="pontiac",
            publisher="City of Pontiac",
            title="Building Safety Division",
            notes="Official Pontiac building safety/inspections/certificates information page.",
            source_kind="municipal_inspection",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://cms3.revize.com/revize/pontiacminew/Document%20Center/Departments/COMMUNITY%20DEVELOPMENT/Code%20Enforcement/Documents%20%26%20Reports/b_s_rentalappsingle_021425%20with%20flow.pdf",
            state="MI",
            county="oakland",
            city="pontiac",
            publisher="City of Pontiac",
            title="Rental Registration Application (1 or 2 family)",
            notes="Official Pontiac rental registration/inspection flow PDF.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.pontiac.mi.us/how_do_i/apply_for/forms___applications.php",
            state="MI",
            county="oakland",
            city="pontiac",
            publisher="City of Pontiac",
            title="Forms, Permits and Applications",
            notes="General forms hub that links building safety and rental application material.",
            source_kind="municipal_guidance",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.pontiac.mi.us/departments/community_development/code_enforcement/index.php",
            state="MI",
            county="oakland",
            city="pontiac",
            publisher="City of Pontiac",
            title="Code Enforcement",
            notes="Code enforcement anchor relevant to rental and operational restrictions.",
            source_kind="municipal_enforcement",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.pontiac.mi.us/departments/community_development/code_enforcement/faqs.php",
            state="MI",
            county="oakland",
            city="pontiac",
            publisher="City of Pontiac",
            title="Code Enforcement FAQs",
            notes="FAQ page clarifying residential rental vs vacant registration constraints and operational expectations.",
            source_kind="municipal_guidance",
            priority=25,
        ),
    ]


def _livonia_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://livonia.gov/241/Inspection-Building-Enforcement",
            state="MI",
            county="wayne",
            city="livonia",
            publisher="City of Livonia",
            title="Inspection (Building & Enforcement)",
            notes="Official Livonia inspection department page.",
            source_kind="municipal_inspection",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://livonia.gov/247/Applications-Forms",
            state="MI",
            county="wayne",
            city="livonia",
            publisher="City of Livonia",
            title="Applications & Forms",
            notes="Official Livonia forms page that includes rental license application.",
            source_kind="municipal_registration",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://livonia.gov/DocumentCenter/View/403/Rental-License-Application-PDF",
            state="MI",
            county="wayne",
            city="livonia",
            publisher="City of Livonia",
            title="Rental License Application",
            notes="Official Livonia rental license application PDF.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://livonia.gov/DocumentCenter/View/4294/Rental-Guide-PDF",
            state="MI",
            county="wayne",
            city="livonia",
            publisher="City of Livonia",
            title="Rental Properties Guide",
            notes="Official Livonia rental guidebook prepared by the Inspection Department.",
            source_kind="municipal_guidance",
            priority=30,
        ),
        PolicyCatalogItem(
            url="https://livonia.gov/1832/Building-Permits-Residential",
            state="MI",
            county="wayne",
            city="livonia",
            publisher="City of Livonia",
            title="Building Permits Residential",
            notes="Operational permitting page that also surfaces residential building and rental-related forms.",
            source_kind="municipal_building_anchor",
            priority=25,
        ),
        PolicyCatalogItem(
            url="https://livonia.gov/101/Departments",
            state="MI",
            county="wayne",
            city="livonia",
            publisher="City of Livonia",
            title="Departments",
            notes="Department index that explicitly points to inspection/certificate-of-occupancy responsibilities.",
            source_kind="municipal_guidance",
            priority=35,
        ),
    ]


def _westland_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityofwestland.com/203/Residential-Rental-Program",
            state="MI",
            county="wayne",
            city="westland",
            publisher="City of Westland",
            title="Residential Rental Program",
            notes="Official Westland rental registration/inspection/certification page.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwestland.com/194/Building-Division",
            state="MI",
            county="wayne",
            city="westland",
            publisher="City of Westland",
            title="Building Division",
            notes="Official Westland building division page with rental certificates and inspections links.",
            source_kind="municipal_inspection",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwestland.com/DocumentCenter/View/170/Rental-Registration-Application-PDF",
            state="MI",
            county="wayne",
            city="westland",
            publisher="City of Westland",
            title="Rental Registration Application",
            notes="Official Westland rental registration application PDF.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwestland.com/1845/Planning-Building-Department",
            state="MI",
            county="wayne",
            city="westland",
            publisher="City of Westland",
            title="Planning & Building Department",
            notes="Operational building department anchor with building inspection and compliance resources.",
            source_kind="municipal_building_anchor",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.cityofwestland.com/550/Buying-or-Selling-a-Home",
            state="MI",
            county="wayne",
            city="westland",
            publisher="City of Westland",
            title="Buying or Selling a Home",
            notes="Operational page exposing home certification inspection and inspection checklist resources that support turnover/compliance understanding.",
            source_kind="municipal_guidance",
            priority=30,
        ),
    ]


def _taylor_pack() -> list[PolicyCatalogItem]:
    return [
        PolicyCatalogItem(
            url="https://www.cityoftaylor.com/236/Rental-Department",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="Rental Department",
            notes="Official Taylor rental inspections and registration page.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://www.cityoftaylor.com/FormCenter/Building-Department-Forms-11/Building-Department-Rental-Property-Insp-110",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="Rental Property Inspection Application",
            notes="Official Taylor rental inspection application.",
            source_kind="municipal_inspection",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://cityoftaylor.com/FormCenter/Building-Department-Forms-11/Rental-Property-Registration-Application-111",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="Rental Property Registration Application",
            notes="Official Taylor rental property registration application.",
            source_kind="municipal_registration",
            priority=10,
        ),
        PolicyCatalogItem(
            url="https://ci.taylor.mi.us/DocumentCenter/View/11608/2025-2026-Rental-Inspection",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="2025-2026 Rental Inspection Form",
            notes="Official Taylor rental inspection form/document.",
            source_kind="municipal_inspection",
            priority=20,
        ),
        PolicyCatalogItem(
            url="https://www.cityoftaylor.com/FormCenter/Building-Department-Forms-11/Building-Department-ReOccupancy-Inspecti-64",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="Re-Occupancy Inspection Application",
            notes="Operational turnover/re-occupancy inspection page relevant to rental transitions.",
            source_kind="municipal_certificate",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.cityoftaylor.com/DocumentCenter/View/8259/Residential-and-Commercial-Reoccupancy-Application-2022",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="Residential and Commercial Reoccupancy Application",
            notes="PDF form supporting re-occupancy inspection and turnover workflow.",
            source_kind="municipal_certificate",
            priority=15,
        ),
        PolicyCatalogItem(
            url="https://www.cityoftaylor.com/FormCenter/Building-Department-Forms-11",
            state="MI",
            county="wayne",
            city="taylor",
            publisher="City of Taylor",
            title="Building Department Forms",
            notes="Taylor forms hub that exposes rental inspection and re-occupancy inspection paths together.",
            source_kind="municipal_guidance",
            priority=25,
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
        return [
            "detroit",
            "dearborn",
            "warren",
            "southfield",
            "pontiac",
            "livonia",
            "westland",
            "taylor",
        ]

    if f == "se_mi":
        return ["detroit"]

    if f in city_registry:
        return [f]

    return []


def catalog_mi_authoritative(focus: str = "se_mi_extended") -> list[PolicyCatalogItem]:
    """
    Focuses:
    - se_mi: federal/state + Detroit
    - se_mi_extended: federal/state + Detroit + Dearborn + Warren + Southfield + Pontiac + Livonia + Westland + Taylor
    - all_verified_core: alias for se_mi_extended
    - wayne_county_core / oakland_county_core / macomb_county_core
    - detroit / dearborn / warren / southfield / pontiac / livonia / westland / taylor
    """
    items = _federal_and_state_baseline()
    city_registry = _city_pack_registry()
    expanded_focuses = _expand_focuses(focus)

    for name in expanded_focuses:
        pack_builder = city_registry.get(name)
        if not pack_builder:
            continue
        items.extend(pack_builder())

    return _sorted_then_deduped(items)
