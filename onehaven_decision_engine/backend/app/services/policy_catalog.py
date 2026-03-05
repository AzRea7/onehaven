# backend/app/services/policy_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CatalogItem:
    key: str
    url: str
    publisher: str
    title: str
    program_type: Optional[str] = None
    state: Optional[str] = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    notes: Optional[str] = None


def catalog_mi_authoritative(focus: str = "se_mi") -> list[CatalogItem]:
    """
    Curated high-trust policy sources.

    Focus levels:
      - se_mi: Southeast Michigan starter pack + federal anchors
      - mi_statewide: Michigan + federal anchors
    """
    items: list[CatalogItem] = []

    # ------------------------------------------------------------------
    # FEDERAL / HUD / HCV ANCHORS
    # ------------------------------------------------------------------

    # HCV regulations
    items.append(
        CatalogItem(
            key="federal_ecfr_24cfr_part_982",
            url="https://www.ecfr.gov/current/title-24/subtitle-B/chapter-IX/part-982",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 982 — Section 8 Tenant-Based Assistance: Housing Choice Voucher Program",
            program_type="hcv",
            notes="Canonical HCV program regulations. Primary federal anchor.",
        )
    )

    # NSPIRE / HUD program requirements
    items.append(
        CatalogItem(
            key="federal_ecfr_24cfr_part_5",
            url="https://www.ecfr.gov/current/title-24/subtitle-A/part-5",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 5 — General HUD Program Requirements",
            program_type="hcv",
            notes="Program-wide HUD requirements. Includes current NSPIRE-related structure.",
        )
    )

    items.append(
        CatalogItem(
            key="federal_ecfr_24cfr_part_5_subpart_g",
            url="https://www.ecfr.gov/current/title-24/subtitle-A/part-5/subpart-G",
            publisher="eCFR (U.S. Government Publishing Office)",
            title="24 CFR Part 5 Subpart G — Physical Inspection of Real Estate",
            program_type="hcv",
            notes="Current NSPIRE inspection framework anchor.",
        )
    )

    # NSPIRE standards notice
    items.append(
        CatalogItem(
            key="federal_register_nspire_standards_2023",
            url="https://www.federalregister.gov/documents/2023/06/22/2023-13293/national-standards-for-the-physical-inspection-of-real-estate-inspection-standards",
            publisher="Federal Register",
            title="National Standards for the Physical Inspection of Real Estate (NSPIRE) Inspection Standards",
            program_type="hcv",
            notes="Federal Register standards notice for NSPIRE.",
        )
    )

    # Updated compliance-date notice (newer than the 2024 extension)
    items.append(
        CatalogItem(
            key="federal_register_nspire_hcv_extension_2025",
            url="https://www.federalregister.gov/documents/2025/09/30/2025-19070/economic-growth-regulatory-relief-and-consumer-protection-act-implementation-of-national-standards",
            publisher="Federal Register",
            title="Extension of NSPIRE Compliance Date for HCV, PBV and Mod Rehab (2025 notice)",
            program_type="hcv",
            notes="Use this instead of treating the 2024 delay as the latest word.",
        )
    )

    # HCV guidebook / admin guidance anchor
    items.append(
        CatalogItem(
            key="hud_hcv_guidebook_anchor",
            url="https://www.hud.gov/program_offices/public_indian_housing/programs/hcv",
            publisher="HUD",
            title="HUD Housing Choice Voucher Program (program landing page / guidance hub)",
            program_type="hcv",
            notes="Program guidance hub. Use alongside CFR, not instead of CFR.",
        )
    )

    # ------------------------------------------------------------------
    # MICHIGAN STATE LAW ANCHORS
    # ------------------------------------------------------------------

    items.append(
        CatalogItem(
            key="mi_legislature_landlord_tenant_act_index",
            url="https://www.legislature.mi.gov/Laws/Index?ObjectName=mcl-Act-348-of-1972",
            publisher="Michigan Legislature",
            title="Act 348 of 1972 — Landlord and Tenant Relationships",
            notes="Primary Michigan landlord-tenant act index. Expand with section-specific ingestion later.",
        )
    )

    items.append(
        CatalogItem(
            key="mi_legislature_security_deposit_554_602",
            url="https://www.legislature.mi.gov/Laws/MCL?objectName=mcl-554-602",
            publisher="Michigan Legislature",
            title="MCL 554.602 — Security deposit; amount",
            notes="Starter Michigan statutory anchor for deposits.",
        )
    )

    items.append(
        CatalogItem(
            key="mi_legislature_chapter_554_index",
            url="https://www.legislature.mi.gov/Laws/Index?ObjectName=mcl-chap554",
            publisher="Michigan Legislature",
            title="Michigan Compiled Laws Chapter 554 index",
            notes="Useful statewide landlord/tenant chapter index.",
        )
    )

    # ------------------------------------------------------------------
    # DETROIT / SOUTHEAST MICHIGAN STARTER
    # ------------------------------------------------------------------
    if focus in {"se_mi", "mi_statewide"}:
        # Keep these as official city / program landing pages you can validate and expand.
        items.append(
            CatalogItem(
                key="detroit_landlord_guide_pdf",
                url="https://detroitmi.gov/sites/detroitmi.localhost/files/2021-08/Landlord%20Guide.pdf",
                publisher="City of Detroit",
                title="City of Detroit Landlord Guide (PDF)",
                state="MI",
                county="Wayne",
                city="Detroit",
                notes="Detroit operational starter artifact. Verify currentness during review.",
            )
        )

        items.append(
            CatalogItem(
                key="mshda_home",
                url="https://www.michigan.gov/mshda",
                publisher="Michigan State Housing Development Authority (MSHDA)",
                title="MSHDA official site",
                state="MI",
                notes="State housing authority anchor; useful for program references and state-level housing materials.",
            )
        )

    return items
