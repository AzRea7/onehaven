from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


AUTHORITY_AUTHORITATIVE_BINDING = "authoritative_binding"
AUTHORITY_AUTHORITATIVE_GUIDANCE = "authoritative_guidance"
AUTHORITY_APPROVED_OPERATIONAL = "approved_operational"
AUTHORITY_EVIDENCE_BACKED_SECONDARY = "evidence_backed_secondary"
AUTHORITY_INFERRED = "inferred"
AUTHORITY_UNTRUSTED = "untrusted"

CANONICAL_AUTHORITY_TIERS: tuple[str, ...] = (
    AUTHORITY_AUTHORITATIVE_BINDING,
    AUTHORITY_AUTHORITATIVE_GUIDANCE,
    AUTHORITY_APPROVED_OPERATIONAL,
    AUTHORITY_EVIDENCE_BACKED_SECONDARY,
    AUTHORITY_INFERRED,
    AUTHORITY_UNTRUSTED,
)

SOURCE_KIND_TO_AUTHORITY_TIER: dict[str, str] = {
    "federal_regulation": AUTHORITY_AUTHORITATIVE_BINDING,
    "state_statute": AUTHORITY_AUTHORITATIVE_BINDING,
    "county_code": AUTHORITY_AUTHORITATIVE_BINDING,
    "municipal_code": AUTHORITY_AUTHORITATIVE_BINDING,
    "court_rule": AUTHORITY_AUTHORITATIVE_BINDING,
    "executive_order": AUTHORITY_AUTHORITATIVE_BINDING,
    "official_notice": AUTHORITY_AUTHORITATIVE_GUIDANCE,
    "federal_program_guide": AUTHORITY_AUTHORITATIVE_GUIDANCE,
    "state_program_rule": AUTHORITY_AUTHORITATIVE_GUIDANCE,
    "housing_authority": AUTHORITY_AUTHORITATIVE_GUIDANCE,
    "pha_admin_plan": AUTHORITY_AUTHORITATIVE_GUIDANCE,
    "pha_program_page": AUTHORITY_AUTHORITATIVE_GUIDANCE,
    "city_program_page": AUTHORITY_APPROVED_OPERATIONAL,
    "county_program_page": AUTHORITY_APPROVED_OPERATIONAL,
    "permit_portal": AUTHORITY_APPROVED_OPERATIONAL,
    "fee_schedule": AUTHORITY_APPROVED_OPERATIONAL,
    "public_contact_directory": AUTHORITY_APPROVED_OPERATIONAL,
    "city_form": AUTHORITY_EVIDENCE_BACKED_SECONDARY,
    "inspection_standards": AUTHORITY_EVIDENCE_BACKED_SECONDARY,
    "dataset": AUTHORITY_EVIDENCE_BACKED_SECONDARY,
    "pdf_upload": AUTHORITY_EVIDENCE_BACKED_SECONDARY,
    "manual_summary": AUTHORITY_INFERRED,
    "crawler_inference": AUTHORITY_INFERRED,
    "llm_extraction": AUTHORITY_INFERRED,
    "unknown": AUTHORITY_UNTRUSTED,
}


@dataclass(frozen=True)
class AuthorityTier:
    name: str
    rank: int
    supports_binding_coverage: bool
    support_only: bool
    description: str
    accepted_source_kinds: tuple[str, ...] = ()
    downgraded_from: tuple[str, ...] = ()


AUTHORITY_TIERS: dict[str, AuthorityTier] = {
    AUTHORITY_AUTHORITATIVE_BINDING: AuthorityTier(
        name=AUTHORITY_AUTHORITATIVE_BINDING,
        rank=100,
        supports_binding_coverage=True,
        support_only=False,
        description="Binding legal or regulatory authority suitable for final legal coverage.",
        accepted_source_kinds=(
            "federal_regulation",
            "state_statute",
            "county_code",
            "municipal_code",
            "court_rule",
            "executive_order",
        ),
    ),
    AUTHORITY_AUTHORITATIVE_GUIDANCE: AuthorityTier(
        name=AUTHORITY_AUTHORITATIVE_GUIDANCE,
        rank=80,
        supports_binding_coverage=False,
        support_only=True,
        description="Official guidance or program authority that can support interpretation but not replace binding law.",
        accepted_source_kinds=(
            "official_notice",
            "federal_program_guide",
            "state_program_rule",
            "housing_authority",
            "pha_admin_plan",
            "pha_program_page",
        ),
    ),
    AUTHORITY_APPROVED_OPERATIONAL: AuthorityTier(
        name=AUTHORITY_APPROVED_OPERATIONAL,
        rank=60,
        supports_binding_coverage=False,
        support_only=True,
        description="Official operational information that is useful for workflow, contacts, fees, and implementation details.",
        accepted_source_kinds=(
            "city_program_page",
            "county_program_page",
            "permit_portal",
            "fee_schedule",
            "public_contact_directory",
        ),
    ),
    AUTHORITY_EVIDENCE_BACKED_SECONDARY: AuthorityTier(
        name=AUTHORITY_EVIDENCE_BACKED_SECONDARY,
        rank=40,
        supports_binding_coverage=False,
        support_only=True,
        description="Secondary evidence with provenance that may support a rule but cannot independently satisfy legal truth.",
        accepted_source_kinds=(
            "city_form",
            "inspection_standards",
            "dataset",
            "pdf_upload",
        ),
    ),
    AUTHORITY_INFERRED: AuthorityTier(
        name=AUTHORITY_INFERRED,
        rank=20,
        supports_binding_coverage=False,
        support_only=True,
        description="Derived or inferred content used for gap detection, not for final truth.",
        accepted_source_kinds=(
            "manual_summary",
            "crawler_inference",
            "llm_extraction",
        ),
        downgraded_from=(
            AUTHORITY_AUTHORITATIVE_BINDING,
            AUTHORITY_AUTHORITATIVE_GUIDANCE,
            AUTHORITY_APPROVED_OPERATIONAL,
            AUTHORITY_EVIDENCE_BACKED_SECONDARY,
        ),
    ),
    AUTHORITY_UNTRUSTED: AuthorityTier(
        name=AUTHORITY_UNTRUSTED,
        rank=0,
        supports_binding_coverage=False,
        support_only=True,
        description="Untrusted or unknown material that must not support final decisioning.",
        accepted_source_kinds=("unknown",),
    ),
}


def get_authority_tier(name: str | None) -> AuthorityTier:
    if not name:
        return AUTHORITY_TIERS[AUTHORITY_UNTRUSTED]
    return AUTHORITY_TIERS.get(str(name).strip().lower(), AUTHORITY_TIERS[AUTHORITY_UNTRUSTED])


def authority_rank(name: str | None) -> int:
    return get_authority_tier(name).rank


def supports_binding_coverage(name: str | None) -> bool:
    return get_authority_tier(name).supports_binding_coverage


def is_support_only_authority(name: str | None) -> bool:
    return get_authority_tier(name).support_only


def classify_source_kind_to_authority(source_kind: str | None) -> str:
    if not source_kind:
        return AUTHORITY_UNTRUSTED
    return SOURCE_KIND_TO_AUTHORITY_TIER.get(str(source_kind).strip().lower(), AUTHORITY_UNTRUSTED)


def authority_at_least(candidate: str | None, minimum: str | None) -> bool:
    return authority_rank(candidate) >= authority_rank(minimum)


def best_authority(values: Iterable[str | None]) -> str:
    best = AUTHORITY_UNTRUSTED
    best_rank = -1
    for value in values:
        rank = authority_rank(value)
        if rank > best_rank:
            best = get_authority_tier(value).name
            best_rank = rank
    return best


def should_downgrade_to_inferred(
    *,
    source_kind: str | None,
    has_citation: bool,
    has_chain_of_custody: bool,
    extracted_from_pdf_only: bool,
    lifecycle: str | None = None,
) -> bool:
    """
    Conservative downgrade gate for Step 4.
    This does not resolve truth yet; it just prevents weak inputs from masquerading
    as authoritative before Step 5 resolution exists.
    """
    authority = classify_source_kind_to_authority(source_kind)
    if authority in {AUTHORITY_INFERRED, AUTHORITY_UNTRUSTED}:
        return True
    if lifecycle and str(lifecycle).strip().lower() not in {"approved", "active", "current"}:
        return True
    if not has_citation:
        return True
    if extracted_from_pdf_only:
        return True
    if authority_rank(authority) >= authority_rank(AUTHORITY_AUTHORITATIVE_GUIDANCE) and not has_chain_of_custody:
        return True
    return False


def serialize_authority_tiers() -> dict[str, dict[str, Any]]:
    return {name: asdict(tier) for name, tier in AUTHORITY_TIERS.items()}