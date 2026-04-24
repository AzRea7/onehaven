from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


MATERIALITY_CRITICAL = "critical"
MATERIALITY_IMPORTANT = "important"
MATERIALITY_INFORMATIONAL = "informational"

EFFECT_HARD_BLOCK = "hard_block"
EFFECT_SOFT_BLOCK = "soft_block"
EFFECT_ADVISORY = "advisory"


@dataclass(frozen=True)
class MaterialityDefinition:
    level: str
    rank: int
    description: str


MATERIALITY_DEFINITIONS: dict[str, MaterialityDefinition] = {
    MATERIALITY_CRITICAL: MaterialityDefinition(
        level=MATERIALITY_CRITICAL,
        rank=100,
        description="Missing or conflicting truth can directly block legal reliance or safe operation.",
    ),
    MATERIALITY_IMPORTANT: MaterialityDefinition(
        level=MATERIALITY_IMPORTANT,
        rank=60,
        description="Important for operations, payment, or workflow quality, but not always a hard legal block.",
    ),
    MATERIALITY_INFORMATIONAL: MaterialityDefinition(
        level=MATERIALITY_INFORMATIONAL,
        rank=20,
        description="Useful context or support details that should not independently block the system.",
    ),
}


@dataclass(frozen=True)
class RuleFamilyMateriality:
    rule_family: str
    materiality: str
    business_effect: str
    inspection_risk: bool
    payment_risk: bool
    workflow_only_effect: bool
    default_decision_effect: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_RULE_FAMILY_MATERIALITY: dict[str, RuleFamilyMateriality] = {
    "rental_license_required": RuleFamilyMateriality(
        rule_family="rental_license_required",
        materiality=MATERIALITY_CRITICAL,
        business_effect="Operating without a required rental license can block lawful renting.",
        inspection_risk=False,
        payment_risk=True,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_HARD_BLOCK,
    ),
    "rental_registration_required": RuleFamilyMateriality(
        rule_family="rental_registration_required",
        materiality=MATERIALITY_IMPORTANT,
        business_effect="Registration is often required before or alongside operational workflow.",
        inspection_risk=False,
        payment_risk=False,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_SOFT_BLOCK,
    ),
    "inspection_required": RuleFamilyMateriality(
        rule_family="inspection_required",
        materiality=MATERIALITY_CRITICAL,
        business_effect="Inspection requirements can block occupancy, leasing, or voucher approval.",
        inspection_risk=True,
        payment_risk=True,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_HARD_BLOCK,
    ),
    "certificate_required_before_occupancy": RuleFamilyMateriality(
        rule_family="certificate_required_before_occupancy",
        materiality=MATERIALITY_CRITICAL,
        business_effect="Occupancy certificate requirements can hard-block move-in or leasing.",
        inspection_risk=True,
        payment_risk=True,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_HARD_BLOCK,
    ),
    "lead_disclosure_required": RuleFamilyMateriality(
        rule_family="lead_disclosure_required",
        materiality=MATERIALITY_CRITICAL,
        business_effect="Lead compliance failures carry legal and health risk.",
        inspection_risk=True,
        payment_risk=False,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_HARD_BLOCK,
    ),
    "permit_required_for_rehab": RuleFamilyMateriality(
        rule_family="permit_required_for_rehab",
        materiality=MATERIALITY_IMPORTANT,
        business_effect="Permits affect rehab legality and readiness.",
        inspection_risk=True,
        payment_risk=False,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_SOFT_BLOCK,
    ),
    "source_of_income_protected": RuleFamilyMateriality(
        rule_family="source_of_income_protected",
        materiality=MATERIALITY_CRITICAL,
        business_effect="Source-of-income protections affect screening, leasing, and fair housing risk.",
        inspection_risk=False,
        payment_risk=False,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_HARD_BLOCK,
    ),
    "hap_contract_and_tenancy_addendum_required": RuleFamilyMateriality(
        rule_family="hap_contract_and_tenancy_addendum_required",
        materiality=MATERIALITY_IMPORTANT,
        business_effect="Required voucher documents can delay or block payment onboarding.",
        inspection_risk=False,
        payment_risk=True,
        workflow_only_effect=False,
        default_decision_effect=EFFECT_SOFT_BLOCK,
    ),
    "pha_landlord_packet_required": RuleFamilyMateriality(
        rule_family="pha_landlord_packet_required",
        materiality=MATERIALITY_IMPORTANT,
        business_effect="Landlord packet requirements are mostly workflow and payment readiness items.",
        inspection_risk=False,
        payment_risk=True,
        workflow_only_effect=True,
        default_decision_effect=EFFECT_SOFT_BLOCK,
    ),
    "fee_schedule_reference": RuleFamilyMateriality(
        rule_family="fee_schedule_reference",
        materiality=MATERIALITY_INFORMATIONAL,
        business_effect="Fees influence underwriting and operational planning.",
        inspection_risk=False,
        payment_risk=False,
        workflow_only_effect=True,
        default_decision_effect=EFFECT_ADVISORY,
    ),
    "document_reference": RuleFamilyMateriality(
        rule_family="document_reference",
        materiality=MATERIALITY_INFORMATIONAL,
        business_effect="Documents support workflow completeness.",
        inspection_risk=False,
        payment_risk=False,
        workflow_only_effect=True,
        default_decision_effect=EFFECT_ADVISORY,
    ),
    "pha_administrator_changed": RuleFamilyMateriality(
        rule_family="pha_administrator_changed",
        materiality=MATERIALITY_INFORMATIONAL,
        business_effect="Contact changes are useful but not binding truth.",
        inspection_risk=False,
        payment_risk=False,
        workflow_only_effect=True,
        default_decision_effect=EFFECT_ADVISORY,
    ),
}


def get_materiality_definition(level: str | None) -> MaterialityDefinition:
    if not level:
        return MATERIALITY_DEFINITIONS[MATERIALITY_INFORMATIONAL]
    return MATERIALITY_DEFINITIONS.get(str(level).strip().lower(), MATERIALITY_DEFINITIONS[MATERIALITY_INFORMATIONAL])


def get_rule_family_materiality(rule_family: str | None) -> RuleFamilyMateriality:
    if not rule_family:
        return RuleFamilyMateriality(
            rule_family="unknown",
            materiality=MATERIALITY_INFORMATIONAL,
            business_effect="Unknown rule family.",
            inspection_risk=False,
            payment_risk=False,
            workflow_only_effect=True,
            default_decision_effect=EFFECT_ADVISORY,
        )
    return _RULE_FAMILY_MATERIALITY.get(
        str(rule_family).strip().lower(),
        RuleFamilyMateriality(
            rule_family=str(rule_family).strip().lower(),
            materiality=MATERIALITY_IMPORTANT,
            business_effect="Unmapped rule family; treat conservatively until categorized.",
            inspection_risk=False,
            payment_risk=False,
            workflow_only_effect=False,
            default_decision_effect=EFFECT_SOFT_BLOCK,
        ),
    )


def serialize_materiality() -> dict[str, dict[str, Any]]:
    return {name: asdict(item) for name, item in MATERIALITY_DEFINITIONS.items()}


def serialize_rule_family_materiality() -> dict[str, dict[str, Any]]:
    return {name: item.to_dict() for name, item in _RULE_FAMILY_MATERIALITY.items()}