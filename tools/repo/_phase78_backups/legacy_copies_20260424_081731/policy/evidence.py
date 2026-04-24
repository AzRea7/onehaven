
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


EVIDENCE_ROLE_TRUTH_CAPABLE = "truth_capable"
EVIDENCE_ROLE_SUPPORT_ONLY = "support_only"
EVIDENCE_ROLE_EVIDENCE_ONLY = "evidence_only"
EVIDENCE_ROLE_UNTRUSTED = "untrusted"

TRUTH_ROLE_BINDING_CANDIDATE = "binding_candidate"
TRUTH_ROLE_SUPPORT_ONLY = "support_only"
TRUTH_ROLE_EVIDENCE_ONLY = "evidence_only"
TRUTH_ROLE_UNTRUSTED = "untrusted"

EVIDENCE_TYPE_HTML = "html"
EVIDENCE_TYPE_PDF = "pdf"
EVIDENCE_TYPE_DATASET = "dataset"
EVIDENCE_TYPE_API = "api"
EVIDENCE_TYPE_FORM = "form"
EVIDENCE_TYPE_MANUAL = "manual"
EVIDENCE_TYPE_UNKNOWN = "unknown"


@dataclass(frozen=True)
class EvidenceRule:
    evidence_type: str
    default_role: str
    truth_role: str
    requires_chain_of_custody: bool
    can_independently_support_binding_truth: bool
    description: str


EVIDENCE_RULES: dict[str, EvidenceRule] = {
    EVIDENCE_TYPE_HTML: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_HTML,
        default_role=EVIDENCE_ROLE_TRUTH_CAPABLE,
        truth_role=TRUTH_ROLE_BINDING_CANDIDATE,
        requires_chain_of_custody=True,
        can_independently_support_binding_truth=True,
        description="Official HTML/web publication with verifiable origin may support truth.",
    ),
    EVIDENCE_TYPE_API: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_API,
        default_role=EVIDENCE_ROLE_TRUTH_CAPABLE,
        truth_role=TRUTH_ROLE_BINDING_CANDIDATE,
        requires_chain_of_custody=True,
        can_independently_support_binding_truth=True,
        description="Structured authoritative API output may support truth.",
    ),
    EVIDENCE_TYPE_PDF: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_PDF,
        default_role=EVIDENCE_ROLE_SUPPORT_ONLY,
        truth_role=TRUTH_ROLE_EVIDENCE_ONLY,
        requires_chain_of_custody=True,
        can_independently_support_binding_truth=False,
        description="PDFs are auditable evidence and can strengthen truth, but cannot independently satisfy binding legal coverage.",
    ),
    EVIDENCE_TYPE_DATASET: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_DATASET,
        default_role=EVIDENCE_ROLE_SUPPORT_ONLY,
        truth_role=TRUTH_ROLE_SUPPORT_ONLY,
        requires_chain_of_custody=True,
        can_independently_support_binding_truth=False,
        description="Datasets can support or corroborate a rule, but must not silently become legal truth without authoritative backing.",
    ),
    EVIDENCE_TYPE_FORM: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_FORM,
        default_role=EVIDENCE_ROLE_EVIDENCE_ONLY,
        truth_role=TRUTH_ROLE_EVIDENCE_ONLY,
        requires_chain_of_custody=True,
        can_independently_support_binding_truth=False,
        description="Forms, packets, and application documents are operational evidence, not binding law.",
    ),
    EVIDENCE_TYPE_MANUAL: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_MANUAL,
        default_role=EVIDENCE_ROLE_UNTRUSTED,
        truth_role=TRUTH_ROLE_UNTRUSTED,
        requires_chain_of_custody=False,
        can_independently_support_binding_truth=False,
        description="Manual notes or summaries are untrusted unless corroborated.",
    ),
    EVIDENCE_TYPE_UNKNOWN: EvidenceRule(
        evidence_type=EVIDENCE_TYPE_UNKNOWN,
        default_role=EVIDENCE_ROLE_UNTRUSTED,
        truth_role=TRUTH_ROLE_UNTRUSTED,
        requires_chain_of_custody=False,
        can_independently_support_binding_truth=False,
        description="Unknown evidence cannot support truth.",
    ),
}


@dataclass(frozen=True)
class AssertionEvidenceMetadata:
    source_id: str | None
    source_url: str | None
    source_kind: str | None
    evidence_type: str
    evidence_role: str
    truth_role: str
    chain_of_custody_complete: bool
    citation_present: bool
    hash_present: bool
    retrieved_at_present: bool
    page_locator_present: bool
    extracted_from_pdf_only: bool
    truth_eligible: bool
    support_only_marker: bool
    publisher: str | None = None
    extraction_method: str | None = None
    publication_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_evidence_rule(evidence_type: str | None) -> EvidenceRule:
    if not evidence_type:
        return EVIDENCE_RULES[EVIDENCE_TYPE_UNKNOWN]
    return EVIDENCE_RULES.get(str(evidence_type).strip().lower(), EVIDENCE_RULES[EVIDENCE_TYPE_UNKNOWN])


def determine_evidence_role(
    *,
    evidence_type: str | None,
    source_is_authoritative: bool = False,
    pdf_only: bool = False,
    dataset_only: bool = False,
) -> str:
    rule = get_evidence_rule(evidence_type)
    if pdf_only:
        return EVIDENCE_ROLE_SUPPORT_ONLY
    if dataset_only and not source_is_authoritative:
        return EVIDENCE_ROLE_SUPPORT_ONLY
    if source_is_authoritative and rule.can_independently_support_binding_truth:
        return EVIDENCE_ROLE_TRUTH_CAPABLE
    return rule.default_role


def determine_truth_role(
    *,
    evidence_type: str | None,
    source_is_authoritative: bool = False,
    pdf_only: bool = False,
    dataset_only: bool = False,
) -> str:
    rule = get_evidence_rule(evidence_type)
    if pdf_only:
        return TRUTH_ROLE_EVIDENCE_ONLY
    if dataset_only and not source_is_authoritative:
        return TRUTH_ROLE_SUPPORT_ONLY
    if source_is_authoritative and rule.can_independently_support_binding_truth:
        return TRUTH_ROLE_BINDING_CANDIDATE
    return rule.truth_role


def chain_of_custody_complete(
    *,
    source_url: str | None,
    retrieved_at: str | None,
    content_hash: str | None,
    citation: str | None,
) -> bool:
    return bool(source_url and retrieved_at and content_hash and citation)


def pdf_can_independently_support_binding_truth() -> bool:
    return False


def dataset_can_independently_support_binding_truth() -> bool:
    return False


def required_assertion_metadata_fields() -> tuple[str, ...]:
    return (
        "source_id",
        "source_url",
        "source_kind",
        "evidence_type",
        "evidence_role",
        "truth_role",
        "chain_of_custody_complete",
        "citation_present",
        "hash_present",
        "retrieved_at_present",
        "extracted_from_pdf_only",
        "truth_eligible",
        "support_only_marker",
    )


def evidence_boundary_summary(*, evidence_type: str | None, source_is_authoritative: bool = False, pdf_only: bool = False, dataset_only: bool = False) -> dict[str, Any]:
    role = determine_evidence_role(
        evidence_type=evidence_type,
        source_is_authoritative=source_is_authoritative,
        pdf_only=pdf_only,
        dataset_only=dataset_only,
    )
    truth_role = determine_truth_role(
        evidence_type=evidence_type,
        source_is_authoritative=source_is_authoritative,
        pdf_only=pdf_only,
        dataset_only=dataset_only,
    )
    rule = get_evidence_rule(evidence_type)
    truth_eligible = bool(source_is_authoritative and rule.can_independently_support_binding_truth and not pdf_only and not dataset_only)
    return {
        "evidence_type": rule.evidence_type,
        "evidence_role": role,
        "truth_role": truth_role,
        "requires_chain_of_custody": rule.requires_chain_of_custody,
        "can_independently_support_binding_truth": bool(rule.can_independently_support_binding_truth and truth_eligible),
        "truth_eligible": truth_eligible,
        "support_only_marker": role in {EVIDENCE_ROLE_SUPPORT_ONLY, EVIDENCE_ROLE_EVIDENCE_ONLY},
    }


def serialize_evidence_rules() -> dict[str, dict[str, Any]]:
    return {name: asdict(item) for name, item in EVIDENCE_RULES.items()}
