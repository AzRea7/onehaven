from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.policy.authority import authority_rank, best_authority, is_support_only_authority
from onehaven_platform.backend.src.domain.policy.conflicts import assess_conflict, is_blocking_conflict
from onehaven_platform.backend.src.domain.policy.evidence import determine_evidence_role
from onehaven_platform.backend.src.domain.policy.materiality import get_rule_family_materiality
from onehaven_platform.backend.src.policy_models import PolicyAssertion, PolicySource


NON_TRUTH_VALIDATION_STATES = {"weak_support", "ambiguous", "unsupported", "conflicting"}
NON_PROJECTABLE_RULE_STATUSES = {"candidate", "draft", "replaced", "superseded", "conflicting", "stale"}
NON_PROJECTABLE_GOVERNANCE_STATES = {"draft", "replaced", "superseded"}
NON_CURRENT_REVIEW_STATES = {"needs_manual_review", "rejected"}


@dataclass(frozen=True)
class TruthCandidate:
    assertion_id: int
    source_id: int | None
    rule_key: str
    rule_family: str
    rule_category: str | None
    value_text: str | None
    value_json: dict[str, Any]
    authority_tier: str
    authority_rank: int
    lifecycle_state: str
    validation_state: str
    trust_state: str
    review_status: str
    governance_state: str
    rule_status: str
    evidence_role: str
    confidence: float
    extraction_confidence: float
    citation_quality: float
    freshness_score: float
    effective_date: datetime | None
    source_url: str | None
    source_kind: str | None
    source_level: str | None
    current: bool
    truth_eligible: bool
    projectable: bool
    supporting_only: bool
    blocking_category: bool
    raw_excerpt: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TruthResolutionRecord:
    rule_family: str
    rule_key: str
    rule_category: str | None
    selected: dict[str, Any] | None
    selected_assertion_id: int | None
    supporting_evidence: list[dict[str, Any]]
    suppressed_inputs: list[dict[str, Any]]
    competing_inputs: list[dict[str, Any]]
    conflict_status: str
    blocking_conflict: bool
    confidence: float
    authority_tier: str | None
    lifecycle_state: str
    review_status: str
    truth_status: str
    explanation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TruthResolutionBundle:
    state: str
    county: str | None
    city: str | None
    pha_name: str | None
    resolved_rules: list[TruthResolutionRecord]
    selected_rule_families: list[str]
    conflicting_rule_families: list[str]
    blocking_rule_families: list[str]
    support_only_rule_families: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "county": self.county,
            "city": self.city,
            "pha_name": self.pha_name,
            "resolved_rules": [row.to_dict() for row in self.resolved_rules],
            "selected_rule_families": list(self.selected_rule_families),
            "conflicting_rule_families": list(self.conflicting_rule_families),
            "blocking_rule_families": list(self.blocking_rule_families),
            "support_only_rule_families": list(self.support_only_rule_families),
        }


def _norm_state(value: str | None) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    return raw or None


def _norm_text(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None


def _loads_dict(value: Any) -> dict[str, Any]:
    import json

    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate[:32].replace("+00:00", ""))
        except Exception:
            continue
    return None


def _assertion_matches_scope(assertion: PolicyAssertion, *, county: str | None, city: str | None, pha_name: str | None) -> bool:
    row_county = _norm_lower(getattr(assertion, "county", None))
    row_city = _norm_lower(getattr(assertion, "city", None))
    row_pha = _norm_text(getattr(assertion, "pha_name", None))
    if row_county and row_county != county:
        return False
    if row_city and row_city != city:
        return False
    if row_pha and row_pha != pha_name:
        return False
    return True


def _source_row(db: Session, source_id: int | None) -> PolicySource | None:
    if not source_id:
        return None
    try:
        return db.get(PolicySource, int(source_id))
    except Exception:
        return None


def _assertion_rule_family(assertion: PolicyAssertion) -> str:
    raw = str(getattr(assertion, "rule_family", None) or "").strip().lower()
    if raw:
        return raw
    raw = str(getattr(assertion, "rule_key", None) or "").strip().lower()
    if raw:
        return raw
    return "unknown_rule_family"


def _assertion_rule_key(assertion: PolicyAssertion) -> str:
    raw = str(getattr(assertion, "rule_key", None) or "").strip().lower()
    return raw or _assertion_rule_family(assertion)


def _assertion_category(assertion: PolicyAssertion) -> str | None:
    raw = str(getattr(assertion, "normalized_category", None) or getattr(assertion, "rule_category", None) or "").strip().lower()
    return raw or None


def _lifecycle_state(assertion: PolicyAssertion) -> str:
    governance = str(getattr(assertion, "governance_state", None) or "").strip().lower()
    review = str(getattr(assertion, "review_status", None) or "").strip().lower()
    rule = str(getattr(assertion, "rule_status", None) or "").strip().lower()
    if governance in {"active", "approved"} and review in {"verified", "accepted", "approved", "projected"} and rule in {"active", "approved", ""}:
        return "approved_current"
    if governance in NON_PROJECTABLE_GOVERNANCE_STATES or rule in NON_PROJECTABLE_RULE_STATUSES:
        return "non_projectable"
    if review in NON_CURRENT_REVIEW_STATES:
        return "needs_review"
    return "candidate"


def _citation_quality(assertion: PolicyAssertion) -> float:
    citation_json = _loads_dict(getattr(assertion, "citation_json", None))
    if "citation_quality" in citation_json:
        return max(0.0, min(1.0, _safe_float(citation_json.get("citation_quality"), 0.0)))
    score = 0.0
    if getattr(assertion, "source_citation", None):
        score += 0.40
    if citation_json.get("url"):
        score += 0.20
    if citation_json.get("title"):
        score += 0.20
    if citation_json.get("publisher"):
        score += 0.10
    if citation_json.get("raw_excerpt") or getattr(assertion, "raw_excerpt", None):
        score += 0.10
    return round(min(1.0, score), 6)


def _freshness_score(assertion: PolicyAssertion, source: PolicySource | None) -> float:
    source_score = _safe_float(getattr(source, "freshness_score", None), -1.0) if source is not None else -1.0
    if source_score >= 0.0:
        return max(0.0, min(1.0, source_score))
    status = str(getattr(source, "freshness_status", None) or "").strip().lower() if source is not None else ""
    if status in {"fresh", "current", "validated"}:
        return 1.0
    if status in {"stale", "aged"}:
        return 0.35
    if status in {"fetch_failed", "blocked", "error"}:
        return 0.0
    return 0.60


def _truth_eligibility(assertion: PolicyAssertion, source: PolicySource | None) -> tuple[bool, bool, str]:
    authority_tier = str(
        getattr(assertion, "authority_tier", None)
        or getattr(source, "authority_tier", None)
        or "untrusted"
    ).strip().lower()
    validation_state = str(getattr(assertion, "validation_state", None) or "pending").strip().lower()
    lifecycle = _lifecycle_state(assertion)
    source_truth_boundary = str(getattr(source, "truth_boundary", None) or getattr(source, "policy_source_truth_boundary", None) or "eligible_evidence").strip().lower() if source is not None else "eligible_evidence"
    projectable = (
        validation_state not in NON_TRUTH_VALIDATION_STATES
        and lifecycle == "approved_current"
        and source_truth_boundary != "discovery_only"
        and getattr(assertion, "superseded_by_assertion_id", None) is None
        and getattr(assertion, "replaced_by_assertion_id", None) is None
        and bool(getattr(assertion, "is_current", True))
    )
    support_only = is_support_only_authority(authority_tier)
    return projectable or support_only, projectable, authority_tier


def build_truth_candidate(db: Session, assertion: PolicyAssertion, source: PolicySource | None = None) -> TruthCandidate:
    source = source or _source_row(db, getattr(assertion, "source_id", None))
    value_json = _loads_dict(getattr(assertion, "value_json", None))
    confidence = _safe_float(getattr(assertion, "confidence", None), 0.0)
    extraction_confidence = _safe_float(getattr(assertion, "extraction_confidence", None), confidence)
    source_authority = str(getattr(source, "authority_tier", None) or "").strip().lower() if source is not None else ""
    eligible, projectable, authority_tier = _truth_eligibility(assertion, source)
    if not authority_tier:
        authority_tier = source_authority or "untrusted"
    evidence_role = determine_evidence_role(
        evidence_type=str(getattr(source, "publication_type", None) or value_json.get("publication_type") or "unknown").strip().lower(),
        source_is_authoritative=authority_rank(authority_tier) >= authority_rank("authoritative_binding"),
        pdf_only=bool(value_json.get("is_pdf_backed") or value_json.get("pdf_only")),
    )
    category = _assertion_category(assertion)
    materiality = get_rule_family_materiality(_assertion_rule_family(assertion))
    return TruthCandidate(
        assertion_id=int(getattr(assertion, "id", 0) or 0),
        source_id=int(getattr(assertion, "source_id", 0) or 0) or None,
        rule_key=_assertion_rule_key(assertion),
        rule_family=_assertion_rule_family(assertion),
        rule_category=category,
        value_text=str(value_json.get("value") or value_json.get("status") or value_json.get("answer") or "").strip() or None,
        value_json=value_json,
        authority_tier=authority_tier,
        authority_rank=authority_rank(authority_tier),
        lifecycle_state=_lifecycle_state(assertion),
        validation_state=str(getattr(assertion, "validation_state", None) or "pending").strip().lower(),
        trust_state=str(getattr(assertion, "trust_state", None) or "extracted").strip().lower(),
        review_status=str(getattr(assertion, "review_status", None) or "").strip().lower(),
        governance_state=str(getattr(assertion, "governance_state", None) or "").strip().lower(),
        rule_status=str(getattr(assertion, "rule_status", None) or "").strip().lower(),
        evidence_role=evidence_role,
        confidence=round(max(0.0, min(1.0, confidence)), 6),
        extraction_confidence=round(max(0.0, min(1.0, extraction_confidence)), 6),
        citation_quality=_citation_quality(assertion),
        freshness_score=_freshness_score(assertion, source),
        effective_date=_parse_dt(getattr(assertion, "effective_date", None) or value_json.get("effective_date")),
        source_url=str(getattr(source, "url", None) or "").strip() or None,
        source_kind=str(getattr(source, "source_kind", None) or "").strip() or None,
        source_level=str(getattr(assertion, "source_level", None) or getattr(source, "source_type", None) or "").strip().lower() or None,
        current=bool(getattr(assertion, "is_current", True)),
        truth_eligible=eligible,
        projectable=projectable,
        supporting_only=(support_only or evidence_role != "truth_capable"),
        blocking_category=(materiality.materiality == "critical"),
        raw_excerpt=str(getattr(assertion, "raw_excerpt", None) or "").strip() or None,
    )


def _candidate_sort_key(candidate: TruthCandidate) -> tuple[Any, ...]:
    return (
        1 if candidate.projectable else 0,
        candidate.authority_rank,
        candidate.freshness_score,
        candidate.citation_quality,
        candidate.extraction_confidence,
        candidate.confidence,
        1 if candidate.current else 0,
        -candidate.assertion_id,
    )


def _serialize_candidate(candidate: TruthCandidate) -> dict[str, Any]:
    payload = candidate.to_dict()
    if payload.get("effective_date") is not None:
        payload["effective_date"] = payload["effective_date"].isoformat()
    return payload


def _same_material_value(a: TruthCandidate, b: TruthCandidate) -> bool:
    a_value = str(a.value_text or a.value_json.get("normalized_value") or a.value_json.get("status") or "").strip().lower()
    b_value = str(b.value_text or b.value_json.get("normalized_value") or b.value_json.get("status") or "").strip().lower()
    if not a_value and not b_value:
        return True
    return a_value == b_value


def resolve_truth_candidates(candidates: Sequence[TruthCandidate]) -> TruthResolutionRecord:
    if not candidates:
        return TruthResolutionRecord(
            rule_family="unknown_rule_family",
            rule_key="unknown_rule_family",
            rule_category=None,
            selected=None,
            selected_assertion_id=None,
            supporting_evidence=[],
            suppressed_inputs=[],
            competing_inputs=[],
            conflict_status="none",
            blocking_conflict=False,
            confidence=0.0,
            authority_tier=None,
            lifecycle_state="missing",
            review_status="missing",
            truth_status="missing",
            explanation="No candidates available.",
        )

    ordered = sorted(candidates, key=_candidate_sort_key, reverse=True)
    selected = ordered[0]
    support_rows: list[dict[str, Any]] = []
    suppressed_rows: list[dict[str, Any]] = []
    competing_rows: list[dict[str, Any]] = []
    conflict_codes: list[str] = []

    for challenger in ordered[1:]:
        same_value = _same_material_value(selected, challenger)
        assessment = assess_conflict(
            same_family=True,
            value_a=selected.value_text or selected.value_json,
            value_b=challenger.value_text or challenger.value_json,
            role_a=selected.evidence_role,
            role_b=challenger.evidence_role,
            both_truth_capable=(selected.evidence_role == "truth_capable" and challenger.evidence_role == "truth_capable"),
        )
        if same_value or not assessment.blocking:
            support_rows.append(_serialize_candidate(challenger))
        else:
            competing_rows.append(_serialize_candidate(challenger))
        suppressed_rows.append(_serialize_candidate(challenger))
        conflict_codes.append(assessment.code)

    blocking_conflict = any(is_blocking_conflict(code) for code in conflict_codes)
    conflict_status = "blocking" if blocking_conflict else ("warning" if conflict_codes else "none")
    authority_tier = best_authority([row.authority_tier for row in ordered])
    penalty = 0.25 if blocking_conflict else (0.05 if conflict_codes else 0.0)
    confidence = round(max(0.0, min(1.0, (selected.confidence * 0.45) + (selected.extraction_confidence * 0.20) + (selected.citation_quality * 0.20) + (selected.freshness_score * 0.15) - penalty)), 6)
    truth_status = "projectable_truth" if selected.projectable and not blocking_conflict else ("support_only" if selected.supporting_only and not blocking_conflict else "manual_review")
    explanation = "Selected highest-ranked current candidate"
    if blocking_conflict:
        explanation = "Selected highest-ranked candidate, but unresolved same-family material conflict remains"
    elif support_rows:
        explanation = "Selected highest-ranked candidate and linked weaker corroborating evidence"

    return TruthResolutionRecord(
        rule_family=selected.rule_family,
        rule_key=selected.rule_key,
        rule_category=selected.rule_category,
        selected=_serialize_candidate(selected),
        selected_assertion_id=selected.assertion_id,
        supporting_evidence=support_rows,
        suppressed_inputs=suppressed_rows,
        competing_inputs=competing_rows,
        conflict_status=conflict_status,
        blocking_conflict=blocking_conflict,
        confidence=confidence,
        authority_tier=authority_tier,
        lifecycle_state=selected.lifecycle_state,
        review_status=selected.review_status,
        truth_status=truth_status,
        explanation=explanation,
    )


def resolve_market_truth(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    assertion_rows: Sequence[PolicyAssertion] | None = None,
) -> TruthResolutionBundle:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    rows: list[PolicyAssertion]
    if assertion_rows is not None:
        rows = list(assertion_rows)
    else:
        stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)
        if hasattr(PolicyAssertion, "org_id"):
            if org_id is None:
                stmt = stmt.where(PolicyAssertion.org_id.is_(None))
            else:
                stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
        rows = [row for row in db.scalars(stmt).all() if _assertion_matches_scope(row, county=cnty, city=cty, pha_name=pha)]

    grouped: dict[str, list[TruthCandidate]] = {}
    for assertion in rows:
        candidate = build_truth_candidate(db, assertion)
        grouped.setdefault(candidate.rule_family, []).append(candidate)

    resolved: list[TruthResolutionRecord] = []
    conflicting: list[str] = []
    blocking: list[str] = []
    support_only: list[str] = []
    selected_families: list[str] = []

    for family in sorted(grouped):
        record = resolve_truth_candidates(grouped[family])
        resolved.append(record)
        selected_families.append(record.rule_family)
        if record.conflict_status != "none":
            conflicting.append(record.rule_family)
        if record.blocking_conflict:
            blocking.append(record.rule_family)
        if record.truth_status == "support_only":
            support_only.append(record.rule_family)

    return TruthResolutionBundle(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        resolved_rules=resolved,
        selected_rule_families=selected_families,
        conflicting_rule_families=conflicting,
        blocking_rule_families=blocking,
        support_only_rule_families=support_only,
    )


def resolve_property_truth(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
) -> dict[str, Any]:
    bundle = resolve_market_truth(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    return bundle.to_dict()
