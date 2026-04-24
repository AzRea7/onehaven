
from __future__ import annotations

import json
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.policy_models import PolicyAssertion, PolicySource


def _loads_dict(value: Any) -> dict[str, Any]:
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


def evidence_chain_for_assertion(assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, Any]:
    citation = _loads_dict(getattr(assertion, "citation_json", None))
    provenance = _loads_dict(getattr(assertion, "rule_provenance_json", None))
    validation_state = str(getattr(assertion, "validation_state", None) or "").strip().lower()
    return {
        "assertion_id": int(getattr(assertion, "id", 0) or 0),
        "rule_family": str(getattr(assertion, "rule_family", None) or getattr(assertion, "rule_key", None) or "").strip().lower(),
        "rule_key": str(getattr(assertion, "rule_key", None) or "").strip().lower(),
        "normalized_category": getattr(assertion, "normalized_category", None) or getattr(assertion, "rule_category", None),
        "source": {
            "source_id": int(getattr(source, "id", 0) or 0) if source is not None else None,
            "url": getattr(source, "url", None) if source is not None else citation.get("url"),
            "publisher": getattr(source, "publisher", None) if source is not None else citation.get("publisher"),
            "publication_type": getattr(source, "publication_type", None) if source is not None else citation.get("publication_type"),
            "source_type": getattr(source, "source_type", None) if source is not None else provenance.get("source_type"),
            "authority_tier": getattr(source, "authority_tier", None) if source is not None else None,
            "authority_use_type": getattr(source, "authority_use_type", None) if source is not None else None,
        },
        "evidence": {
            "evidence_role": citation.get("evidence_role") or provenance.get("evidence_role"),
            "publication_type": citation.get("publication_type") or provenance.get("publication_type"),
            "projectable_truth": bool(citation.get("projectable_truth") if "projectable_truth" in citation else provenance.get("projectable_truth")),
            "requires_binding_authority": bool(citation.get("requires_binding_authority") if "requires_binding_authority" in citation else provenance.get("requires_binding_authority")),
            "citation_quality": citation.get("citation_quality"),
            "raw_excerpt": citation.get("raw_excerpt") or getattr(assertion, "raw_excerpt", None),
            "conflict_hints": citation.get("conflict_hints") or provenance.get("conflict_hints") or [],
        },
        "validation": {
            "validation_state": validation_state,
            "trust_state": getattr(assertion, "trust_state", None),
            "coverage_status": getattr(assertion, "coverage_status", None),
            "confidence": float(getattr(assertion, "confidence", 0.0) or 0.0),
            "authority_score": float(getattr(assertion, "authority_score", 0.0) or 0.0),
            "extraction_confidence": float(getattr(assertion, "extraction_confidence", 0.0) or 0.0),
        },
    }


def rollup_evidence_for_truth_record(
    db: Session,
    *,
    truth_record: dict[str, Any],
    supporting_assertion_ids: Iterable[int] | None = None,
    selected_assertion_id: int | None = None,
) -> dict[str, Any]:
    assertion_ids = [int(v) for v in (supporting_assertion_ids or []) if v is not None]
    if selected_assertion_id is not None and int(selected_assertion_id) not in assertion_ids:
        assertion_ids.insert(0, int(selected_assertion_id))

    assertions = list(
        db.scalars(
            select(PolicyAssertion).where(PolicyAssertion.id.in_(assertion_ids))
        ).all()
    ) if assertion_ids else []

    source_map: dict[int, PolicySource] = {}
    for row in assertions:
        source_id = getattr(row, "source_id", None)
        if source_id is None:
            continue
        if int(source_id) in source_map:
            continue
        try:
            src = db.get(PolicySource, int(source_id))
        except Exception:
            src = None
        if src is not None:
            source_map[int(source_id)] = src

    chains = [evidence_chain_for_assertion(row, source_map.get(int(getattr(row, "source_id", 0) or 0))) for row in assertions]
    counts = {
        "assertion_count": len(chains),
        "truth_capable_count": sum(1 for c in chains if c["evidence"].get("projectable_truth")),
        "support_only_count": sum(1 for c in chains if c["evidence"].get("evidence_role") == "support_only"),
        "evidence_only_count": sum(1 for c in chains if c["evidence"].get("evidence_role") == "evidence_only"),
        "manual_review_count": sum(1 for c in chains if c["validation"].get("validation_state") in {"ambiguous", "weak_support"}),
    }

    return {
        "truth_record": dict(truth_record or {}),
        "selected_assertion_id": int(selected_assertion_id) if selected_assertion_id is not None else None,
        "chains": chains,
        "counts": counts,
        "auditable": True,
        "bounded_evidence": True,
    }
