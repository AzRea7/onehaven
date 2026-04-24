from __future__ import annotations

from typing import Any

from onehaven_platform.backend.src.domain.workflow.stages import (
    STAGES,
    clamp_stage,
    next_stage,
    stage_catalog,
    stage_label,
    stage_meta,
    stage_rank,
)
from onehaven_platform.backend.src.services.pane_routing_service import build_pane_context
from onehaven_platform.backend.src.services.compliance_projection_service import build_property_projection_snapshot
from onehaven_platform.backend.src.services.state_machine_service import get_state_payload, get_transition_payload


def _rollback_quietly(db) -> None:
    try:
        db.rollback()
    except Exception:
        pass


PRE_CLOSE_STAGES = {
    "deal",
    "underwritten",
    "pursuing",
    "offer_prep",
    "offer_ready",
    "offer_submitted",
    "negotiating",
    "under_contract",
    "due_diligence",
    "closing",
    "compliance",
}

POST_CLOSE_STAGES = {
    "owned",
    "tenant",
    "lease_up",
    "stabilized",
    "operations",
    "management",
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _safe_string_list(value: Any) -> list[str]:
    out: list[str] = []
    for item in _safe_list(value):
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _projection_payload(db, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    try:
        snapshot = build_property_projection_snapshot(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        return snapshot if isinstance(snapshot, dict) else None
    except Exception:
        _rollback_quietly(db)
        return None


def _jurisdiction_trust_payload(projection_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not projection_snapshot:
        return {}

    projection = projection_snapshot.get("projection") or {}
    projection_reason = projection.get("projection_reason") or {}
    trust = projection_reason.get("jurisdiction_trust") or {}
    if isinstance(trust, dict):
        return trust
    return {}


def _trust_blocker_reasons(trust: dict[str, Any]) -> list[str]:
    return [str(x) for x in (trust.get("blocker_reasons") or []) if str(x).strip()]


def _trust_manual_review_reasons(trust: dict[str, Any]) -> list[str]:
    return [str(x) for x in (trust.get("manual_review_reasons") or []) if str(x).strip()]


def _dedupe_reasons(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _proof_gap_items(proof_obligations: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        item for item in (proof_obligations or [])
        if str(item.get("proof_status") or "").strip().lower() in {"missing", "expired", "mismatched"}
    ]


def _proof_blocking_reasons(proof_gap_items: list[dict[str, Any]] | None) -> list[str]:
    reasons: list[str] = []
    for item in proof_gap_items or []:
        if not bool(item.get("blocking")):
            continue
        label = str(item.get("proof_label") or item.get("rule_key") or "required proof").strip()
        status = str(item.get("proof_status") or "missing").strip().lower()
        gap = str(item.get("evidence_gap") or "").strip()
        if gap:
            reasons.append(gap)
        else:
            reasons.append(f"{label} is {status}.")
    return _dedupe_reasons(reasons)


def _safe_to_rely_on_from_trust_and_proof(*, trust: dict[str, Any], proof_gap_items: list[dict[str, Any]] | None) -> bool:
    blocker = _build_jurisdiction_blocker_from_trust(trust)
    if bool(blocker.get("blocking")):
        return False
    for item in proof_gap_items or []:
        if bool(item.get("blocking")):
            return False
    return bool(blocker.get("safe_for_user_reliance", blocker.get("safe_for_projection", False)))


def _build_jurisdiction_blocker_from_trust(trust: dict[str, Any]) -> dict[str, Any]:
    trust = trust if isinstance(trust, dict) else {}

    decision_code = str(trust.get("decision_code") or trust.get("decision") or trust.get("code") or "").strip() or None
    safe_for_projection = bool(trust.get("safe_for_projection", False))
    safe_for_user_reliance = bool(trust.get("safe_for_user_reliance", False))

    blocker_reasons = _trust_blocker_reasons(trust)
    manual_review_reasons = _trust_manual_review_reasons(trust)

    critical_missing_categories = list(trust.get("critical_missing_categories") or trust.get("missing_critical_categories") or [])
    critical_stale_categories = list(trust.get("critical_stale_categories") or [])
    stale_authoritative_categories = list(trust.get("stale_authoritative_categories") or critical_stale_categories)
    critical_inferred_categories = list(trust.get("critical_inferred_categories") or [])
    critical_conflicting_categories = list(trust.get("critical_conflicting_categories") or [])
    incomplete_required_tiers = list(trust.get("incomplete_required_tiers") or [])

    lockout_active = bool(
        trust.get("lockout_active")
        or trust.get("jurisdiction_lockout_active")
        or decision_code in {"jurisdiction_lockout_active", "locked_out", "blocked_due_to_lockout"}
    )
    validation_pending = bool(
        trust.get("validation_pending")
        or trust.get("validation_required")
        or trust.get("validation_blocking")
        or decision_code in {"validation_pending", "validation_required", "blocked_due_to_validation"}
    )

    blocking_reasons: list[str] = []
    if lockout_active:
        blocking_reasons.append("Jurisdiction lockout is active.")
    if critical_missing_categories:
        blocking_reasons.append("Critical jurisdiction coverage is missing for: " + ", ".join(critical_missing_categories) + ".")
    if stale_authoritative_categories:
        blocking_reasons.append("Critical authoritative jurisdiction sources are stale for: " + ", ".join(stale_authoritative_categories) + ".")
    if incomplete_required_tiers:
        blocking_reasons.append("Required jurisdiction authority tiers are incomplete for: " + ", ".join(incomplete_required_tiers) + ".")
    if critical_conflicting_categories:
        blocking_reasons.append("Critical jurisdiction conflicts remain unresolved for: " + ", ".join(critical_conflicting_categories) + ".")
    if validation_pending:
        blocking_reasons.append("Required jurisdiction validation has not cleared all required rules.")
    for reason in blocker_reasons:
        blocking_reasons.append(reason)
    for reason in manual_review_reasons:
        blocking_reasons.append(reason)

    blocking = bool(
        lockout_active
        or critical_missing_categories
        or stale_authoritative_categories
        or incomplete_required_tiers
        or critical_conflicting_categories
        or validation_pending
    )

    blocked_reason = None
    if blocking:
        if lockout_active:
            blocked_reason = "Jurisdiction lockout is active."
        elif critical_missing_categories:
            blocked_reason = "Critical jurisdiction coverage is missing."
        elif stale_authoritative_categories:
            blocked_reason = "Critical authoritative jurisdiction sources are stale."
        elif incomplete_required_tiers:
            blocked_reason = "Required jurisdiction authority tiers are incomplete."
        elif validation_pending:
            blocked_reason = "Required jurisdiction validation has not cleared all required rules."
        elif critical_conflicting_categories:
            blocked_reason = "Critical jurisdiction conflicts remain unresolved."
        else:
            blocked_reason = "Jurisdiction trust is insufficient for safe compliance use."

    return {
        "blocking": blocking,
        "blocked_reason": blocked_reason,
        "decision_code": decision_code,
        "safe_for_projection": safe_for_projection,
        "safe_for_user_reliance": safe_for_user_reliance,
        "lockout_active": lockout_active,
        "validation_pending": validation_pending,
        "critical_missing_categories": critical_missing_categories,
        "critical_stale_categories": critical_stale_categories,
        "stale_authoritative_categories": stale_authoritative_categories,
        "critical_inferred_categories": critical_inferred_categories,
        "critical_conflicting_categories": critical_conflicting_categories,
        "incomplete_required_tiers": incomplete_required_tiers,
        "trust_blocker_reasons": _dedupe_reasons(blocker_reasons),
        "manual_review_reasons": _dedupe_reasons(manual_review_reasons),
        "blocking_reasons": _dedupe_reasons(blocking_reasons),
        "jurisdiction_trust": trust,
    }


def build_property_jurisdiction_blocker(db, *, org_id: int, property_id: int) -> dict[str, Any]:
    try:
        projection_snapshot = _projection_payload(db, org_id=org_id, property_id=property_id)
        trust = _jurisdiction_trust_payload(projection_snapshot)
        blocker = _build_jurisdiction_blocker_from_trust(trust)
        blocker["property_id"] = int(property_id)
        proof_obligations: list[dict[str, Any]] = []
        proof_counts: dict[str, Any] = {}
        if projection_snapshot and isinstance(projection_snapshot, dict):
            proof_obligations = list(
                projection_snapshot.get("proof_obligations")
                or ((projection_snapshot.get("projection") or {}).get("proof_obligations") if isinstance(projection_snapshot.get("projection"), dict) else [])
                or []
            )
            proof_counts = dict(
                projection_snapshot.get("proof_counts")
                or ((projection_snapshot.get("projection") or {}).get("proof_counts") if isinstance(projection_snapshot.get("projection"), dict) else {})
                or {}
            )
        proof_gap_items = _proof_gap_items(proof_obligations)
        proof_blocking_reasons = _proof_blocking_reasons(proof_gap_items)
        legally_unsafe = bool(blocker.get("blocking")) or bool(proof_blocking_reasons)
        informationally_incomplete = (not legally_unsafe) and bool(proof_gap_items)
        unsafe_reasons = _dedupe_reasons(list(blocker.get("blocking_reasons") or []) + proof_blocking_reasons)
        informational_reasons = _dedupe_reasons([
            str(item.get("evidence_gap") or f"{item.get('proof_label') or item.get('rule_key') or 'Required proof'} is {str(item.get('proof_status') or 'missing').strip().lower()}.")
            for item in proof_gap_items if not bool(item.get("blocking"))
        ])
        blocker["proof_obligations"] = proof_obligations
        blocker["proof_counts"] = proof_counts
        blocker["proof_gap_items"] = proof_gap_items
        blocker["proof_blocking_reasons"] = proof_blocking_reasons
        blocker["safe_to_rely_on"] = _safe_to_rely_on_from_trust_and_proof(trust=trust, proof_gap_items=proof_gap_items)
        blocker["legally_unsafe"] = legally_unsafe
        blocker["informationally_incomplete"] = informationally_incomplete
        blocker["unsafe_reasons"] = unsafe_reasons
        blocker["informational_reasons"] = informational_reasons
        blocker["ok"] = True
        return blocker
    except Exception:
        _rollback_quietly(db)
        return {
            "ok": False,
            "property_id": int(property_id),
            "blocking": True,
            "blocked_reason": "Jurisdiction blocker computation failed.",
            "jurisdiction_trust": {},
            "proof_obligations": [],
            "proof_counts": {},
            "proof_gap_items": [],
            "proof_blocking_reasons": [],
            "safe_to_rely_on": False,
            "legally_unsafe": True,
            "informationally_incomplete": False,
            "unsafe_reasons": ["Jurisdiction blocker computation failed."],
            "informational_reasons": [],
        }


def _build_compliance_gate(
    projection_snapshot: dict[str, Any] | None,
    *,
    current_stage: str,
) -> dict[str, Any]:
    if not projection_snapshot or not projection_snapshot.get("projection"):
        return {
            "ok": True,
            "severity": "info",
            "status": "not_available",
            "blocked_reason": None,
            "warning_reason": None,
            "warning_count": 0,
            "blocking_count": 0,
            "unknown_count": 0,
            "stale_count": 0,
            "conflicting_count": 0,
            "readiness_score": None,
            "confidence_score": None,
            "projected_compliance_cost": None,
            "projected_days_to_rent": None,
            "blockers": [],
            "warnings": [],
            "impacted_rules": [],
            "unresolved_evidence_gaps": [],
            "post_close_recheck_needed": False,
            "jurisdiction_trust": {},
            "jurisdiction_blocking": False,
            "critical_missing_categories": [],
            "critical_stale_categories": [],
            "critical_inferred_categories": [],
            "critical_conflicting_categories": [],
            "trust_decision_code": None,
            "trust_blocker_reasons": [],
            "manual_review_reasons": [],
            "safe_for_projection": None,
            "safe_for_user_reliance": None,
            "safe_to_rely_on": None,
            "legally_unsafe": False,
            "informationally_incomplete": False,
            "unsafe_reasons": [],
            "informational_reasons": [],
            "proof_obligations": [],
            "proof_gap_count": 0,
        }

    projection = projection_snapshot.get("projection") or {}
    blockers = list(projection_snapshot.get("blockers") or [])
    proof_obligations = list(
        projection_snapshot.get("proof_obligations")
        or ((projection_snapshot.get("projection") or {}).get("proof_obligations") if isinstance(projection_snapshot.get("projection"), dict) else [])
        or []
    )
    proof_gap_items = _proof_gap_items(proof_obligations)
    blocking_count = _safe_int(projection.get("blocking_count"))
    unknown_count = _safe_int(projection.get("unknown_count"))
    stale_count = _safe_int(projection.get("stale_count"))
    conflicting_count = _safe_int(projection.get("conflicting_count"))
    readiness_score = _safe_float(projection.get("readiness_score"), 0.0)
    confidence_score = _safe_float(projection.get("confidence_score"), 0.0)
    projected_cost = projection.get("projected_compliance_cost")
    projected_days = projection.get("projected_days_to_rent")
    impacted_rules = list(projection.get("impacted_rules") or [])
    unresolved_gaps = list(projection.get("unresolved_evidence_gaps") or [])

    jurisdiction_trust = _jurisdiction_trust_payload(projection_snapshot)
    completeness_status = str(jurisdiction_trust.get("completeness_status") or "").strip().lower()
    coverage_confidence = str(jurisdiction_trust.get("coverage_confidence") or "").strip().lower()
    production_readiness = str(jurisdiction_trust.get("production_readiness") or "").strip().lower()
    completeness_score = _safe_float(jurisdiction_trust.get("completeness_score"), 0.0)

    critical_missing_categories = list(jurisdiction_trust.get("critical_missing_categories") or [])
    critical_stale_categories = list(jurisdiction_trust.get("critical_stale_categories") or [])
    critical_inferred_categories = list(jurisdiction_trust.get("critical_inferred_categories") or [])
    critical_conflicting_categories = list(jurisdiction_trust.get("critical_conflicting_categories") or [])

    jurisdiction_blocker = _build_jurisdiction_blocker_from_trust(jurisdiction_trust)
    safe_for_projection = bool(jurisdiction_blocker.get("safe_for_projection", False))
    safe_for_user_reliance = bool(jurisdiction_blocker.get("safe_for_user_reliance", False))
    trust_decision_code = jurisdiction_blocker.get("decision_code")
    trust_blocker_reasons = list(jurisdiction_blocker.get("trust_blocker_reasons") or [])
    manual_review_reasons = list(jurisdiction_blocker.get("manual_review_reasons") or [])

    warnings: list[str] = []
    if proof_gap_items:
        unresolved_gaps.extend(
            [
                {
                    "rule_key": item.get("rule_key"),
                    "gap": item.get("evidence_gap") or f"Missing required proof: {item.get('proof_label')}",
                }
                for item in proof_gap_items
            ]
        )
        blockers.extend(
            [
                {
                    "rule_key": item.get("rule_key"),
                    "title": item.get("proof_label"),
                    "evaluation_status": item.get("proof_status"),
                    "evidence_gap": item.get("evidence_gap") or f"Missing required proof: {item.get('proof_label')}",
                }
                for item in proof_gap_items if bool(item.get("blocking"))
            ]
        )

    blocked_reason = None
    warning_reason = None

    jurisdiction_blocking = bool(jurisdiction_blocker.get("blocking"))
    proof_blocking_reasons = _proof_blocking_reasons(proof_gap_items)
    safe_to_rely_on = _safe_to_rely_on_from_trust_and_proof(trust=jurisdiction_trust, proof_gap_items=proof_gap_items)

    hard_block = (
        blocking_count > 0
        or conflicting_count > 0
        or readiness_score < 45.0
        or jurisdiction_blocking
        or bool(proof_blocking_reasons)
        or production_readiness in {"blocked", "needs_review"}
    )

    soft_warn = (
        unknown_count > 0
        or stale_count > 0
        or confidence_score < 0.65
        or readiness_score < 70.0
        or len(unresolved_gaps) > 0
        or bool(critical_stale_categories)
        or bool(critical_inferred_categories)
        or coverage_confidence == "low"
        or completeness_status in {"missing", "partial", "stale", "conflicting"}
        or completeness_score < 0.80
        or bool(manual_review_reasons)
        or (not safe_for_user_reliance and not hard_block)
        or (not safe_to_rely_on and not hard_block)
    )

    if blocking_count > 0:
        warnings.append(f"{blocking_count} blocking compliance requirement(s) remain unresolved.")
    if conflicting_count > 0:
        warnings.append(f"{conflicting_count} conflicting compliance rule state(s) require review.")
    if stale_count > 0:
        warnings.append(f"{stale_count} stale compliance proof item(s) need refresh.")
    if unknown_count > 0:
        warnings.append(f"{unknown_count} unknown compliance requirement(s) still need proof.")
    if confidence_score < 0.65:
        warnings.append(f"Compliance confidence is low ({confidence_score:.2f}).")
    if readiness_score < 70.0:
        warnings.append(f"Compliance readiness is below target ({readiness_score:.1f}).")
    if projected_cost is not None:
        warnings.append(f"Projected compliance cost is ${_safe_float(projected_cost):,.0f}.")
    if projected_days is not None:
        warnings.append(f"Projected days to rent impact is {_safe_int(projected_days)} day(s).")

    if critical_missing_categories:
        warnings.append(
            "Critical jurisdiction coverage is missing for: "
            + ", ".join(critical_missing_categories)
            + "."
        )
    if critical_stale_categories:
        warnings.append(
            "Critical jurisdiction coverage is stale for: "
            + ", ".join(critical_stale_categories)
            + "."
        )
    if critical_inferred_categories:
        warnings.append(
            "Critical jurisdiction coverage is inferred-only for: "
            + ", ".join(critical_inferred_categories)
            + "."
        )
    if critical_conflicting_categories:
        warnings.append(
            "Critical jurisdiction coverage is conflicting for: "
            + ", ".join(critical_conflicting_categories)
            + "."
        )
    if coverage_confidence == "low":
        warnings.append("Jurisdiction coverage confidence is low.")
    if completeness_status in {"missing", "partial", "stale", "conflicting"}:
        warnings.append(
            f"Jurisdiction completeness is {completeness_status} "
            f"({completeness_score:.2f})."
        )
    for reason in trust_blocker_reasons:
        warnings.append(f"Jurisdiction trust blocker: {reason}.")
    for reason in manual_review_reasons:
        warnings.append(f"Jurisdiction manual review required: {reason}.")
    if proof_blocking_reasons:
        for reason in proof_blocking_reasons:
            warnings.append(f"Required property proof blocker: {reason}")

    if hard_block and current_stage in PRE_CLOSE_STAGES:
        if blocking_count > 0:
            blocked_reason = "Pre-close compliance blocker(s) remain unresolved."
        elif conflicting_count > 0:
            blocked_reason = "Conflicting compliance evidence must be resolved before closing."
        elif proof_blocking_reasons:
            blocked_reason = proof_blocking_reasons[0]
        elif jurisdiction_blocker.get("blocked_reason"):
            blocked_reason = str(jurisdiction_blocker.get("blocked_reason"))
        elif not safe_for_projection or production_readiness in {"blocked", "needs_review"}:
            blocked_reason = "Jurisdiction trust is too weak for a safe pre-close decision."
        else:
            blocked_reason = "Compliance readiness is too low to proceed safely."
    elif soft_warn and current_stage in PRE_CLOSE_STAGES:
        if critical_stale_categories:
            warning_reason = "Critical jurisdiction proof is stale and should be refreshed before close."
        elif critical_inferred_categories:
            warning_reason = "Critical jurisdiction coverage is inferred-only and needs stronger verification."
        elif manual_review_reasons:
            warning_reason = "Jurisdiction trust requires manual review before relying on compliance automation."
        elif coverage_confidence == "low":
            warning_reason = "Jurisdiction trust is weak and should be reviewed before close."
        elif stale_count > 0:
            warning_reason = "Compliance proof is stale and should be refreshed before close."
        elif not safe_to_rely_on:
            warning_reason = "Compliance details are informational only until required proof is verified."
        elif unknown_count > 0:
            warning_reason = "Compliance proof is incomplete before close."
        elif confidence_score < 0.65:
            warning_reason = "Compliance confidence is too low for a clean pre-close decision."
        else:
            warning_reason = "Compliance risk needs review before close."

    post_close_recheck_needed = current_stage in POST_CLOSE_STAGES and (
        stale_count > 0
        or unknown_count > 0
        or conflicting_count > 0
        or blocking_count > 0
        or bool(critical_stale_categories)
        or bool(critical_inferred_categories)
        or bool(critical_conflicting_categories)
        or bool(manual_review_reasons)
        or not safe_for_user_reliance
        or (not safe_to_rely_on and not hard_block)
    )

    ok = not (current_stage in PRE_CLOSE_STAGES and hard_block)
    severity = "high" if not ok else ("warning" if soft_warn else "info")
    status = "blocked" if not ok else ("warning" if soft_warn else "ok")

    return {
        "ok": ok,
        "severity": severity,
        "status": status,
        "blocked_reason": blocked_reason,
        "warning_reason": warning_reason,
        "warning_count": len(warnings),
        "blocking_count": blocking_count,
        "unknown_count": unknown_count,
        "stale_count": stale_count,
        "conflicting_count": conflicting_count,
        "readiness_score": readiness_score,
        "confidence_score": confidence_score,
        "projected_compliance_cost": projected_cost,
        "projected_days_to_rent": projected_days,
        "blockers": blockers,
        "warnings": warnings,
        "impacted_rules": impacted_rules,
        "unresolved_evidence_gaps": unresolved_gaps,
        "proof_obligations": proof_obligations,
        "proof_gap_count": len(proof_gap_items),
        "post_close_recheck_needed": post_close_recheck_needed,
        "jurisdiction_trust": jurisdiction_trust,
        "jurisdiction_blocker": jurisdiction_blocker,
        "jurisdiction_blocking": jurisdiction_blocking,
        "jurisdiction_lockout_active": bool(jurisdiction_blocker.get("lockout_active")),
        "jurisdiction_validation_pending": bool(jurisdiction_blocker.get("validation_pending")),
        "jurisdiction_blocked_reason": jurisdiction_blocker.get("blocked_reason"),
        "jurisdiction_blocker_reasons": list(jurisdiction_blocker.get("blocking_reasons") or []),
        "critical_missing_categories": critical_missing_categories,
        "critical_stale_categories": critical_stale_categories,
        "critical_inferred_categories": critical_inferred_categories,
        "critical_conflicting_categories": critical_conflicting_categories,
        "trust_decision_code": trust_decision_code,
        "trust_blocker_reasons": trust_blocker_reasons,
        "manual_review_reasons": manual_review_reasons,
        "safe_for_projection": safe_for_projection,
        "safe_for_user_reliance": safe_for_user_reliance,
        "safe_to_rely_on": safe_to_rely_on,
        "legally_unsafe": bool(hard_block),
        "informationally_incomplete": bool(soft_warn and not hard_block),
        "unsafe_reasons": _dedupe_reasons(list(trust_blocker_reasons) + proof_blocking_reasons + ([blocked_reason] if blocked_reason else [])),
        "informational_reasons": _dedupe_reasons(([warning_reason] if warning_reason else []) + [str(item.get("evidence_gap") or "").strip() for item in proof_gap_items if not bool(item.get("blocking"))]),
    }


def _build_pre_close_risk_summary(compliance_gate: dict[str, Any], *, current_stage: str) -> dict[str, Any]:
    if current_stage not in PRE_CLOSE_STAGES:
        return {
            "active": False,
            "status": "not_applicable",
            "severity": "info",
            "blocking": False,
            "warnings": [],
            "summary": None,
        }

    blocking = not bool(compliance_gate.get("ok", True))
    warnings = list(compliance_gate.get("warnings") or [])
    if blocking:
        summary = compliance_gate.get("blocked_reason") or "Compliance risk blocks pre-close progression."
        status = "blocked"
        severity = "high"
    elif warnings:
        summary = compliance_gate.get("warning_reason") or "Compliance risk should be reviewed before close."
        status = "warning"
        severity = "warning"
    else:
        summary = "No material pre-close compliance gating issues detected."
        status = "ok"
        severity = "info"

    return {
        "active": True,
        "status": status,
        "severity": severity,
        "blocking": blocking,
        "warnings": warnings,
        "summary": summary,
        "projected_compliance_cost": compliance_gate.get("projected_compliance_cost"),
        "projected_days_to_rent": compliance_gate.get("projected_days_to_rent"),
        "jurisdiction_trust": compliance_gate.get("jurisdiction_trust") or {},
        "trust_decision_code": compliance_gate.get("trust_decision_code"),
        "trust_blocker_reasons": compliance_gate.get("trust_blocker_reasons") or [],
        "manual_review_reasons": compliance_gate.get("manual_review_reasons") or [],
    }


def _build_post_close_recheck_summary(compliance_gate: dict[str, Any], *, current_stage: str) -> dict[str, Any]:
    if current_stage not in POST_CLOSE_STAGES:
        return {
            "active": False,
            "status": "not_applicable",
            "needed": False,
            "reason": None,
        }

    needed = bool(compliance_gate.get("post_close_recheck_needed"))
    if not needed:
        return {
            "active": True,
            "status": "ok",
            "needed": False,
            "reason": None,
        }

    if compliance_gate.get("critical_conflicting_categories"):
        reason = "Post-close jurisdiction trust contains critical conflicting coverage."
    elif compliance_gate.get("critical_stale_categories"):
        reason = "Post-close critical jurisdiction proof has gone stale and should be re-evaluated."
    elif compliance_gate.get("critical_inferred_categories"):
        reason = "Post-close critical jurisdiction coverage is still inferred-only."
    elif compliance_gate.get("manual_review_reasons"):
        reason = "Post-close jurisdiction trust still requires manual review."
    elif _safe_int(compliance_gate.get("stale_count")) > 0:
        reason = "Post-close compliance proof has gone stale and should be re-evaluated."
    elif _safe_int(compliance_gate.get("unknown_count")) > 0:
        reason = "Post-close compliance still includes unknown requirements."
    elif _safe_int(compliance_gate.get("conflicting_count")) > 0:
        reason = "Post-close compliance contains conflicting evidence."
    else:
        reason = "Post-close compliance blockers still exist."

    return {
        "active": True,
        "status": "recheck_required",
        "needed": True,
        "reason": reason,
        "warnings": list(compliance_gate.get("warnings") or []),
        "jurisdiction_trust": compliance_gate.get("jurisdiction_trust") or {},
    }


def _effective_gate(
    state_gate: dict[str, Any],
    compliance_gate: dict[str, Any],
    *,
    current_stage: str,
) -> dict[str, Any]:
    base = dict(state_gate or {})
    if current_stage in PRE_CLOSE_STAGES and not compliance_gate.get("ok", True):
        base["ok"] = False
        base["blocked_reason"] = compliance_gate.get("blocked_reason") or base.get("blocked_reason")
        base["compliance_gate"] = compliance_gate
        if not base.get("allowed_next_stage"):
            base["allowed_next_stage"] = "compliance"
        base["code"] = "compliance_projection_blocked"
    elif compliance_gate.get("status") == "warning":
        base["ok"] = True
        base["compliance_gate"] = compliance_gate
        base["warning_reason"] = compliance_gate.get("warning_reason")
    return base


def build_workflow_summary(
    db,
    *,
    org_id: int,
    property_id: int,
    principal: Any = None,
    recompute: bool = True,
) -> dict[str, Any]:
    try:
        state = get_state_payload(
            db,
            org_id=org_id,
            property_id=property_id,
            recompute=recompute,
        )
        tx = get_transition_payload(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    except Exception:
        _rollback_quietly(db)
        state = get_state_payload(
            db,
            org_id=org_id,
            property_id=property_id,
            recompute=False,
        )
        tx = get_transition_payload(
            db,
            org_id=org_id,
            property_id=property_id,
        )

    state = _safe_dict(state)
    tx = _safe_dict(tx)

    cur = clamp_stage(state.get("current_stage"))
    cur_rank = stage_rank(cur)
    nxt = next_stage(cur)
    state_gate = _safe_dict(tx.get("gate"))
    next_actions = _safe_string_list(state.get("next_actions"))
    stage_completion_summary = _safe_dict(state.get("stage_completion_summary"))
    constraints = _safe_dict(state.get("constraints"))
    outstanding_tasks = _safe_dict(state.get("outstanding_tasks"))
    pane = build_pane_context(
        current_stage=cur,
        constraints=constraints,
        principal=principal,
        org_id=org_id,
    )

    projection_snapshot = _projection_payload(db, org_id=org_id, property_id=property_id) or {}
    compliance_gate = _build_compliance_gate(projection_snapshot, current_stage=cur)
    gate = _effective_gate(state_gate, compliance_gate, current_stage=cur)
    pre_close_risk = _build_pre_close_risk_summary(compliance_gate, current_stage=cur)
    post_close_recheck = _build_post_close_recheck_summary(compliance_gate, current_stage=cur)

    projection = dict((projection_snapshot or {}).get("projection") or {})

    completed_lookup = {
        str(item.get("stage")): bool(item.get("is_complete"))
        for item in (stage_completion_summary.get("by_stage") or [])
        if isinstance(item, dict)
    }

    rows = []
    for stage in STAGES:
        rank = stage_rank(stage)
        meta = stage_meta(stage)
        is_completed = bool(completed_lookup.get(stage, False))
        is_current = rank == cur_rank
        is_next = nxt == stage
        rows.append(
            {
                "key": stage,
                "rank": rank,
                "label": stage_label(stage),
                "group": meta.get("group"),
                "is_completed": is_completed,
                "is_current": is_current,
                "is_next": is_next,
            }
        )

    action_recommendations = list(next_actions)
    if compliance_gate.get("status") == "blocked":
        action_recommendations.insert(0, "Resolve compliance blockers before proceeding.")
    elif compliance_gate.get("status") == "warning":
        action_recommendations.insert(0, "Refresh or verify compliance evidence before relying on the workflow state.")

    primary_action = {
        "pane": "compliance" if compliance_gate.get("status") == "blocked" else (
            pane.get("current_pane")
            or pane.get("visible_pane")
            or pane.get("suggested_pane")
            or "management"
        ),
        "label": "Resolve compliance blockers" if compliance_gate.get("status") == "blocked" else (
            "Review compliance warnings" if compliance_gate.get("status") == "warning" else "Continue workflow"
        ),
        "reason": compliance_gate.get("blocked_reason") or compliance_gate.get("warning_reason"),
    }

    return {
        "ok": True,
        "current_stage": cur,
        "next_stage": nxt,
        "current_stage_label": stage_label(cur),
        "current_stage_meta": stage_meta(cur),
        "pane": pane,
        "stage_rows": rows,
        "gate": gate,
        "pre_close_risk": pre_close_risk,
        "primary_action": primary_action,
        "next_actions": action_recommendations,
        "constraints": constraints,
        "outstanding_tasks": outstanding_tasks,
        "transition": tx,
        "compliance_projection": projection,
        "compliance_gate": compliance_gate,
        "unsafe_reasons": list(compliance_gate.get("unsafe_reasons") or []),
        "informational_reasons": list(compliance_gate.get("informational_reasons") or []),
        "safe_to_rely_on": compliance_gate.get("safe_to_rely_on"),
        "legally_unsafe": compliance_gate.get("legally_unsafe"),
        "informationally_incomplete": compliance_gate.get("informationally_incomplete"),
        "jurisdiction_trust": compliance_gate.get("jurisdiction_trust") or {},
        "post_close_recheck": post_close_recheck,
    }

# --- tier-two evidence-first final overrides ---


def _tier2_boundary_message(status: str) -> str:
    mapping = {
        "blocked": "Critical evidence or jurisdiction blockers must be resolved before the workflow can safely proceed.",
        "warning": "The workflow can continue only with review; evidence is incomplete or stale.",
        "ready": "The workflow can proceed with current evidence.",
        "info": "Compliance gate information is available but not decisive.",
    }
    return mapping.get(str(status or "info").strip().lower(), mapping["info"])


_tier2_original_build_property_jurisdiction_blocker = build_property_jurisdiction_blocker
_tier2_original_build_workflow_summary = build_workflow_summary


def build_property_jurisdiction_blocker(db, *, org_id: int, property_id: int) -> dict[str, Any]:
    blocker = dict(
        _tier2_original_build_property_jurisdiction_blocker(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    )
    if not blocker.get("ok"):
        return blocker

    trust = dict(blocker.get("jurisdiction_trust") or {})
    blocker["truth_model"] = {
        "mode": "evidence_first",
        "crawler_role": "discovery_and_refresh_only",
        "freshness_role": "support_only",
    }
    blocker["reliance_boundary"] = {
        "status": "not_safe_to_rely_on" if bool(blocker.get("legally_unsafe")) else (
            "degraded_review_required" if bool(blocker.get("informationally_incomplete")) else "operationally_reliable"
        ),
        "message": "Property workflow gating is based on evidence-first jurisdiction truth plus property proof obligations.",
    }
    blocker["blocking_categories"] = list(
        trust.get("critical_missing_categories")
        or trust.get("missing_critical_categories")
        or []
    )
    blocker["degraded_categories"] = list(
        trust.get("critical_stale_categories")
        or trust.get("critical_inferred_categories")
        or []
    )
    blocker["freshness_signal_only_categories"] = list(trust.get("freshness_signal_only_categories") or [])
    return blocker


def build_workflow_summary(db, *, org_id: int, property_id: int) -> dict[str, Any]:
    summary = dict(
        _tier2_original_build_workflow_summary(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    )
    compliance_gate = dict(summary.get("compliance_gate") or {})
    gate_status = str(compliance_gate.get("status") or "info").strip().lower()

    compliance_gate["truth_model"] = {
        "mode": "evidence_first",
        "crawler_role": "discovery_and_refresh_only",
        "freshness_role": "support_only",
    }
    compliance_gate["reliance_boundary"] = {
        "status": "not_safe_to_rely_on" if gate_status == "blocked" else (
            "degraded_review_required" if gate_status == "warning" else "operationally_reliable"
        ),
        "message": _tier2_boundary_message(gate_status),
    }
    compliance_gate["workflow_truth_source"] = "jurisdiction_trust_plus_property_proof"
    compliance_gate["final_gatekeeper"] = "workflow_gate_service"

    summary["compliance_gate"] = compliance_gate
    summary["safe_to_rely_on"] = bool(compliance_gate.get("safe_to_rely_on", summary.get("safe_to_rely_on")))
    summary["legally_unsafe"] = bool(compliance_gate.get("legally_unsafe", summary.get("legally_unsafe")))
    summary["informationally_incomplete"] = bool(compliance_gate.get("informationally_incomplete", summary.get("informationally_incomplete")))
    summary["truth_model"] = dict(compliance_gate.get("truth_model") or {})
    summary["reliance_boundary"] = dict(compliance_gate.get("reliance_boundary") or {})
    return summary
