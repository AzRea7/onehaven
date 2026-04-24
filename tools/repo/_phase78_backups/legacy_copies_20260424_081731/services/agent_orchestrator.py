# backend/app/services/agent_orchestrator.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import AgentRun, Property
from products.ops.backend.src.services.properties.state_machine import compute_and_persist_stage, get_state_payload


@dataclass(frozen=True)
class PlannedRun:
    property_id: int
    agent_key: str
    reason: str
    idempotency_key: str


CANONICAL_AGENT_KEYS = {
    "deal_intake",
    "underwrite",
    "rent_reasonableness",
    "hqs_precheck",
    "packet_builder",
    "photo_rehab",
    "next_actions",
    "timeline_nudger",
    "ops_judge",
    "trust_recompute",
}


def _loads_json(val: Any):
    if val is None:
        return None
    if isinstance(val, (list, dict, int, float, bool)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return s
    return None


def _normalize_next_actions(raw: Any) -> list[dict[str, Any]]:
    decoded = _loads_json(raw)
    if decoded is None:
        return []

    if isinstance(decoded, dict):
        out: list[dict[str, Any]] = []

        next_actions = decoded.get("next_actions")
        if isinstance(next_actions, list):
            for item in next_actions:
                if isinstance(item, dict):
                    out.append(item)
                elif isinstance(item, str):
                    out.append({"type": item})
                elif item is not None:
                    out.append({"type": "note", "value": str(item)})

        for key, value in decoded.items():
            if key == "next_actions":
                continue
            if isinstance(value, dict):
                out.append({"type": key, **value})
            elif isinstance(value, list):
                out.append({"type": key, "items": value})
            else:
                out.append({"type": key, "value": value})
        return out

    if isinstance(decoded, str):
        return [{"type": decoded}]

    if isinstance(decoded, list):
        out: list[dict[str, Any]] = []
        for item in decoded:
            if item is None:
                continue
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str):
                out.append({"type": item})
            else:
                out.append({"type": "note", "value": str(item)})
        return out

    return [{"type": "note", "value": str(decoded)}]


def _normalize_constraints(raw: Any) -> list[dict[str, Any]]:
    decoded = _loads_json(raw)
    if decoded is None:
        return []

    if isinstance(decoded, list):
        out: list[dict[str, Any]] = []
        for item in decoded:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str):
                out.append({"type": item})
            else:
                out.append({"type": "constraint", "value": str(item)})
        return out

    if isinstance(decoded, dict):
        out: list[dict[str, Any]] = []
        for key, value in decoded.items():
            if isinstance(value, dict):
                out.append({"type": key, **value})
            else:
                out.append({"type": key, "value": value})
        return out

    if isinstance(decoded, str):
        return [{"type": decoded}]

    return [{"type": "constraint", "value": str(decoded)}]


def _fingerprint(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _property_has_photos(prop: Property) -> bool:
    for field in (
        "photos_json",
        "image_urls_json",
        "zillow_photos_json",
        "listing_photos_json",
        "photo_urls_json",
    ):
        val = getattr(prop, field, None)
        if isinstance(val, list) and len(val) > 0:
            return True
        if isinstance(val, str) and val.strip():
            return True
    return False


def _add(planned: list[tuple[str, str]], agent_key: str, reason: str) -> None:
    if agent_key in CANONICAL_AGENT_KEYS:
        planned.append((agent_key, reason))


def plan_agent_runs(db: Session, *, org_id: int, property_id: int) -> List[PlannedRun]:
    prop = db.scalar(select(Property).where(Property.org_id == int(org_id)).where(Property.id == int(property_id)))
    if prop is None:
        return []

    compute_and_persist_stage(db, org_id=int(org_id), property=prop)
    state_payload = get_state_payload(db, org_id=int(org_id), property_id=int(property_id), recompute=True)

    bucket = _hour_bucket(datetime.utcnow())
    count = db.scalar(
        select(func.count(AgentRun.id))
        .where(AgentRun.org_id == int(org_id))
        .where(AgentRun.property_id == int(property_id))
        .where(AgentRun.created_at >= bucket)
    )
    if int(count or 0) >= int(settings.agents_max_runs_per_property_per_hour):
        return []

    next_actions = _normalize_next_actions(
        state_payload.get("outstanding_tasks") or state_payload.get("next_actions")
    )
    constraints = _normalize_constraints(state_payload.get("constraints"))
    stage = str(state_payload.get("current_stage") or "deal").strip().lower()

    planned: list[tuple[str, str]] = []

    # Stage-driven backbone
    if stage in {"import", "intake", "deal"}:
        _add(planned, "deal_intake", "stage implies intake/deal work")
        _add(planned, "underwrite", "deal-stage underwriting baseline should exist")
        _add(planned, "rent_reasonableness", "deal-stage rent baseline should exist")
        _add(planned, "packet_builder", "packet readiness starts early")

    if stage in {"decision", "acquisition"}:
        _add(planned, "underwrite", "decision/acquisition support")
        _add(planned, "rent_reasonableness", "decision/acquisition rent validation")
        _add(planned, "packet_builder", "decision/acquisition packet support")

    if stage in {"rehab_plan", "rehab_exec"}:
        _add(planned, "timeline_nudger", "rehab work needs continuity pressure")
        _add(planned, "packet_builder", "rehab stage packet completeness")
        if bool(getattr(settings, "agents_enable_photo_rehab", True)) and _property_has_photos(prop):
            _add(planned, "photo_rehab", "rehab stage has photos available for issue extraction")

    if stage in {"compliance"}:
        _add(planned, "hqs_precheck", "stage implies HQS readiness")
        _add(planned, "packet_builder", "compliance stage packet support")
        _add(planned, "timeline_nudger", "compliance stage needs follow-through")

    if stage in {"tenant", "lease"}:
        _add(planned, "hqs_precheck", "tenant/lease stage should confirm compliance readiness")
        _add(planned, "packet_builder", "tenant/lease packet support")
        _add(planned, "timeline_nudger", "tenant placement needs momentum")

    if stage in {"cash", "equity"}:
        _add(planned, "underwrite", "cash/equity stage should refresh economics")
        _add(planned, "timeline_nudger", "cash/equity stage follow-up")

    # Next-actions-driven planning
    for action in next_actions:
        typ = str((action or {}).get("type") or "").lower()

        if "valuation_due" in typ or "missing_valuation" in typ or "missing_underwriting" in typ:
            _add(planned, "underwrite", "next action indicates underwriting/valuation work")

        if "rent_gap" in typ or "rent_reconciliation_gap" in typ or "rent_reasonableness" in typ:
            _add(planned, "rent_reasonableness", "next action indicates rent review")

        if "packet_incomplete" in typ or "missing_packet" in typ:
            _add(planned, "packet_builder", "next action indicates packet work")

        if "needs_checklist" in typ or "missing_checklist" in typ:
            _add(planned, "hqs_precheck", "next action indicates checklist generation")

        if "inspection_not_passed" in typ or "missing_inspection" in typ or "hqs" in typ:
            _add(planned, "hqs_precheck", "next action indicates compliance follow-up")

        if "photos" in typ or "rehab" in typ:
            _add(planned, "photo_rehab", "next action indicates photo rehab work")

    # Constraint-driven planning
    for constraint in constraints:
        typ = str((constraint or {}).get("type") or "").lower()

        if "rent_reconciliation_gap" in typ:
            _add(planned, "rent_reasonableness", "constraint indicates rent reconciliation gap")

        if "missing_valuation" in typ or "valuation_due" in typ or "missing_underwriting" in typ:
            _add(planned, "underwrite", "constraint indicates underwriting work")

        if "missing_checklist" in typ or "inspection_not_passed" in typ:
            _add(planned, "hqs_precheck", "constraint indicates compliance work")

        if "packet" in typ:
            _add(planned, "packet_builder", "constraint indicates packet incompleteness")

    if bool(getattr(settings, "agents_enable_trust_recompute", True)):
        _add(planned, "trust_recompute", "keep deterministic trust fresh after workflow state changes")

    _add(planned, "next_actions", "synthesize deterministic next-step CTAs from current state")

    if bool(getattr(settings, "agents_enable_ops_judge", True)):
        _add(planned, "ops_judge", "synthesize specialist outputs into a ranked next-step plan")

    state_blob = {
        "plan_version": getattr(settings, "decision_version", "v0"),
        "stage": stage,
        "next_actions": next_actions,
        "constraints": constraints,
        "property_id": int(property_id),
        "photo_hint": _property_has_photos(prop),
    }
    fp = _fingerprint(state_blob)

    out: list[PlannedRun] = []
    for agent_key, reason in planned:
        idempotency_key = f"{org_id}:{property_id}:{agent_key}:{fp}"
        out.append(
            PlannedRun(
                property_id=int(property_id),
                agent_key=agent_key,
                reason=reason,
                idempotency_key=idempotency_key,
            )
        )

    # Deduplicate while preserving first reason
    uniq: dict[str, PlannedRun] = {}
    for row in out:
        uniq.setdefault(row.agent_key, row)
    return list(uniq.values())


def _safe_json_dump(x: Any) -> str:
    try:
        return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def on_run_terminal(db: Session, *, run_id: int, org_id: int | None = None) -> None:
    """
    Legacy compatibility hook.

    The runtime orchestrator is the real fan-out path now. This function still:
      1) annotates failure-derived next actions on property state
      2) then delegates to runtime fan-out

    That keeps older call sites alive without splitting orchestration truth in two.
    """
    run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id)))
    if run is None:
        return

    resolved_org_id = int(org_id) if org_id is not None else int(run.org_id)
    property_id = int(run.property_id) if getattr(run, "property_id", None) is not None else None
    if property_id is None:
        return

    prop = db.scalar(select(Property).where(Property.org_id == resolved_org_id).where(Property.id == property_id))
    if prop is None:
        return

    state = compute_and_persist_stage(db, org_id=resolved_org_id, property=prop)

    status = str(getattr(run, "status", "") or "").lower()
    agent_key = str(getattr(run, "agent_key", "") or "").lower()

    if status in {"failed", "timed_out"}:
        existing_raw = getattr(state, "outstanding_tasks_json", None)
        existing = _normalize_next_actions(existing_raw)

        def has_type(t: str) -> bool:
            return any(str(x.get("type", "")).lower() == t.lower() for x in existing if isinstance(x, dict))

        if agent_key in {"rent_reasonableness"} and not has_type("rent_gap"):
            existing.append({"type": "rent_gap", "source": "agent_failure", "run_id": int(run.id)})

        if agent_key in {"hqs_precheck"} and not has_type("missing_checklist"):
            existing.append({"type": "missing_checklist", "source": "agent_failure", "run_id": int(run.id)})

        if agent_key in {"packet_builder"} and not has_type("packet_incomplete"):
            existing.append({"type": "packet_incomplete", "source": "agent_failure", "run_id": int(run.id)})

        if agent_key in {"underwrite", "deal_underwrite"} and not has_type("missing_underwriting"):
            existing.append({"type": "missing_underwriting", "source": "agent_failure", "run_id": int(run.id)})

        setattr(state, "outstanding_tasks_json", _safe_json_dump({"next_actions": existing}))
        db.add(state)

    db.commit()

    try:
        from .agent_orchestrator_runtime import on_run_terminal as runtime_on_run_terminal

        runtime_on_run_terminal(db, org_id=resolved_org_id, run_id=int(run_id))
    except Exception:
        db.rollback()
        