# onehaven_decision_engine/backend/app/domain/agents/impl/ops_judge.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AgentRun


def run_ops_judge(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Judge/Critic (recommend-only):
      - consumes recent agent outputs for a property
      - emits ranked recommendations + risk flags
      - MUST emit actions: []
    """
    if not property_id:
        return {
            "agent_key": "ops_judge",
            "summary": "No property_id provided.",
            "facts": {},
            "actions": [],
            "recommendations": [],
        }

    runs = db.scalars(
        select(AgentRun)
        .where(AgentRun.org_id == org_id, AgentRun.property_id == int(property_id))
        .order_by(AgentRun.id.desc())
        .limit(25)
    ).all()

    # Focus only on completed/blocked runs with outputs
    usable = [
        r for r in runs
        if getattr(r, "output_json", None) and str(getattr(r, "status", "")).lower() in {"done", "blocked"}
    ]

    # Extract “signals”
    signals: list[dict[str, Any]] = []
    for r in usable:
        out = getattr(r, "output_json", None) or {}
        signals.append({
            "run_id": int(r.id),
            "agent_key": str(r.agent_key),
            "status": str(r.status),
            "summary": out.get("summary"),
            "facts": out.get("facts") or {},
            "recommendations": out.get("recommendations") or [],
            "proposed_actions_count": len((out.get("actions") or [])),
        })

    # Simple deterministic prioritization:
    # - pending approvals are top priority
    pending = [r for r in runs if str(getattr(r, "approval_status", "")).lower() == "pending"]
    recs: list[dict[str, Any]] = []

    if pending:
        recs.append({
            "type": "approval_required",
            "property_id": int(property_id),
            "priority": "high",
            "reason": f"{len(pending)} agent run(s) are blocked pending approval. Approve/apply to unblock workflow.",
            "blocked_runs": [{"run_id": int(r.id), "agent_key": str(r.agent_key)} for r in pending[:10]],
        })

    # Pull best recommendations from other agents (if present)
    extracted: list[dict[str, Any]] = []
    for s in signals:
        for rr in (s.get("recommendations") or [])[:10]:
            if isinstance(rr, dict):
                extracted.append({
                    "source_agent": s["agent_key"],
                    "property_id": int(property_id),
                    "priority": rr.get("priority") or "medium",
                    "reason": rr.get("reason") or rr.get("text") or rr.get("type") or "recommendation",
                    "payload": rr,
                })

    # Deterministic priority order
    prio_order = {"high": 0, "medium": 1, "low": 2}
    extracted.sort(key=lambda x: prio_order.get(str(x.get("priority")).lower(), 9))

    recs.extend(extracted[:12])

    # Risk flags (simple but useful)
    risks: list[str] = []
    if not usable:
        risks.append("No recent usable agent outputs found (runs missing output_json).")
    if len(pending) > 3:
        risks.append("Many pending approvals; consider tightening mutation scope or batching approvals.")

    return {
        "agent_key": "ops_judge",
        "summary": f"Judge synthesized {len(signals)} recent agent outputs into {len(recs)} recommended next steps.",
        "facts": {
            "property_id": int(property_id),
            "signals_count": len(signals),
            "blocked_pending_approval": len(pending),
            "risks": risks,
        },
        "actions": [],  # ✅ recommend_only contract compliance
        "recommendations": recs,
    }