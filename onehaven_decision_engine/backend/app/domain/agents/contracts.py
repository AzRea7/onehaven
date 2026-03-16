# backend/app/domain/agents/contracts.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


AgentMode = str  # recommend_only | mutate_requires_approval | autonomous_mutate


@dataclass(frozen=True)
class AgentContract:
    agent_key: str
    mode: AgentMode
    title: str
    category: str
    llm_mode: str = "deterministic"  # deterministic | llm_text | llm_vision | hybrid
    requires_human: bool = False
    allowed_entity_types: List[str] = field(default_factory=list)
    allowed_operations: List[str] = field(default_factory=list)
    required_root_fields: List[str] = field(default_factory=lambda: ["agent_key", "summary"])
    required_fact_fields: List[str] = field(default_factory=list)
    required_recommendation_fields: List[str] = field(default_factory=list)
    max_actions: int = 25


_ALIAS_TO_CANONICAL: Dict[str, str] = {
    "deal_underwrite": "underwrite",
    "rehab_from_photos": "photo_rehab",
    "next_actions_planner": "next_actions",
}


def canonical_agent_key(agent_key: str) -> str:
    raw = str(agent_key or "").strip()
    return _ALIAS_TO_CANONICAL.get(raw, raw)


CONTRACTS: Dict[str, AgentContract] = {
    "deal_intake": AgentContract(
        agent_key="deal_intake",
        title="Deal Intake",
        category="intake",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "underwrite": AgentContract(
        agent_key="underwrite",
        title="Deal Underwrite",
        category="deal",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "rent_reasonableness": AgentContract(
        agent_key="rent_reasonableness",
        title="Rent Reasonableness",
        category="rent",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "hqs_precheck": AgentContract(
        agent_key="hqs_precheck",
        title="HQS Precheck",
        category="compliance",
        mode="mutate_requires_approval",
        llm_mode="hybrid",
        requires_human=True,
        allowed_entity_types=["rehab_task", "workflow_event", "checklist_item"],
        allowed_operations=["create", "update_status"],
        required_root_fields=["agent_key", "summary", "facts", "actions"],
        required_fact_fields=["property_id"],
        max_actions=25,
    ),
    "packet_builder": AgentContract(
        agent_key="packet_builder",
        title="Packet Builder",
        category="packet",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "photo_rehab": AgentContract(
        agent_key="photo_rehab",
        title="Rehab From Photos",
        category="rehab",
        mode="mutate_requires_approval",
        llm_mode="llm_vision",
        requires_human=True,
        allowed_entity_types=["rehab_task", "workflow_event"],
        allowed_operations=["create"],
        required_root_fields=["agent_key", "summary", "facts", "actions"],
        required_fact_fields=["property_id"],
        max_actions=30,
    ),
    "next_actions": AgentContract(
        agent_key="next_actions",
        title="Next Actions Planner",
        category="ops",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "timeline_nudger": AgentContract(
        agent_key="timeline_nudger",
        title="Timeline Nudger",
        category="ops",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "ops_judge": AgentContract(
        agent_key="ops_judge",
        title="Ops Judge",
        category="ops",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
    "trust_recompute": AgentContract(
        agent_key="trust_recompute",
        title="Trust Recompute",
        category="ops",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "reason"],
    ),
}


def get_contract(agent_key: str) -> AgentContract:
    key = canonical_agent_key(agent_key)
    contract = CONTRACTS.get(key)
    if contract is None:
        return AgentContract(
            agent_key=key,
            title=key.replace("_", " ").title(),
            category="misc",
            mode="recommend_only",
            llm_mode="deterministic",
            required_root_fields=["agent_key", "summary"],
        )
    return contract


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _validate_recommendations(
    recommendations: Any,
    contract: AgentContract,
    errors: List[str],
) -> None:
    if recommendations is None:
        return
    if not isinstance(recommendations, list):
        errors.append("recommendations must be a list")
        return
    for idx, rec in enumerate(recommendations):
        if not isinstance(rec, dict):
            errors.append(f"recommendations[{idx}] must be an object")
            continue
        for field in contract.required_recommendation_fields:
            if field not in rec:
                errors.append(f"recommendations[{idx}].{field} required")
            elif field == "type" and not _is_nonempty_str(rec.get("type")):
                errors.append(f"recommendations[{idx}].type must be a non-empty string")
            elif field == "reason" and not _is_nonempty_str(rec.get("reason")):
                errors.append(f"recommendations[{idx}].reason must be a non-empty string")


def _validate_actions(
    actions: Any,
    contract: AgentContract,
    errors: List[str],
) -> None:
    if contract.mode == "recommend_only":
        if actions not in (None, [], ()):
            errors.append("recommend_only agents cannot emit actions")
        return

    if not isinstance(actions, list):
        errors.append("actions must be a list")
        return

    if len(actions) > int(contract.max_actions):
        errors.append(f"actions exceeds max_actions={contract.max_actions}")

    for idx, action in enumerate(actions):
        if not isinstance(action, dict):
            errors.append(f"actions[{idx}] must be an object")
            continue

        entity_type = action.get("entity_type")
        op = action.get("op")
        data = action.get("data")

        if not _is_nonempty_str(entity_type):
            errors.append(f"actions[{idx}].entity_type required")
        elif contract.allowed_entity_types and str(entity_type) not in contract.allowed_entity_types:
            errors.append(
                f"actions[{idx}].entity_type '{entity_type}' not allowed for {contract.agent_key}"
            )

        if not _is_nonempty_str(op):
            errors.append(f"actions[{idx}].op required")
        elif contract.allowed_operations and str(op) not in contract.allowed_operations:
            errors.append(f"actions[{idx}].op '{op}' not allowed for {contract.agent_key}")

        if not isinstance(data, dict):
            errors.append(f"actions[{idx}].data must be an object")

        if not _is_nonempty_str(action.get("reason")):
            errors.append(f"actions[{idx}].reason required")

        if isinstance(data, dict):
            if entity_type == "rehab_task" and op == "create":
                if not _is_nonempty_str(data.get("title")):
                    errors.append(f"actions[{idx}].data.title required for rehab_task create")
            if entity_type == "workflow_event" and op == "create":
                if not _is_nonempty_str(data.get("event_type")):
                    errors.append(f"actions[{idx}].data.event_type required for workflow_event create")
            if entity_type == "checklist_item" and op == "update_status":
                if data.get("item_id") in (None, ""):
                    errors.append(f"actions[{idx}].data.item_id required for checklist_item update_status")
                if not _is_nonempty_str(data.get("status")):
                    errors.append(f"actions[{idx}].data.status required for checklist_item update_status")


def validate_agent_output(agent_key: str, output: dict[str, Any]) -> Tuple[bool, List[str]]:
    contract = get_contract(agent_key)
    errors: List[str] = []

    if not isinstance(output, dict):
        return False, ["agent output must be an object"]

    for field in contract.required_root_fields:
        if field not in output:
            errors.append(f"{field} required")

    out_key = canonical_agent_key(str(output.get("agent_key") or ""))
    expected_key = canonical_agent_key(agent_key)
    if out_key and out_key != expected_key:
        errors.append(f"agent_key mismatch: expected {expected_key}, got {out_key}")

    summary = output.get("summary")
    if "summary" in output and not _is_nonempty_str(summary):
        errors.append("summary must be a non-empty string")

    facts = output.get("facts")
    if facts is not None:
        if not isinstance(facts, dict):
            errors.append("facts must be an object")
        else:
            for field in contract.required_fact_fields:
                if field not in facts:
                    errors.append(f"facts.{field} required")

    _validate_recommendations(output.get("recommendations"), contract, errors)
    _validate_actions(output.get("actions"), contract, errors)

    confidence = output.get("confidence")
    if confidence is not None:
        try:
            conf = float(confidence)
            if conf < 0.0 or conf > 1.0:
                errors.append("confidence must be between 0 and 1")
        except Exception:
            errors.append("confidence must be numeric")

    citations = output.get("citations")
    if citations is not None and not isinstance(citations, list):
        errors.append("citations must be a list")

    return len(errors) == 0, errors
