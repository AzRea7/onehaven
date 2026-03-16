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


CONTRACTS: Dict[str, AgentContract] = {
    "deal_underwrite": AgentContract(
        agent_key="deal_underwrite",
        title="Deal Underwrite",
        category="deal",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "title", "reason"],
    ),
    "rent_reasonableness": AgentContract(
        agent_key="rent_reasonableness",
        title="Rent Reasonableness",
        category="rent",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "title", "reason"],
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
        max_actions=30,
    ),
    "packet_builder": AgentContract(
        agent_key="packet_builder",
        title="Packet Builder",
        category="packet",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "title", "reason"],
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
        required_fact_fields=["property_id", "photo_count"],
        max_actions=40,
    ),
    "trust_recompute": AgentContract(
        agent_key="trust_recompute",
        title="Trust Recompute",
        category="trust",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "title", "reason"],
    ),
    "next_actions": AgentContract(
        agent_key="next_actions",
        title="Next Actions Planner",
        category="ops",
        mode="recommend_only",
        llm_mode="hybrid",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
        required_fact_fields=["property_id"],
        required_recommendation_fields=["type", "title", "reason"],
    ),
    # backwards-compatible aliases so old runs and URLs do not implode like a sad toaster.
    "deal_intake": AgentContract(
        agent_key="deal_intake",
        title="Deal Intake",
        category="deal",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
    ),
    "timeline_nudger": AgentContract(
        agent_key="timeline_nudger",
        title="Timeline Nudger",
        category="ops",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
    ),
    "ops_judge": AgentContract(
        agent_key="ops_judge",
        title="Ops Judge",
        category="ops",
        mode="recommend_only",
        llm_mode="deterministic",
        required_root_fields=["agent_key", "summary", "facts", "recommendations"],
    ),
}


DEFAULT_CONTRACT = AgentContract(
    agent_key="unknown",
    title="Unknown",
    category="misc",
    mode="recommend_only",
    llm_mode="deterministic",
    required_root_fields=["agent_key", "summary"],
)


VALID_RECOMMENDATION_PRIORITIES = {"critical", "high", "medium", "low"}


def get_contract(agent_key: str) -> AgentContract:
    return CONTRACTS.get(str(agent_key or "").strip(), DEFAULT_CONTRACT)


def _validate_action_shape(contract: AgentContract, actions: Any) -> List[str]:
    errors: List[str] = []

    if contract.mode == "recommend_only":
        if isinstance(actions, list) and actions:
            errors.append("recommend_only agent may not emit actions")
        return errors

    if not isinstance(actions, list) or not actions:
        errors.append("mutation agent must emit non-empty actions list")
        return errors

    if len(actions) > int(contract.max_actions):
        errors.append(f"actions exceeds max_actions={contract.max_actions}")

    for i, action in enumerate(actions):
        if not isinstance(action, dict):
            errors.append(f"actions[{i}] must be object")
            continue
        entity_type = str(action.get("entity_type") or "").strip()
        op = str(action.get("op") or "").strip()
        data = action.get("data")
        if entity_type not in contract.allowed_entity_types:
            errors.append(f"actions[{i}] entity_type '{entity_type}' not allowed")
        if op not in contract.allowed_operations:
            errors.append(f"actions[{i}] op '{op}' not allowed")
        if not isinstance(data, dict):
            errors.append(f"actions[{i}].data must be object")
    return errors


def validate_agent_output(agent_key: str, output_json: Any) -> Tuple[bool, List[str]]:
    contract = get_contract(agent_key)
    errors: List[str] = []

    if not isinstance(output_json, dict):
        return False, ["output must be object"]

    for field in contract.required_root_fields:
        if field not in output_json:
            errors.append(f"missing required field: {field}")

    facts = output_json.get("facts", {})
    if contract.required_fact_fields:
        if not isinstance(facts, dict):
            errors.append("facts must be object")
        else:
            for field in contract.required_fact_fields:
                if field not in facts:
                    errors.append(f"facts missing required field: {field}")

    recs = output_json.get("recommendations", [])
    if recs is not None and not isinstance(recs, list):
        errors.append("recommendations must be list")
    elif isinstance(recs, list):
        for idx, rec in enumerate(recs):
            if not isinstance(rec, dict):
                errors.append(f"recommendations[{idx}] must be object")
                continue
            for field in contract.required_recommendation_fields:
                if field not in rec:
                    errors.append(f"recommendations[{idx}] missing required field: {field}")
            priority = str(rec.get("priority") or "medium").lower()
            if priority not in VALID_RECOMMENDATION_PRIORITIES:
                errors.append(f"recommendations[{idx}] invalid priority '{priority}'")

    errors.extend(_validate_action_shape(contract, output_json.get("actions")))
    return len(errors) == 0, errors
