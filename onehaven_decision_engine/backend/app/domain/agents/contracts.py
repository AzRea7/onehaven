# backend/app/domain/agents/contracts.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class AgentContract:
    agent_key: str
    mode: str  # recommend_only | mutate_requires_approval | autonomous_mutate
    allowed_entity_types: List[str]
    allowed_operations: List[str]
    required_fields: Dict[str, List[str]]


def _as_list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def _is_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_dict(v: Any) -> bool:
    return isinstance(v, dict)


CONTRACTS: Dict[str, AgentContract] = {
    # SAFE defaults: recommend-only agents that create structured "actions" proposals.
    "deal_intake": AgentContract(
        agent_key="deal_intake",
        mode="recommend_only",
        allowed_entity_types=["WorkflowEvent"],
        allowed_operations=["recommend"],
        required_fields={"WorkflowEvent": ["event_type", "payload"]},
    ),
    "public_records_check": AgentContract(
        agent_key="public_records_check",
        mode="recommend_only",
        allowed_entity_types=["WorkflowEvent"],
        allowed_operations=["recommend"],
        required_fields={"WorkflowEvent": ["event_type", "payload"]},
    ),
    "rent_reasonableness": AgentContract(
        agent_key="rent_reasonableness",
        mode="recommend_only",
        allowed_entity_types=["WorkflowEvent"],
        allowed_operations=["recommend"],
        required_fields={"WorkflowEvent": ["event_type", "payload"]},
    ),
    "hqs_precheck": AgentContract(
        agent_key="hqs_precheck",
        mode="recommend_only",
        allowed_entity_types=["WorkflowEvent"],
        allowed_operations=["recommend"],
        required_fields={"WorkflowEvent": ["event_type", "payload"]},
    ),
    "packet_builder": AgentContract(
        agent_key="packet_builder",
        mode="recommend_only",
        allowed_entity_types=["WorkflowEvent"],
        allowed_operations=["recommend"],
        required_fields={"WorkflowEvent": ["event_type", "payload"]},
    ),
    "timeline_nudger": AgentContract(
        agent_key="timeline_nudger",
        mode="recommend_only",
        allowed_entity_types=["WorkflowEvent"],
        allowed_operations=["recommend"],
        required_fields={"WorkflowEvent": ["event_type", "payload"]},
    ),
}


def validate_agent_output(agent_key: str, output_json: Any) -> Tuple[bool, List[str]]:
    c = CONTRACTS.get(agent_key)
    if c is None:
        return False, [f"unknown agent_key={agent_key}"]

    if not _is_dict(output_json):
        return False, ["output must be a JSON object"]

    # Required top-level keys
    summary = output_json.get("summary")
    actions = output_json.get("actions")

    if not _is_str(summary):
        return False, ["output.summary must be a non-empty string"]

    if actions is not None and not isinstance(actions, list):
        return False, ["output.actions must be a list when provided"]

    errors: List[str] = []
    for i, a in enumerate(_as_list(actions)):
        if not _is_dict(a):
            errors.append(f"actions[{i}] must be an object")
            continue

        entity_type = a.get("entity_type")
        op = a.get("op")
        data = a.get("data")

        if entity_type not in c.allowed_entity_types:
            errors.append(f"actions[{i}].entity_type not allowed: {entity_type}")
        if op not in c.allowed_operations:
            errors.append(f"actions[{i}].op not allowed: {op}")
        if not _is_dict(data):
            errors.append(f"actions[{i}].data must be an object")

        req = c.required_fields.get(str(entity_type), [])
        for k in req:
            if not (isinstance(data, dict) and k in data):
                errors.append(f"actions[{i}].data missing required field: {k}")

    return (len(errors) == 0), errors