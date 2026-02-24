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


CONTRACTS: Dict[str, AgentContract] = {
    "deal_intake": AgentContract(
        agent_key="deal_intake",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={"root": ["summary"]},
    ),
    "public_records_check": AgentContract(
        agent_key="public_records_check",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={"root": ["summary"]},
    ),
    "rent_reasonableness": AgentContract(
        agent_key="rent_reasonableness",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={"root": ["summary"]},
    ),
    "packet_builder": AgentContract(
        agent_key="packet_builder",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={"root": ["summary"]},
    ),
    "timeline_nudger": AgentContract(
        agent_key="timeline_nudger",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={"root": ["summary"]},
    ),

    # ✅ New Judge/Critic (recommend-only)
    "ops_judge": AgentContract(
        agent_key="ops_judge",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={"root": ["summary"]},
    ),

    "hqs_precheck": AgentContract(
        agent_key="hqs_precheck",
        mode="mutate_requires_approval",
        allowed_entity_types=["rehab_task", "workflow_event", "checklist_item"],
        allowed_operations=["create", "update_status"],
        required_fields={"root": ["summary", "actions"]},
    ),
}


def get_contract(agent_key: str) -> AgentContract:
    if agent_key not in CONTRACTS:
        return AgentContract(
            agent_key=agent_key,
            mode="recommend_only",
            allowed_entity_types=[],
            allowed_operations=[],
            required_fields={"root": ["summary"]},
        )
    return CONTRACTS[agent_key]


def validate_agent_output(agent_key: str, output_json: Any) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    c = get_contract(agent_key)

    if not isinstance(output_json, dict):
        return False, ["output must be an object"]

    req = c.required_fields.get("root", [])
    for f in req:
        if f not in output_json:
            errs.append(f"missing required field: {f}")

    actions = output_json.get("actions", None)

    if c.mode == "recommend_only":
        if isinstance(actions, list) and len(actions) > 0:
            errs.append("recommend_only agents may not emit actions[]")
        return (len(errs) == 0), errs

    if not isinstance(actions, list) or len(actions) == 0:
        errs.append("mutation agent must emit non-empty actions[]")
        return (len(errs) == 0), errs

    for i, a in enumerate(actions):
        if not isinstance(a, dict):
            errs.append(f"actions[{i}] must be an object")
            continue
        et = a.get("entity_type")
        op = a.get("op")
        data = a.get("data")
        if et not in c.allowed_entity_types:
            errs.append(f"actions[{i}] entity_type '{et}' not allowed")
        if op not in c.allowed_operations:
            errs.append(f"actions[{i}] op '{op}' not allowed")
        if not isinstance(data, dict):
            errs.append(f"actions[{i}].data must be an object")

    return (len(errs) == 0), errs
