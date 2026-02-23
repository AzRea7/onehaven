# onehaven_decision_engine/backend/app/domain/agents/contracts.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class AgentContract:
    agent_key: str

    # Modes:
    # - recommend_only: output can only contain "recommendations" and "summary"
    # - mutate_requires_approval: output may contain actions but they MUST be approved
    # - autonomous_mutate: output may contain actions applied automatically (keep this rare)
    mode: str  # recommend_only | mutate_requires_approval | autonomous_mutate

    allowed_entity_types: List[str]
    allowed_operations: List[str]

    # Optional field-level constraints per entity_type
    required_fields: Dict[str, List[str]]  # entity_type -> required keys in action["data"]


def _as_list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def _is_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_dict(v: Any) -> bool:
    return isinstance(v, dict)


# Phase-5 safe defaults:
# Your current agents are deterministic and SHOULD be recommend-only.
CONTRACTS: Dict[str, AgentContract] = {
    "deal_intake": AgentContract(
        agent_key="deal_intake",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={},
    ),
    "rent_reasonableness": AgentContract(
        agent_key="rent_reasonableness",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={},
    ),
    "hqs_precheck": AgentContract(
        agent_key="hqs_precheck",
        mode="recommend_only",
        allowed_entity_types=[],
        allowed_operations=[],
        required_fields={},
    ),

    # Examples you’ll likely add next:
    # "rehab_planner": mutate_requires_approval (creates RehabTask actions)
    # "valuation_nudger": recommend_only (suggests valuation update)
}


def get_contract(agent_key: str) -> AgentContract:
    c = CONTRACTS.get(agent_key)
    if c is None:
        # Unknown agents are treated as recommend-only with no actions allowed.
        return AgentContract(agent_key=agent_key, mode="recommend_only", allowed_entity_types=[], allowed_operations=[], required_fields={})
    return c


def validate_agent_output(agent_key: str, output: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Output shape we enforce:

    recommend-only:
      {
        "summary": "...",
        "recommendations": [ ... ]   # optional
      }

    mutate:
      {
        "summary": "...",
        "actions": [
          {
            "entity_type": "rehab_task",
            "op": "create" | "update" | ...
            "data": {...}
          }
        ],
        "recommendations": [...]
      }
    """
    errs: List[str] = []
    contract = get_contract(agent_key)

    if not _is_dict(output):
        return False, ["output must be an object/dict"]

    actions = _as_list(output.get("actions"))
    if contract.mode == "recommend_only":
        if actions:
            errs.append("actions not allowed for recommend_only agents")
        # allow summary/recommendations/anything else informational
        return (len(errs) == 0), errs

    # mutate modes
    for i, a in enumerate(actions):
        if not _is_dict(a):
            errs.append(f"actions[{i}] must be an object")
            continue
        et = a.get("entity_type")
        op = a.get("op")
        data = a.get("data")

        if not _is_str(et):
            errs.append(f"actions[{i}].entity_type must be a non-empty string")
            continue
        if not _is_str(op):
            errs.append(f"actions[{i}].op must be a non-empty string")
            continue
        if et not in contract.allowed_entity_types:
            errs.append(f"actions[{i}].entity_type '{et}' not permitted for agent '{agent_key}'")
        if op not in contract.allowed_operations:
            errs.append(f"actions[{i}].op '{op}' not permitted for agent '{agent_key}'")
        if not _is_dict(data):
            errs.append(f"actions[{i}].data must be an object")
            continue

        req = contract.required_fields.get(et, [])
        for k in req:
            if k not in data:
                errs.append(f"actions[{i}].data missing required field '{k}' for entity_type '{et}'")

    return (len(errs) == 0), errs