# backend/app/domain/agents/validate.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.domain.agents.contracts import get_contract, validate_agent_output as _contracts_validate_agent_output


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: list[str]


def _is_obj(x: Any) -> bool:
    return isinstance(x, dict)


def _is_list(x: Any) -> bool:
    return isinstance(x, list)


def _safe_type(x: Any) -> str:
    return type(x).__name__


def _as_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _validate_action_shape(a: Any, idx: int) -> list[str]:
    errs: list[str] = []
    if not isinstance(a, dict):
        errs.append(f"actions[{idx}] must be object; got {_safe_type(a)}")
        return errs

    # Required keys
    entity_type = a.get("entity_type")
    op = a.get("op")
    data = a.get("data")

    if not isinstance(entity_type, str) or not entity_type.strip():
        errs.append(f"actions[{idx}].entity_type required (string)")
    if not isinstance(op, str) or not op.strip():
        errs.append(f"actions[{idx}].op required (string)")
    if not isinstance(data, dict):
        errs.append(f"actions[{idx}].data required (object)")

    # Optional keys sanity
    if "reason" in a and a["reason"] is not None and not isinstance(a["reason"], str):
        errs.append(f"actions[{idx}].reason must be string if present")

    return errs


def validate_actions(agent_key: str, actions: Any) -> ValidationResult:
    """
    Validates proposed actions against the agent contract.

    This is *structure + contract* validation:
      - actions must be a list[dict]
      - each action requires: entity_type, op, data
      - entity_type/op must be allowed by contract
      - data must be an object (we do NOT deep-validate fields here; that happens in apply layer)

    Designed to be used as defense-in-depth before persisting proposed_actions_json
    or before applying actions.
    """
    errs: list[str] = []
    contract = get_contract(str(agent_key))

    if actions is None:
        return ValidationResult(ok=True, errors=[])

    if not isinstance(actions, list):
        return ValidationResult(ok=False, errors=[f"actions must be a list; got {_safe_type(actions)}"])

    allowed_entities = set(getattr(contract, "allowed_entity_types", []) or [])
    allowed_ops = set(getattr(contract, "allowed_operations", []) or [])

    for i, a in enumerate(actions):
        errs.extend(_validate_action_shape(a, i))
        if not isinstance(a, dict):
            continue

        et = str(a.get("entity_type") or "")
        op = str(a.get("op") or "")

        if et and allowed_entities and et not in allowed_entities:
            errs.append(f"actions[{i}].entity_type '{et}' not allowed (allowed={sorted(allowed_entities)})")
        if op and allowed_ops and op not in allowed_ops:
            errs.append(f"actions[{i}].op '{op}' not allowed (allowed={sorted(allowed_ops)})")

    return ValidationResult(ok=(len(errs) == 0), errors=errs)


def normalize_agent_output(output: Any) -> dict[str, Any]:
    """
    Normalizes an arbitrary output into a dict.
    - If it's already a dict: return as-is
    - If it's a JSON string: attempt decode
    - Otherwise: wrap into {"raw": "..."}
    """
    if isinstance(output, dict):
        return output

    if isinstance(output, str):
        s = output.strip()
        if not s:
            return {}
        try:
            decoded = json.loads(s)
            if isinstance(decoded, dict):
                return decoded
            # If it's valid JSON but not an object, wrap it.
            return {"raw": decoded}
        except Exception:
            return {"raw": s}

    if output is None:
        return {}

    return {"raw": str(output)}


def validate_output(agent_key: str, output: Any) -> ValidationResult:
    """
    Wrapper around contracts.validate_agent_output() that:
      - normalizes output to dict
      - returns ValidationResult
    """
    out = normalize_agent_output(output)
    ok, errs = _contracts_validate_agent_output(str(agent_key), out)
    return ValidationResult(ok=bool(ok), errors=list(errs or []))


def validate_output_and_actions(agent_key: str, output: Any) -> ValidationResult:
    """
    Convenience:
      1) validate output contract (summary/actions/recommendations schema, etc)
      2) if output contains actions, validate those actions against contract allow-lists
    """
    out = normalize_agent_output(output)

    base = validate_output(agent_key, out)
    if not base.ok:
        return base

    actions = out.get("actions")
    if actions is None:
        return ValidationResult(ok=True, errors=[])

    act = validate_actions(agent_key, actions)
    if not act.ok:
        # attach errors with context, but keep them readable
        return ValidationResult(ok=False, errors=[*act.errors])

    return ValidationResult(ok=True, errors=[])