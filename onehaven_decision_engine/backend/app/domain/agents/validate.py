from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

from app.domain.agents.contracts import get_contract, validate_agent_output as _validate


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: List[str]


def validate_agent_output(agent_key: str, output_json: Any) -> ValidationResult:
    ok, errors = _validate(agent_key, output_json)
    return ValidationResult(ok=ok, errors=errors)


def ensure_valid_agent_output(agent_key: str, output_json: Any) -> None:
    result = validate_agent_output(agent_key, output_json)
    if not result.ok:
        joined = "; ".join(result.errors) or "unknown validation error"
        raise ValueError(f"Invalid output for {agent_key}: {joined}")


def summarize_contract(agent_key: str) -> dict[str, Any]:
    contract = get_contract(agent_key)
    return {
        "agent_key": contract.agent_key,
        "mode": contract.mode,
        "llm_mode": contract.llm_mode,
        "requires_human": contract.requires_human,
        "allowed_entity_types": list(contract.allowed_entity_types),
        "allowed_operations": list(contract.allowed_operations),
        "required_root_fields": list(contract.required_root_fields),
        "required_fact_fields": list(contract.required_fact_fields),
        "max_actions": int(contract.max_actions),
    }
