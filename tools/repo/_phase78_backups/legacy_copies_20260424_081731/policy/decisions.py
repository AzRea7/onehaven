from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


DECISION_SAFE_FOR_USER_RELIANCE = "safe_for_user_reliance"
DECISION_SAFE_WITH_WARNINGS = "safe_with_warnings"
DECISION_MANUAL_REVIEW_REQUIRED = "manual_review_required"
DECISION_BLOCKED_MISSING_CRITICAL_COVERAGE = "blocked_missing_critical_coverage"
DECISION_BLOCKED_STALE_AUTHORITY = "blocked_stale_authority"
DECISION_BLOCKED_UNRESOLVED_CONFLICT = "blocked_unresolved_conflict"

CANONICAL_POLICY_DECISIONS: tuple[str, ...] = (
    DECISION_SAFE_FOR_USER_RELIANCE,
    DECISION_SAFE_WITH_WARNINGS,
    DECISION_MANUAL_REVIEW_REQUIRED,
    DECISION_BLOCKED_MISSING_CRITICAL_COVERAGE,
    DECISION_BLOCKED_STALE_AUTHORITY,
    DECISION_BLOCKED_UNRESOLVED_CONFLICT,
)


@dataclass(frozen=True)
class DecisionDefinition:
    name: str
    rank: int
    is_safe: bool
    is_blocking: bool
    requires_manual_intervention: bool
    description: str


DECISION_DEFINITIONS: dict[str, DecisionDefinition] = {
    DECISION_SAFE_FOR_USER_RELIANCE: DecisionDefinition(
        name=DECISION_SAFE_FOR_USER_RELIANCE,
        rank=100,
        is_safe=True,
        is_blocking=False,
        requires_manual_intervention=False,
        description="Suitable for downstream user reliance.",
    ),
    DECISION_SAFE_WITH_WARNINGS: DecisionDefinition(
        name=DECISION_SAFE_WITH_WARNINGS,
        rank=80,
        is_safe=True,
        is_blocking=False,
        requires_manual_intervention=False,
        description="Operationally usable, but carries explicit warnings or caveats.",
    ),
    DECISION_MANUAL_REVIEW_REQUIRED: DecisionDefinition(
        name=DECISION_MANUAL_REVIEW_REQUIRED,
        rank=50,
        is_safe=False,
        is_blocking=False,
        requires_manual_intervention=True,
        description="Needs operator review before reliance.",
    ),
    DECISION_BLOCKED_MISSING_CRITICAL_COVERAGE: DecisionDefinition(
        name=DECISION_BLOCKED_MISSING_CRITICAL_COVERAGE,
        rank=20,
        is_safe=False,
        is_blocking=True,
        requires_manual_intervention=True,
        description="Critical expected coverage is missing.",
    ),
    DECISION_BLOCKED_STALE_AUTHORITY: DecisionDefinition(
        name=DECISION_BLOCKED_STALE_AUTHORITY,
        rank=15,
        is_safe=False,
        is_blocking=True,
        requires_manual_intervention=True,
        description="Required authority exists but is too stale to rely on.",
    ),
    DECISION_BLOCKED_UNRESOLVED_CONFLICT: DecisionDefinition(
        name=DECISION_BLOCKED_UNRESOLVED_CONFLICT,
        rank=10,
        is_safe=False,
        is_blocking=True,
        requires_manual_intervention=True,
        description="Material conflict remains unresolved.",
    ),
}


def get_decision_definition(name: str | None) -> DecisionDefinition:
    if not name:
        return DECISION_DEFINITIONS[DECISION_MANUAL_REVIEW_REQUIRED]
    return DECISION_DEFINITIONS.get(str(name).strip().lower(), DECISION_DEFINITIONS[DECISION_MANUAL_REVIEW_REQUIRED])


def is_blocking_decision(name: str | None) -> bool:
    return get_decision_definition(name).is_blocking


def is_safe_decision(name: str | None) -> bool:
    return get_decision_definition(name).is_safe


def requires_manual_intervention(name: str | None) -> bool:
    return get_decision_definition(name).requires_manual_intervention


def most_severe_decision(*names: str | None) -> str:
    selected = DECISION_SAFE_FOR_USER_RELIANCE
    selected_rank = 10**9
    for name in names:
        definition = get_decision_definition(name)
        if definition.rank < selected_rank:
            selected = definition.name
            selected_rank = definition.rank
    return selected


def serialize_decisions() -> dict[str, dict[str, Any]]:
    return {name: asdict(item) for name, item in DECISION_DEFINITIONS.items()}