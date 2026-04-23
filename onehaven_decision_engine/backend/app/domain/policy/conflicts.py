from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


CONFLICT_NONE = "none"
CONFLICT_SAME_FAMILY_DUPLICATE = "same_family_duplicate"
CONFLICT_SAME_FAMILY_MATERIAL = "same_family_material"
CONFLICT_CROSS_FAMILY_MATERIAL = "cross_family_material"
CONFLICT_ROLE_MISMATCH = "role_mismatch"

SEVERITY_NONE = "none"
SEVERITY_WARNING = "warning"
SEVERITY_BLOCKING = "blocking"


@dataclass(frozen=True)
class ConflictDefinition:
    code: str
    severity: str
    blocking: bool
    description: str


CONFLICT_DEFINITIONS: dict[str, ConflictDefinition] = {
    CONFLICT_NONE: ConflictDefinition(
        code=CONFLICT_NONE,
        severity=SEVERITY_NONE,
        blocking=False,
        description="No conflict detected.",
    ),
    CONFLICT_SAME_FAMILY_DUPLICATE: ConflictDefinition(
        code=CONFLICT_SAME_FAMILY_DUPLICATE,
        severity=SEVERITY_WARNING,
        blocking=False,
        description="Multiple records agree or are functionally duplicates within the same family.",
    ),
    CONFLICT_SAME_FAMILY_MATERIAL: ConflictDefinition(
        code=CONFLICT_SAME_FAMILY_MATERIAL,
        severity=SEVERITY_BLOCKING,
        blocking=True,
        description="Same-family records materially disagree on a truth-bearing value.",
    ),
    CONFLICT_CROSS_FAMILY_MATERIAL: ConflictDefinition(
        code=CONFLICT_CROSS_FAMILY_MATERIAL,
        severity=SEVERITY_BLOCKING,
        blocking=True,
        description="Different families conflict in a way that changes decision meaning.",
    ),
    CONFLICT_ROLE_MISMATCH: ConflictDefinition(
        code=CONFLICT_ROLE_MISMATCH,
        severity=SEVERITY_WARNING,
        blocking=False,
        description="Operational or support-only evidence is being used against a truth-capable rule role.",
    ),
}


@dataclass(frozen=True)
class ConflictAssessment:
    code: str
    severity: str
    blocking: bool
    same_family: bool
    confidence_penalty: float
    explanation: str


def get_conflict_definition(code: str | None) -> ConflictDefinition:
    if not code:
        return CONFLICT_DEFINITIONS[CONFLICT_NONE]
    return CONFLICT_DEFINITIONS.get(str(code).strip().lower(), CONFLICT_DEFINITIONS[CONFLICT_NONE])


def is_blocking_conflict(code: str | None) -> bool:
    return get_conflict_definition(code).blocking


def assess_conflict(
    *,
    same_family: bool,
    value_a: Any,
    value_b: Any,
    role_a: str | None = None,
    role_b: str | None = None,
    both_truth_capable: bool = False,
) -> ConflictAssessment:
    norm_a = None if value_a is None else str(value_a).strip().lower()
    norm_b = None if value_b is None else str(value_b).strip().lower()

    if norm_a == norm_b:
        code = CONFLICT_SAME_FAMILY_DUPLICATE if same_family else CONFLICT_NONE
        definition = get_conflict_definition(code)
        return ConflictAssessment(
            code=definition.code,
            severity=definition.severity,
            blocking=definition.blocking,
            same_family=same_family,
            confidence_penalty=0.0,
            explanation=definition.description,
        )

    if role_a and role_b and str(role_a).strip().lower() != str(role_b).strip().lower():
        definition = get_conflict_definition(CONFLICT_ROLE_MISMATCH)
        return ConflictAssessment(
            code=definition.code,
            severity=definition.severity,
            blocking=definition.blocking,
            same_family=same_family,
            confidence_penalty=0.10,
            explanation=definition.description,
        )

    if both_truth_capable:
        code = CONFLICT_SAME_FAMILY_MATERIAL if same_family else CONFLICT_CROSS_FAMILY_MATERIAL
        definition = get_conflict_definition(code)
        return ConflictAssessment(
            code=definition.code,
            severity=definition.severity,
            blocking=definition.blocking,
            same_family=same_family,
            confidence_penalty=0.45,
            explanation=definition.description,
        )

    definition = get_conflict_definition(CONFLICT_ROLE_MISMATCH)
    return ConflictAssessment(
        code=definition.code,
        severity=definition.severity,
        blocking=definition.blocking,
        same_family=same_family,
        confidence_penalty=0.15,
        explanation="Conflict exists but at least one side is not truth-capable, so it downgrades confidence rather than fully blocking.",
    )


def merge_conflict_codes(codes: Iterable[str | None]) -> str:
    winner = CONFLICT_NONE
    winner_blocking = False
    for code in codes:
        definition = get_conflict_definition(code)
        if definition.blocking and not winner_blocking:
            winner = definition.code
            winner_blocking = True
        elif not winner_blocking and definition.code != CONFLICT_NONE:
            winner = definition.code
    return winner


def serialize_conflicts() -> dict[str, dict[str, Any]]:
    return {name: asdict(item) for name, item in CONFLICT_DEFINITIONS.items()}