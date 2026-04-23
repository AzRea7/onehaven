from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class FixPlanStep:
    title: str
    category: str
    urgency: str
    blocking: bool
    estimated_cost: float
    reason: str
    source: str
    task_status: str | None = None
    deadline: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _task_urgency(task: Any) -> str:
    raw = str(getattr(task, "priority", None) or getattr(task, "urgency", None) or "").strip().lower()
    if raw in {"critical", "high", "urgent"}:
        return "immediate"
    if raw in {"medium", "soon"}:
        return "near_term"
    return "standard"


def _blocking_category(category: str) -> bool:
    return category in {
        "inspection",
        "occupancy",
        "lead",
        "registration",
        "rental_license",
        "section8",
        "program_overlay",
        "source_of_income",
        "permits",
        "safety",
    }


def build_fix_plan(
    *,
    rehab_tasks: Iterable[Any] | None,
    missing_critical_requirements: Iterable[str] | None = None,
    unresolved_categories: Iterable[str] | None = None,
    inspection_findings: Iterable[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    steps: list[FixPlanStep] = []

    for task in rehab_tasks or []:
        status = str(getattr(task, "status", "") or "").strip().lower()
        if status in {"done", "completed", "cancelled"}:
            continue
        category = str(getattr(task, "category", None) or "general").strip().lower()
        steps.append(
            FixPlanStep(
                title=str(getattr(task, "title", None) or "Remediation task"),
                category=category,
                urgency=_task_urgency(task),
                blocking=_blocking_category(category),
                estimated_cost=_safe_float(getattr(task, "cost_estimate", None), 0.0),
                reason=str(getattr(task, "description", None) or "Open remediation task."),
                source="rehab_task",
                task_status=status or None,
                deadline=getattr(task, "deadline", None).isoformat() if getattr(task, "deadline", None) else None,
            )
        )

    for category in missing_critical_requirements or []:
        text = str(category).strip().lower()
        if not text:
            continue
        steps.append(
            FixPlanStep(
                title=f"Resolve {text.replace('_', ' ')} requirement",
                category=text,
                urgency="immediate",
                blocking=True,
                estimated_cost=250.0 if text in {"registration", "documents", "contacts"} else 750.0,
                reason="Critical compliance requirement is still missing.",
                source="missing_requirement",
            )
        )

    for category in unresolved_categories or []:
        text = str(category).strip().lower()
        if not text:
            continue
        steps.append(
            FixPlanStep(
                title=f"Review unresolved {text.replace('_', ' ')} issue",
                category=text,
                urgency="near_term",
                blocking=_blocking_category(text),
                estimated_cost=150.0,
                reason="Compliance category is unresolved or incomplete.",
                source="unresolved_category",
            )
        )

    for finding in inspection_findings or []:
        code = str(finding.get("code") or "").strip().lower()
        if not code:
            continue
        steps.append(
            FixPlanStep(
                title=str(finding.get("label") or code.replace("_", " ").title()),
                category=code,
                urgency="immediate" if bool(finding.get("blocking")) else "near_term",
                blocking=bool(finding.get("blocking")),
                estimated_cost=_safe_float(finding.get("estimated_cost"), 300.0 if bool(finding.get("blocking")) else 125.0),
                reason=str(finding.get("reason") or "Inspection-related issue requires remediation."),
                source="inspection_risk",
            )
        )

    def sort_key(step: FixPlanStep) -> tuple[int, int, float, str]:
        urgency_rank = {"immediate": 0, "near_term": 1, "standard": 2}.get(step.urgency, 3)
        return (0 if step.blocking else 1, urgency_rank, -step.estimated_cost, step.title.lower())

    steps = sorted(steps, key=sort_key)

    total_cost = round(sum(step.estimated_cost for step in steps), 2)
    blocking_steps = [step.to_dict() for step in steps if step.blocking]
    non_blocking_steps = [step.to_dict() for step in steps if not step.blocking]

    return {
        "steps": [step.to_dict() for step in steps],
        "blocking_steps": blocking_steps,
        "non_blocking_steps": non_blocking_steps,
        "total_estimated_cost": total_cost,
        "blocking_step_count": len(blocking_steps),
        "non_blocking_step_count": len(non_blocking_steps),
    }
