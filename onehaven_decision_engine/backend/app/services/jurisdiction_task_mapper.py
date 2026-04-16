# backend/app/services/jurisdiction_task_mapper.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from ..domain.jurisdiction_categories import normalize_categories
from ..policy_models import JurisdictionProfile


TASK_PRIORITY_HIGH = "high"
TASK_PRIORITY_MEDIUM = "medium"
TASK_PRIORITY_LOW = "low"

TASK_STATUS_TODO = "todo"


@dataclass(frozen=True)
class JurisdictionTask:
    task_key: str
    title: str
    category: str
    status: str
    priority: str
    reason: str
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_key": self.task_key,
            "title": self.title,
            "category": self.category,
            "status": self.status,
            "priority": self.priority,
            "reason": self.reason,
            "metadata": self.metadata,
        }


def _loads_json_list(value: Any) -> list[Any]:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    return []


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _scope_label(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
) -> str:
    if city and state:
        return f"{city}, {state}"
    if county and state:
        return f"{county} County, {state}"
    if pha_name:
        return pha_name
    if state:
        return state
    return "jurisdiction"


def _missing_category_title(category: str) -> str:
    mapping = {
        "rental_license": "Verify rental license requirement",
        "registration": "Verify rental registration requirement",
        "inspection": "Confirm inspection workflow and cadence",
        "section8": "Confirm Section 8 / PHA policy coverage",
        "safety": "Validate safety compliance requirements",
        "lead": "Validate lead-based paint requirements",
        "permits": "Confirm permit and rehab approval requirements",
        "zoning": "Confirm zoning / use restrictions",
        "tax": "Confirm local tax obligations",
        "utilities": "Confirm utility responsibility rules",
        "occupancy": "Confirm occupancy / certificate rules",
    }
    return mapping.get(category, f"Verify {category.replace('_', ' ')} requirements")


def _missing_category_reason(category: str, scope_label: str) -> str:
    mapping = {
        "rental_license": f"{scope_label} is missing verified rental license coverage.",
        "registration": f"{scope_label} is missing rental registration coverage.",
        "inspection": f"{scope_label} is missing inspection authority or cadence coverage.",
        "section8": f"{scope_label} is missing verified PHA / Section 8 operational coverage.",
        "safety": f"{scope_label} is missing safety-rule coverage.",
        "lead": f"{scope_label} is missing lead-risk coverage.",
        "permits": f"{scope_label} is missing permit workflow coverage.",
        "zoning": f"{scope_label} is missing zoning coverage.",
        "tax": f"{scope_label} is missing tax-rule coverage.",
        "utilities": f"{scope_label} is missing utility coverage.",
        "occupancy": f"{scope_label} is missing occupancy-rule coverage.",
    }
    return mapping.get(category, f"{scope_label} is missing {category.replace('_', ' ')} coverage.")


def _priority_for_missing_category(category: str) -> str:
    if category in {"rental_license", "inspection", "section8", "safety"}:
        return TASK_PRIORITY_HIGH
    if category in {"registration", "lead", "permits", "occupancy"}:
        return TASK_PRIORITY_MEDIUM
    return TASK_PRIORITY_LOW

def build_jurisdiction_tasks(
    *,
    market: dict[str, Any],
    required_categories: Iterable[str],
    category_coverage: dict[str, Any],
    stale_status: str = "fresh",
) -> dict[str, Any]:
    state = market.get("state")
    county = market.get("county")
    city = market.get("city")
    pha_name = market.get("pha_name")

    required = normalize_categories(required_categories)
    actions: list[dict[str, Any]] = []

    for category in required:
        status = str((category_coverage or {}).get(category) or "missing").strip().lower()
        if status not in {"missing", "conditional"}:
            continue

        task = build_missing_category_task(
            category=category,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )

        actions.append(
            {
                "code": f"resolve_{category}",
                "title": task.title,
                "category": category,
                "status": task.status,
                "priority": task.priority,
                "reason": task.reason,
                "metadata": task.metadata,
            }
        )

    if str(stale_status or "").strip().lower() == "stale":
        task = build_refresh_task(
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            stale_reason="source_freshness_expired",
        )
        actions.append(
            {
                "code": "refresh_stale_rules",
                "title": task.title,
                "category": "jurisdiction",
                "status": task.status,
                "priority": task.priority,
                "reason": task.reason,
                "metadata": task.metadata,
            }
        )

    return {
        "ok": True,
        "market": dict(market or {}),
        "required_categories": required,
        "category_coverage": dict(category_coverage or {}),
        "stale_status": stale_status,
        "required_actions": actions,
        "count": len(actions),
    }

def build_missing_category_task(
    *,
    category: str,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    jurisdiction_profile_id: int | None = None,
) -> JurisdictionTask:
    scope_label = _scope_label(state=state, county=county, city=city, pha_name=pha_name)

    return JurisdictionTask(
        task_key=f"jurisdiction_missing_category:{category}",
        title=_missing_category_title(category),
        category="jurisdiction",
        status=TASK_STATUS_TODO,
        priority=_priority_for_missing_category(category),
        reason=_missing_category_reason(category, scope_label),
        metadata={
            "task_type": "jurisdiction_missing_category",
            "normalized_category": category,
            "state": state,
            "county": county,
            "city": city,
            "pha_name": pha_name,
            "jurisdiction_profile_id": jurisdiction_profile_id,
            "scope_label": scope_label,
        },
    )


def build_refresh_task(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    stale_reason: str | None,
    pha_name: str | None = None,
    jurisdiction_profile_id: int | None = None,
) -> JurisdictionTask:
    scope_label = _scope_label(state=state, county=county, city=city, pha_name=pha_name)
    reason = (
        f"{scope_label} jurisdiction data is stale."
        if not stale_reason
        else f"{scope_label} jurisdiction data is stale ({stale_reason})."
    )

    return JurisdictionTask(
        task_key="jurisdiction_refresh_required",
        title="Refresh jurisdiction policy sources",
        category="jurisdiction",
        status=TASK_STATUS_TODO,
        priority=TASK_PRIORITY_HIGH,
        reason=reason,
        metadata={
            "task_type": "jurisdiction_refresh_required",
            "state": state,
            "county": county,
            "city": city,
            "pha_name": pha_name,
            "jurisdiction_profile_id": jurisdiction_profile_id,
            "stale_reason": stale_reason,
            "scope_label": scope_label,
        },
    )


def build_completeness_review_task(
    *,
    completeness_status: str,
    completeness_score: float,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    jurisdiction_profile_id: int | None = None,
) -> JurisdictionTask:
    scope_label = _scope_label(state=state, county=county, city=city, pha_name=pha_name)

    if completeness_status == "missing":
        title = "Build initial jurisdiction policy coverage"
        priority = TASK_PRIORITY_HIGH
    else:
        title = "Complete jurisdiction policy coverage"
        priority = TASK_PRIORITY_MEDIUM

    return JurisdictionTask(
        task_key="jurisdiction_completeness_review",
        title=title,
        category="jurisdiction",
        status=TASK_STATUS_TODO,
        priority=priority,
        reason=(
            f"{scope_label} jurisdiction coverage is {completeness_status} "
            f"({round(float(completeness_score or 0.0) * 100)}% complete)."
        ),
        metadata={
            "task_type": "jurisdiction_completeness_review",
            "completeness_status": completeness_status,
            "completeness_score": float(completeness_score or 0.0),
            "state": state,
            "county": county,
            "city": city,
            "pha_name": pha_name,
            "jurisdiction_profile_id": jurisdiction_profile_id,
            "scope_label": scope_label,
        },
    )


def build_escalation_task(
    *,
    escalation_code: str,
    title: str,
    reason: str,
    priority: str,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    jurisdiction_profile_id: int | None = None,
    categories: Iterable[Any] | None = None,
) -> JurisdictionTask:
    return JurisdictionTask(
        task_key=f"jurisdiction_escalation:{escalation_code}",
        title=title,
        category="jurisdiction_review",
        status=TASK_STATUS_TODO,
        priority=priority,
        reason=reason,
        metadata={
            "task_type": "jurisdiction_escalation",
            "escalation_code": escalation_code,
            "categories": normalize_categories(categories),
            "state": state,
            "county": county,
            "city": city,
            "pha_name": pha_name,
            "jurisdiction_profile_id": jurisdiction_profile_id,
            "scope_label": _scope_label(state=state, county=county, city=city, pha_name=pha_name),
        },
    )


def dedupe_tasks(tasks: Iterable[JurisdictionTask]) -> list[JurisdictionTask]:
    seen: set[str] = set()
    deduped: list[JurisdictionTask] = []

    for task in tasks:
        key = task.task_key
        if key in seen:
            continue
        seen.add(key)
        deduped.append(task)

    return dedupe_tasks(tasks)


def map_jurisdiction_tasks(
    *,
    completeness_status: str,
    completeness_score: float,
    missing_categories: Iterable[Any] | None,
    is_stale: bool,
    stale_reason: str | None,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    jurisdiction_profile_id: int | None = None,
    critical_categories: Iterable[Any] | None = None,
    stale_categories: Iterable[Any] | None = None,
    conflicting_categories: Iterable[Any] | None = None,
    discovery_retries_exhausted: bool = False,
) -> list[JurisdictionTask]:
    tasks: list[JurisdictionTask] = []

    normalized_missing = normalize_categories(missing_categories)
    normalized_critical = normalize_categories(critical_categories)
    normalized_stale_categories = normalize_categories(stale_categories)
    normalized_conflicting = normalize_categories(conflicting_categories)

    if completeness_status in {"missing", "partial"}:
        tasks.append(
            build_completeness_review_task(
                completeness_status=completeness_status,
                completeness_score=completeness_score,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
            )
        )

    for category in normalized_missing:
        tasks.append(
            build_missing_category_task(
                category=category,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
            )
        )

    if is_stale:
        tasks.append(
            build_refresh_task(
                state=state,
                county=county,
                city=city,
                stale_reason=stale_reason,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
            )
        )

    critical_missing = [category for category in normalized_missing if category in set(normalized_critical)]
    if critical_missing:
        tasks.append(
            build_escalation_task(
                escalation_code="critical_category_missing",
                title="Escalate critical jurisdiction gap",
                reason=(
                    f"{_scope_label(state=state, county=county, city=city, pha_name=pha_name)} "
                    f"is missing critical jurisdiction coverage."
                ),
                priority=TASK_PRIORITY_HIGH,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
                categories=critical_missing,
            )
        )

    critical_stale = [category for category in normalized_stale_categories if category in set(normalized_critical)]
    if critical_stale:
        tasks.append(
            build_escalation_task(
                escalation_code="critical_category_stale",
                title="Escalate stale critical jurisdiction coverage",
                reason=(
                    f"{_scope_label(state=state, county=county, city=city, pha_name=pha_name)} "
                    f"has stale coverage in critical categories."
                ),
                priority=TASK_PRIORITY_HIGH,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
                categories=critical_stale,
            )
        )

    if normalized_conflicting:
        tasks.append(
            build_escalation_task(
                escalation_code="authoritative_conflict",
                title="Resolve authoritative jurisdiction conflict",
                reason=(
                    f"{_scope_label(state=state, county=county, city=city, pha_name=pha_name)} "
                    f"has conflicting jurisdiction evidence that requires operator review."
                ),
                priority=TASK_PRIORITY_HIGH,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
                categories=normalized_conflicting,
            )
        )

    if discovery_retries_exhausted:
        tasks.append(
            build_escalation_task(
                escalation_code="discovery_retries_exhausted",
                title="Review exhausted jurisdiction discovery retries",
                reason=(
                    f"{_scope_label(state=state, county=county, city=city, pha_name=pha_name)} "
                    f"exhausted automated discovery retries without resolving coverage gaps."
                ),
                priority=TASK_PRIORITY_MEDIUM,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                jurisdiction_profile_id=jurisdiction_profile_id,
                categories=[],
            )
        )

    return deduped_tasks(tasks)


def deduped_tasks(tasks: Iterable[JurisdictionTask]) -> list[JurisdictionTask]:
    seen: set[str] = set()
    deduped: list[JurisdictionTask] = []

    for task in tasks:
        key = task.task_key
        if key in seen:
            continue
        seen.add(key)
        deduped.append(task)

    return deduped


def _profile_meta(profile: JurisdictionProfile) -> dict[str, Any]:
    try:
        payload = json.loads(getattr(profile, "policy_json", None) or "{}")
    except Exception:
        payload = {}
    meta = payload.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _profile_critical_categories(profile: JurisdictionProfile) -> list[str]:
    meta = _profile_meta(profile)
    completeness_meta = meta.get("completeness") or {}
    candidates = (
        completeness_meta.get("critical_categories")
        or meta.get("critical_categories")
        or []
    )
    return normalize_categories(candidates)


def _profile_stale_categories(profile: JurisdictionProfile) -> list[str]:
    meta = _profile_meta(profile)
    completeness_meta = meta.get("completeness") or {}
    source_freshness = _loads_json_dict(getattr(profile, "source_freshness_json", None))
    scoring = source_freshness.get("scoring") or {}
    candidates = (
        completeness_meta.get("stale_categories")
        or scoring.get("stale_categories")
        or []
    )
    return normalize_categories(candidates)


def _profile_conflicting_categories(profile: JurisdictionProfile) -> list[str]:
    meta = _profile_meta(profile)
    completeness_meta = meta.get("completeness") or {}
    source_freshness = _loads_json_dict(getattr(profile, "source_freshness_json", None))
    scoring = source_freshness.get("scoring") or {}
    candidates = (
        completeness_meta.get("conflicting_categories")
        or scoring.get("conflicting_categories")
        or []
    )
    return normalize_categories(candidates)


def _profile_discovery_retries_exhausted(profile: JurisdictionProfile) -> bool:
    source_freshness = _loads_json_dict(getattr(profile, "source_freshness_json", None))
    scoring = source_freshness.get("scoring") or {}
    for candidate in (
        source_freshness.get("discovery_retries_exhausted"),
        source_freshness.get("retry_exhausted"),
        source_freshness.get("retries_exhausted"),
        scoring.get("discovery_retries_exhausted"),
        scoring.get("retry_exhausted"),
        scoring.get("retries_exhausted"),
    ):
        if candidate is True:
            return True
    retry_count = 0
    max_retry_count = 0
    try:
        retry_count = int(source_freshness.get("discovery_retry_count") or 0)
    except Exception:
        retry_count = 0
    try:
        max_retry_count = int(
            source_freshness.get("discovery_retry_max")
            or source_freshness.get("discovery_max_retries")
            or 0
        )
    except Exception:
        max_retry_count = 0
    return max_retry_count > 0 and retry_count >= max_retry_count


def map_profile_jurisdiction_tasks(profile: Optional[JurisdictionProfile]) -> list[JurisdictionTask]:
    if profile is None:
        return []

    missing_categories = _loads_json_list(getattr(profile, "missing_categories_json", None))

    return map_jurisdiction_tasks(
        completeness_status=(getattr(profile, "completeness_status", None) or "missing").strip().lower(),
        completeness_score=float(getattr(profile, "completeness_score", 0.0) or 0.0),
        missing_categories=missing_categories,
        is_stale=bool(getattr(profile, "is_stale", False)),
        stale_reason=getattr(profile, "stale_reason", None),
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
        jurisdiction_profile_id=getattr(profile, "id", None),
        critical_categories=_profile_critical_categories(profile),
        stale_categories=_profile_stale_categories(profile),
        conflicting_categories=_profile_conflicting_categories(profile),
        discovery_retries_exhausted=_profile_discovery_retries_exhausted(profile),
    )


def map_profile_jurisdiction_task_dicts(profile: Optional[JurisdictionProfile]) -> list[dict[str, Any]]:
    return [task.to_dict() for task in map_profile_jurisdiction_tasks(profile)]


# ---- Chunk 5 task helpers ----

def build_rule_gap_task(
    *,
    rule_key: str,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    jurisdiction_profile_id: int | None = None,
) -> JurisdictionTask:
    scope_label = _scope_label(state=state, county=county, city=city, pha_name=pha_name)
    return JurisdictionTask(
        task_key=f'jurisdiction_missing_rule:{rule_key}',
        title=f'Verify rule: {rule_key.replace("_", " ")}',
        category='jurisdiction',
        status=TASK_STATUS_TODO,
        priority=TASK_PRIORITY_HIGH,
        reason=f'{scope_label} is missing direct evidence for rule {rule_key}.',
        metadata={
            'task_type': 'jurisdiction_missing_rule',
            'rule_key': rule_key,
            'state': state,
            'county': county,
            'city': city,
            'pha_name': pha_name,
            'jurisdiction_profile_id': jurisdiction_profile_id,
        },
    )


_base_map_profile_jurisdiction_task_dicts = map_profile_jurisdiction_task_dicts


def map_profile_jurisdiction_task_dicts(profile: Optional[JurisdictionProfile]) -> list[dict[str, Any]]:
    rows = _base_map_profile_jurisdiction_task_dicts(profile)
    if profile is None:
        return rows
    policy = {}
    try:
        policy = json.loads(getattr(profile, 'policy_json', None) or '{}')
    except Exception:
        policy = {}
    missing_rule_keys = list(policy.get('missing_rule_keys') or [])
    extra = [
        build_rule_gap_task(
            rule_key=rule_key,
            state=getattr(profile, 'state', None),
            county=getattr(profile, 'county', None),
            city=getattr(profile, 'city', None),
            pha_name=getattr(profile, 'pha_name', None),
            jurisdiction_profile_id=getattr(profile, 'id', None),
        ).to_dict()
        for rule_key in missing_rule_keys
    ]
    combined = rows + extra
    deduped = []
    seen = set()
    for row in combined:
        key = row.get('task_key')
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped