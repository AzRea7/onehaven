from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Optional

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session

from ..domain.workflow.panes import (
    PANES,
    allowed_panes_for_principal,
    clamp_pane,
    pane_catalog,
    pane_label,
)
from ..models import Deal, Property, UnderwritingResult
from ..services.property_state_machine import get_state_payload

log = logging.getLogger("onehaven.panes")


STANDARD_FILTER_KEYS = (
    "org",
    "city",
    "county",
    "assigned_user",
    "status",
    "stage",
    "urgency",
)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _safe_str(v: Any, default: str = "") -> str:
    try:
        if v is None:
            return default
        return str(v)
    except Exception:
        return default


def _normalize_status(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    return raw or None


def _standard_filters(
    *,
    org_id: int,
    city: Optional[str] = None,
    county: Optional[str] = None,
    assigned_user: Optional[int] = None,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    urgency: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "org": int(org_id),
        "city": _safe_str(city) or None,
        "county": _safe_str(county) or None,
        "assigned_user": assigned_user,
        "status": _normalize_status(status),
        "stage": _normalize_status(stage),
        "urgency": _normalize_status(urgency),
    }


def _infer_plan_name(principal: Any) -> str:
    for attr in ("plan", "plan_name", "subscription_plan", "tier"):
        value = getattr(principal, attr, None)
        if value:
            return str(value).strip().lower()
    return "basic"


def _plan_allows_advanced_fields(plan_name: str) -> bool:
    return plan_name in {"pro", "premium", "enterprise", "authority", "admin"}


def _build_plan_context(principal: Any) -> dict[str, Any]:
    plan_name = _infer_plan_name(principal)
    advanced = _plan_allows_advanced_fields(plan_name)
    return {
        "plan_name": plan_name,
        "advanced_fields_enabled": advanced,
        "gated_fields": [] if advanced else [
            "county_breakdown",
            "advanced_leaderboards",
            "portfolio_risk_details",
            "stale_item_reason_detail",
        ],
    }


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(Deal.updated_at), desc(Deal.id))
        .limit(1)
    )


def _latest_uw(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(UnderwritingResult.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


def _asking_price(prop: Property, deal: Optional[Deal]) -> Optional[float]:
    for attr in ("asking_price", "list_price", "price", "offer_price", "purchase_price"):
        if deal is not None and getattr(deal, attr, None) is not None:
            return _safe_float(getattr(deal, attr, None), 0.0)
    for attr in ("asking_price", "list_price", "price"):
        if getattr(prop, attr, None) is not None:
            return _safe_float(getattr(prop, attr, None), 0.0)
    return None


def _derive_urgency(
    *,
    gate_status: Optional[str],
    blockers: list[Any],
    jurisdiction_is_stale: bool,
    failed_count: int,
    blocked_count: int,
) -> str:
    gate = _normalize_status(gate_status)

    if gate == "blocked" or blocked_count > 0:
        return "critical"
    if failed_count > 0 or len(blockers) >= 3:
        return "high"
    if jurisdiction_is_stale or len(blockers) > 0:
        return "medium"
    return "low"


def _base_property_stmt(
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user: Optional[int] = None,
) -> Any:
    stmt = select(Property).where(Property.org_id == org_id)

    if state:
        stmt = stmt.where(Property.state == state)
    if county:
        stmt = stmt.where(func.lower(Property.county) == county.lower())
    if city:
        stmt = stmt.where(func.lower(Property.city) == city.lower())

    if q:
        like = f"%{q.strip().lower()}%"
        stmt = stmt.where(
            func.lower(
                func.concat(
                    func.coalesce(Property.address, ""),
                    " ",
                    func.coalesce(Property.city, ""),
                    " ",
                    func.coalesce(Property.state, ""),
                    " ",
                    func.coalesce(Property.zip, ""),
                )
            ).like(like)
        )

    if assigned_user is not None:
        candidate_columns = [
            "assigned_user_id",
            "owner_user_id",
            "manager_user_id",
            "agent_user_id",
            "acquisition_user_id",
        ]
        clauses = []
        for col_name in candidate_columns:
            if hasattr(Property, col_name):
                clauses.append(getattr(Property, col_name) == assigned_user)
        if clauses:
            stmt = stmt.where(or_(*clauses))

    return stmt.order_by(desc(Property.id))


def _property_matches_status(
    row: dict[str, Any],
    *,
    status: Optional[str],
) -> bool:
    if not status:
        return True

    wanted = _normalize_status(status)
    current_stage = _safe_str(row.get("current_stage")).lower()
    gate_status = _safe_str(row.get("gate_status")).lower()
    decision = _safe_str(row.get("normalized_decision")).lower()
    pane = _safe_str(row.get("current_pane")).lower()

    return wanted in {current_stage, gate_status, decision, pane}


def _property_matches_stage(
    row: dict[str, Any],
    *,
    stage: Optional[str],
) -> bool:
    if not stage:
        return True
    return _safe_str(row.get("current_stage")).lower() == _normalize_status(stage)


def _property_matches_urgency(
    row: dict[str, Any],
    *,
    urgency: Optional[str],
) -> bool:
    if not urgency:
        return True
    return _safe_str(row.get("urgency")).lower() == _normalize_status(urgency)


def _property_matches_pane(
    pane: str,
    *,
    current_pane: str,
) -> bool:
    if pane == "admin":
        return True
    return current_pane == pane


def _build_row(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    state_payload: dict[str, Any],
) -> dict[str, Any]:
    deal_row = _latest_deal(db, org_id=org_id, property_id=int(prop.id))
    uw_row = _latest_uw(db, org_id=org_id, property_id=int(prop.id))
    constraints = state_payload.get("constraints") or {}
    pane_info = constraints.get("pane") or {}
    jurisdiction = constraints.get("jurisdiction") or {}
    completion = constraints.get("completion") or {}
    inspection = constraints.get("inspection") or {}
    rehab = constraints.get("rehab") or {}
    counts = (state_payload.get("outstanding_tasks") or {}).get("counts") or {}
    blockers = (state_payload.get("outstanding_tasks") or {}).get("blockers") or []
    failed_count = _safe_int(completion.get("failed_count"), 0)
    blocked_count = _safe_int(completion.get("blocked_count"), 0)
    jurisdiction_is_stale = bool(jurisdiction.get("is_stale"))

    urgency = _derive_urgency(
        gate_status=state_payload.get("gate_status"),
        blockers=blockers,
        jurisdiction_is_stale=jurisdiction_is_stale,
        failed_count=failed_count,
        blocked_count=blocked_count,
    )

    return {
        "property_id": int(prop.id),
        "address": getattr(prop, "address", None),
        "city": getattr(prop, "city", None),
        "state": getattr(prop, "state", None),
        "county": getattr(prop, "county", None),
        "zip": getattr(prop, "zip", None),
        "current_stage": state_payload.get("current_stage"),
        "current_stage_label": state_payload.get("current_stage_label"),
        "current_pane": state_payload.get("current_pane"),
        "current_pane_label": state_payload.get("current_pane_label"),
        "suggested_pane": state_payload.get("suggested_pane"),
        "route_reason": state_payload.get("route_reason"),
        "normalized_decision": state_payload.get("normalized_decision"),
        "gate_status": state_payload.get("gate_status"),
        "asking_price": _asking_price(prop, deal_row),
        "projected_monthly_cashflow": _safe_float(getattr(uw_row, "cash_flow", None), 0.0) if uw_row else None,
        "dscr": _safe_float(getattr(uw_row, "dscr", None), 0.0) if uw_row else None,
        "next_actions": state_payload.get("next_actions") or [],
        "blockers": blockers,
        "updated_at": state_payload.get("updated_at"),
        "urgency": urgency,
        "is_stale": jurisdiction_is_stale,
        "failed_count": failed_count,
        "blocked_count": blocked_count,
        "jurisdiction": {
            "exists": jurisdiction.get("exists"),
            "gate_ok": jurisdiction.get("gate_ok"),
            "profile_id": jurisdiction.get("profile_id"),
            "completeness_status": jurisdiction.get("completeness_status"),
            "is_stale": jurisdiction_is_stale,
        },
        "compliance": {
            "is_compliant": completion.get("is_compliant"),
            "completion_pct": completion.get("completion_pct"),
            "failed_count": failed_count,
            "blocked_count": blocked_count,
            "latest_inspection_passed": completion.get("latest_inspection_passed"),
            "open_failed_items": inspection.get("open_failed_items"),
        },
        "rehab": {
            "open": rehab.get("open"),
            "blocked": rehab.get("blocked"),
            "is_complete": constraints.get("rehab_complete"),
        },
        "counts": counts,
        "pane": pane_info,
    }


def _build_blockers(rows: list[dict[str, Any]], *, limit: int = 15) -> list[dict[str, Any]]:
    counter: dict[str, int] = defaultdict(int)
    examples: dict[str, dict[str, Any]] = {}

    for row in rows:
        for blocker in row.get("blockers") or []:
            key = str(blocker).strip()
            if not key:
                continue
            counter[key] += 1
            examples.setdefault(
                key,
                {
                    "blocker": key,
                    "count": 0,
                    "example_property_id": row.get("property_id"),
                    "example_address": row.get("address"),
                    "example_city": row.get("city"),
                    "urgency": row.get("urgency"),
                },
            )

    out = []
    for blocker, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        sample = dict(examples[blocker])
        sample["count"] = count
        out.append(sample)
    return out


def _build_recent_actions(rows: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        actions = row.get("next_actions") or []
        for action in actions[:2]:
            items.append(
                {
                    "property_id": row.get("property_id"),
                    "address": row.get("address"),
                    "city": row.get("city"),
                    "pane": row.get("current_pane"),
                    "stage": row.get("current_stage"),
                    "urgency": row.get("urgency"),
                    "action": action,
                    "updated_at": row.get("updated_at"),
                }
            )

    items.sort(
        key=lambda x: (
            str(x.get("updated_at") or ""),
            str(x.get("city") or ""),
            str(x.get("address") or ""),
        ),
        reverse=True,
    )
    return items[:limit]


def _build_next_actions(rows: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    candidates: list[dict[str, Any]] = []

    for row in rows:
        actions = row.get("next_actions") or []
        blockers = row.get("blockers") or []
        first_blocker = blockers[0] if blockers else None

        for idx, action in enumerate(actions[:2]):
            candidates.append(
                {
                    "property_id": row.get("property_id"),
                    "address": row.get("address"),
                    "city": row.get("city"),
                    "stage": row.get("current_stage"),
                    "pane": row.get("current_pane"),
                    "urgency": row.get("urgency"),
                    "blocker": first_blocker,
                    "action": action,
                    "priority": priority_order.get(_safe_str(row.get("urgency")).lower(), 99) + idx,
                }
            )

    candidates.sort(
        key=lambda x: (
            _safe_int(x.get("priority"), 99),
            str(x.get("city") or ""),
            str(x.get("address") or ""),
        )
    )
    return candidates[:limit]


def _build_stale_items(rows: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
    stale: list[dict[str, Any]] = []

    for row in rows:
        reasons: list[str] = []

        jurisdiction = row.get("jurisdiction") or {}
        compliance = row.get("compliance") or {}

        if jurisdiction.get("is_stale"):
            reasons.append("jurisdiction_stale")
        if _safe_int(compliance.get("failed_count"), 0) > 0:
            reasons.append("compliance_failed_items")
        if _safe_int(compliance.get("blocked_count"), 0) > 0:
            reasons.append("compliance_blocked_items")
        if "missing_cash_transactions" in (row.get("blockers") or []):
            reasons.append("missing_cash_transactions")

        if not reasons:
            continue

        stale.append(
            {
                "property_id": row.get("property_id"),
                "address": row.get("address"),
                "city": row.get("city"),
                "stage": row.get("current_stage"),
                "pane": row.get("current_pane"),
                "urgency": row.get("urgency"),
                "reasons": reasons,
            }
        )

    stale.sort(
        key=lambda x: (
            len(x.get("reasons") or []),
            str(x.get("city") or ""),
            str(x.get("address") or ""),
        ),
        reverse=True,
    )
    return stale[:limit]


def _build_queue_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_stage: dict[str, int] = defaultdict(int)
    by_status: dict[str, int] = defaultdict(int)
    by_urgency: dict[str, int] = defaultdict(int)

    for row in rows:
        by_stage[_safe_str(row.get("current_stage"), "unknown")] += 1
        by_status[_safe_str(row.get("gate_status"), "unknown")] += 1
        by_urgency[_safe_str(row.get("urgency"), "unknown")] += 1

    return {
        "total": len(rows),
        "by_stage": dict(sorted(by_stage.items(), key=lambda item: item[0])),
        "by_status": dict(sorted(by_status.items(), key=lambda item: item[0])),
        "by_urgency": dict(sorted(by_urgency.items(), key=lambda item: item[0])),
    }


def _build_kpis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    with_blockers = sum(1 for row in rows if row.get("blockers"))
    with_next_actions = sum(1 for row in rows if row.get("next_actions"))
    stale_count = sum(1 for row in rows if row.get("is_stale"))
    critical_count = sum(1 for row in rows if row.get("urgency") == "critical")
    high_count = sum(1 for row in rows if row.get("urgency") == "high")

    total_asking = 0.0
    total_cashflow = 0.0
    total_dscr = 0.0
    asking_count = 0
    cashflow_count = 0
    dscr_count = 0

    for row in rows:
        asking_price = row.get("asking_price")
        projected_cashflow = row.get("projected_monthly_cashflow")
        dscr_value = row.get("dscr")

        if asking_price is not None:
            total_asking += _safe_float(asking_price)
            asking_count += 1
        if projected_cashflow is not None:
            total_cashflow += _safe_float(projected_cashflow)
            cashflow_count += 1
        if dscr_value is not None:
            total_dscr += _safe_float(dscr_value)
            dscr_count += 1

    return {
        "total_properties": total,
        "with_blockers": with_blockers,
        "with_next_actions": with_next_actions,
        "stale_items": stale_count,
        "critical_items": critical_count,
        "high_priority_items": high_count,
        "avg_asking_price": round(total_asking / asking_count, 2) if asking_count else 0.0,
        "avg_projected_monthly_cashflow": round(total_cashflow / cashflow_count, 2) if cashflow_count else 0.0,
        "avg_dscr": round(total_dscr / dscr_count, 3) if dscr_count else 0.0,
    }


def _filter_rows_for_pane(
    rows: list[dict[str, Any]],
    *,
    pane: str,
    status: Optional[str],
    stage: Optional[str],
    urgency: Optional[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for row in rows:
        current_pane = clamp_pane(row.get("current_pane"))
        if not _property_matches_pane(pane, current_pane=current_pane):
            continue
        if not _property_matches_status(row, status=status):
            continue
        if not _property_matches_stage(row, stage=stage):
            continue
        if not _property_matches_urgency(row, urgency=urgency):
            continue

        out.append(row)

    return out


def _build_rows_for_scope(
    db: Session,
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user: Optional[int] = None,
    max_scan: int = 1000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scope_t0 = time.perf_counter()

    stmt = _base_property_stmt(
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user=assigned_user,
    )

    query_t0 = time.perf_counter()
    props = list(db.scalars(stmt.limit(max_scan)).all())
    query_ms = round((time.perf_counter() - query_t0) * 1000, 2)

    rows: list[dict[str, Any]] = []
    skipped_errors = 0

    build_t0 = time.perf_counter()
    for prop in props:
        try:
            state_payload = get_state_payload(
                db,
                org_id=org_id,
                property_id=int(prop.id),
                recompute=False,
            )
            rows.append(
                _build_row(
                    db,
                    org_id=org_id,
                    prop=prop,
                    state_payload=state_payload,
                )
            )
        except Exception:
            skipped_errors += 1
            log.exception(
                "pane_scope_row_build_failed",
                extra={"org_id": org_id, "property_id": int(getattr(prop, "id", 0) or 0)},
            )
            continue

    build_ms = round((time.perf_counter() - build_t0) * 1000, 2)
    total_ms = round((time.perf_counter() - scope_t0) * 1000, 2)

    meta = {
        "query_rows": len(props),
        "returned_rows": len(rows),
        "skipped_errors": skipped_errors,
        "query_ms": query_ms,
        "build_ms": build_ms,
        "total_ms": total_ms,
    }
    return rows, meta


def _build_contract_response(
    *,
    pane_key: str,
    principal: Any,
    org_id: int,
    raw_rows: list[dict[str, Any]],
    city: Optional[str],
    county: Optional[str],
    assigned_user: Optional[int],
    status: Optional[str],
    stage: Optional[str],
    urgency: Optional[str],
    limit: int,
) -> dict[str, Any]:
    allowed = allowed_panes_for_principal(principal)
    if pane_key not in allowed and pane_key != "admin":
        return {
            "ok": False,
            "pane": pane_key,
            "pane_label": pane_label(pane_key),
            "error": "pane_not_allowed",
            "allowed_panes": allowed,
        }

    pane_rows = _filter_rows_for_pane(
        raw_rows,
        pane=pane_key,
        status=status,
        stage=stage,
        urgency=urgency,
    )

    plan = _build_plan_context(principal)
    response: dict[str, Any] = {
        "ok": True,
        "pane": pane_key,
        "pane_label": pane_label(pane_key),
        "allowed_panes": allowed,
        "catalog": pane_catalog(),
        "filters": _standard_filters(
            org_id=org_id,
            city=city,
            county=county,
            assigned_user=assigned_user,
            status=status,
            stage=stage,
            urgency=urgency,
        ),
        "plan": plan,
        "kpis": _build_kpis(pane_rows),
        "blockers": _build_blockers(pane_rows),
        "recent_actions": _build_recent_actions(pane_rows),
        "next_actions": _build_next_actions(pane_rows),
        "stale_items": _build_stale_items(pane_rows),
        "queue_counts": _build_queue_counts(pane_rows),
        "rows": pane_rows[: int(limit)],
        "count": len(pane_rows),
    }

    if plan["advanced_fields_enabled"]:
        county_mix: dict[str, int] = defaultdict(int)
        stage_mix: dict[str, int] = defaultdict(int)

        for row in pane_rows:
            county_mix[_safe_str(row.get("county"), "unknown")] += 1
            stage_mix[_safe_str(row.get("current_stage"), "unknown")] += 1

        response["advanced"] = {
            "county_breakdown": [
                {"key": key, "value": value}
                for key, value in sorted(county_mix.items(), key=lambda item: (-item[1], item[0]))
            ],
            "stage_breakdown": [
                {"key": key, "value": value}
                for key, value in sorted(stage_mix.items(), key=lambda item: item[0])
            ],
            "leaderboards": {
                "cashflow": sorted(
                    pane_rows,
                    key=lambda row: _safe_float(row.get("projected_monthly_cashflow"), 0.0),
                    reverse=True,
                )[:10],
                "most_blocked": sorted(
                    pane_rows,
                    key=lambda row: len(row.get("blockers") or []),
                    reverse=True,
                )[:10],
            },
        }

    return response


def build_pane_dashboard(
    db: Session,
    *,
    org_id: int,
    pane: str,
    principal: Any = None,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    urgency: Optional[str] = None,
    assigned_user: Optional[int] = None,
    q: Optional[str] = None,
    limit: int = 200,
) -> dict[str, Any]:
    pane_key = clamp_pane(pane)
    t0 = time.perf_counter()

    raw_rows, scope_meta = _build_rows_for_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user=assigned_user,
        max_scan=max(int(limit) * 5, 500),
    )

    response = _build_contract_response(
        pane_key=pane_key,
        principal=principal,
        org_id=org_id,
        raw_rows=raw_rows,
        city=city,
        county=county,
        assigned_user=assigned_user,
        status=status,
        stage=stage,
        urgency=urgency,
        limit=limit,
    )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    log.info(
        "build_pane_dashboard_complete",
        extra={
            "org_id": org_id,
            "pane": pane_key,
            "count": response.get("count", 0),
            "limit": limit,
            "county": county,
            "city": city,
            "status": status,
            "stage": stage,
            "urgency": urgency,
            "assigned_user": assigned_user,
            "q": q,
            "scope_query_rows": scope_meta["query_rows"],
            "scope_returned_rows": scope_meta["returned_rows"],
            "scope_skipped_errors": scope_meta["skipped_errors"],
            "scope_query_ms": scope_meta["query_ms"],
            "scope_build_ms": scope_meta["build_ms"],
            "total_ms": total_ms,
        },
    )
    return response


def build_all_pane_summaries(
    db: Session,
    *,
    org_id: int,
    principal: Any = None,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    urgency: Optional[str] = None,
    assigned_user: Optional[int] = None,
    q: Optional[str] = None,
    limit: int = 100,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    allowed = allowed_panes_for_principal(principal)

    raw_rows, scope_meta = _build_rows_for_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user=assigned_user,
        max_scan=max(int(limit) * 8, 800),
    )

    panes: list[dict[str, Any]] = []
    for pane in PANES:
        if pane not in allowed and pane != "admin":
            continue

        summary = _build_contract_response(
            pane_key=pane,
            principal=principal,
            org_id=org_id,
            raw_rows=raw_rows,
            city=city,
            county=county,
            assigned_user=assigned_user,
            status=status,
            stage=stage,
            urgency=urgency,
            limit=limit,
        )

        panes.append(
            {
                "pane": pane,
                "pane_label": pane_label(pane),
                "count": summary.get("count", 0),
                "filters": summary.get("filters", {}),
                "kpis": summary.get("kpis", {}),
                "blockers": summary.get("blockers", [])[:3],
                "next_actions": summary.get("next_actions", [])[:3],
                "queue_counts": summary.get("queue_counts", {}),
            }
        )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    log.info(
        "build_all_pane_summaries_complete",
        extra={
            "org_id": org_id,
            "allowed_pane_count": len(allowed),
            "pane_count": len(panes),
            "limit": limit,
            "county": county,
            "city": city,
            "status": status,
            "stage": stage,
            "urgency": urgency,
            "assigned_user": assigned_user,
            "q": q,
            "scope_query_rows": scope_meta["query_rows"],
            "scope_returned_rows": scope_meta["returned_rows"],
            "scope_skipped_errors": scope_meta["skipped_errors"],
            "scope_query_ms": scope_meta["query_ms"],
            "scope_build_ms": scope_meta["build_ms"],
            "total_ms": total_ms,
        },
    )

    return {
        "ok": True,
        "allowed_panes": allowed,
        "catalog": pane_catalog(),
        "filters": _standard_filters(
            org_id=org_id,
            city=city,
            county=county,
            assigned_user=assigned_user,
            status=status,
            stage=stage,
            urgency=urgency,
        ),
        "panes": panes,
    }


def build_portfolio_rollup_with_panes(
    db: Session,
    *,
    org_id: int,
    principal: Any = None,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    urgency: Optional[str] = None,
    assigned_user: Optional[int] = None,
    limit: int = 500,
) -> dict[str, Any]:
    raw_rows, _ = _build_rows_for_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        assigned_user=assigned_user,
        max_scan=max(int(limit), 500),
    )

    filtered_rows = [
        row
        for row in raw_rows
        if _property_matches_status(row, status=status)
        and _property_matches_stage(row, stage=stage)
        and _property_matches_urgency(row, urgency=urgency)
    ]

    pane_counts: dict[str, int] = defaultdict(int)
    properties_with_next_actions = 0

    for row in filtered_rows:
        pane_key = clamp_pane(row.get("current_pane"))
        pane_counts[pane_key] += 1
        if row.get("next_actions"):
            properties_with_next_actions += 1

    return {
        "filters": _standard_filters(
            org_id=org_id,
            city=city,
            county=county,
            assigned_user=assigned_user,
            status=status,
            stage=stage,
            urgency=urgency,
        ),
        "kpis": _build_kpis(filtered_rows),
        "blockers": _build_blockers(filtered_rows),
        "recent_actions": _build_recent_actions(filtered_rows),
        "next_actions": _build_next_actions(filtered_rows),
        "stale_items": _build_stale_items(filtered_rows),
        "queue_counts": _build_queue_counts(filtered_rows),
        "pane_counts": dict(sorted(pane_counts.items(), key=lambda item: item[0])),
        "properties_with_next_actions": properties_with_next_actions,
        "allowed_panes": allowed_panes_for_principal(principal),
        "rows": filtered_rows[: int(limit)],
    }