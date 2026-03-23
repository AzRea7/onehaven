from __future__ import annotations

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
from ..services.dashboard_rollups import compute_rollups
from ..services.property_state_machine import get_state_payload


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


def _base_property_stmt(
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
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

    if assigned_user_id is not None:
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
                clauses.append(getattr(Property, col_name) == assigned_user_id)
        if clauses:
            stmt = stmt.where(or_(*clauses))

    return stmt.order_by(desc(Property.id))


def _property_matches_status(
    state_payload: dict[str, Any],
    *,
    pane: str,
    status: Optional[str],
) -> bool:
    if not status:
        return True

    raw = _normalize_status(status)
    stage = _safe_str(state_payload.get("current_stage")).lower()
    gate_status = _safe_str(state_payload.get("gate_status")).lower()
    decision = _safe_str(state_payload.get("normalized_decision")).lower()

    if raw in {stage, gate_status, decision}:
        return True

    pane_status_map = {
        "investor": {
            "saved": {"discovered"},
            "shortlisted": {"shortlisted"},
            "underwritten": {"underwritten"},
            "review": {"underwritten"},
            "good": {"underwritten", "offer"},
            "rejected": set(),
        },
        "acquisition": {
            "offer": {"offer"},
            "acquired": {"acquired"},
            "blocked": set(),
        },
        "compliance": {
            "rehab": {"rehab"},
            "readying": {"compliance_readying"},
            "inspection": {"inspection_pending"},
            "blocked": set(),
        },
        "tenants": {
            "marketing": {"tenant_marketing"},
            "screening": {"tenant_screening"},
            "leased": {"leased"},
        },
        "management": {
            "occupied": {"occupied"},
            "turnover": {"turnover"},
            "maintenance": {"maintenance"},
        },
        "admin": {
            "all": set(),
        },
    }

    mapped = pane_status_map.get(pane, {})
    if raw in mapped and stage in mapped[raw]:
        return True

    if pane == "investor" and raw == "rejected" and decision == "reject":
        return True
    if raw == "blocked" and gate_status == "blocked":
        return True

    return False


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
        "blockers": (state_payload.get("outstanding_tasks") or {}).get("blockers") or [],
        "updated_at": state_payload.get("updated_at"),
        "jurisdiction": {
            "exists": jurisdiction.get("exists"),
            "gate_ok": jurisdiction.get("gate_ok"),
            "profile_id": jurisdiction.get("profile_id"),
            "completeness_status": jurisdiction.get("completeness_status"),
            "is_stale": jurisdiction.get("is_stale"),
        },
        "compliance": {
            "is_compliant": completion.get("is_compliant"),
            "completion_pct": completion.get("completion_pct"),
            "failed_count": completion.get("failed_count"),
            "blocked_count": completion.get("blocked_count"),
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


def _build_blocker_summary(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
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
                },
            )

    out = []
    for blocker, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        sample = dict(examples[blocker])
        sample["count"] = count
        out.append(sample)
    return out


def _build_recent_actions(rows: list[dict[str, Any]], *, limit: int = 15) -> list[dict[str, Any]]:
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


def _build_recommended_next_actions(rows: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    priority_order = {
        "missing_underwriting": 0,
        "not_acquired": 1,
        "rehab_blocked": 2,
        "inspection_open_failures": 3,
        "compliance_failed_items": 4,
        "missing_tenant_pipeline": 5,
        "tenant_pipeline_in_progress": 6,
        "missing_cash_transactions": 7,
        "turnover_active": 8,
    }

    candidates: list[dict[str, Any]] = []
    for row in rows:
        actions = row.get("next_actions") or []
        blockers = row.get("blockers") or []
        first_blocker = blockers[0] if blockers else ""
        for idx, action in enumerate(actions[:2]):
            candidates.append(
                {
                    "property_id": row.get("property_id"),
                    "address": row.get("address"),
                    "city": row.get("city"),
                    "stage": row.get("current_stage"),
                    "pane": row.get("current_pane"),
                    "blocker": first_blocker,
                    "action": action,
                    "priority": priority_order.get(str(first_blocker), 50) + idx,
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


def _build_stale_items(rows: list[dict[str, Any]], *, limit: int = 15) -> list[dict[str, Any]]:
    stale: list[dict[str, Any]] = []

    for row in rows:
        jurisdiction = row.get("jurisdiction") or {}
        compliance = row.get("compliance") or {}
        blockers = row.get("blockers") or []

        reasons: list[str] = []
        if jurisdiction.get("is_stale"):
            reasons.append("jurisdiction_stale")
        if jurisdiction.get("completeness_status") not in {None, "complete"}:
            reasons.append("jurisdiction_incomplete")
        if _safe_int(compliance.get("failed_count"), 0) > 0:
            reasons.append("compliance_failed_items")
        if _safe_int(compliance.get("blocked_count"), 0) > 0:
            reasons.append("compliance_blocked_items")
        if "missing_cash_transactions" in blockers:
            reasons.append("missing_cash_transactions")
        if "tenant_pipeline_in_progress" in blockers:
            reasons.append("tenant_pipeline_in_progress")

        if not reasons:
            continue

        stale.append(
            {
                "property_id": row.get("property_id"),
                "address": row.get("address"),
                "city": row.get("city"),
                "stage": row.get("current_stage"),
                "pane": row.get("current_pane"),
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


def _build_kpis_for_pane(pane: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    if total == 0:
        base = {
            "total_properties": 0,
            "with_blockers": 0,
            "with_next_actions": 0,
            "stale_count": 0,
        }
        if pane == "investor":
            base.update(
                {
                    "good_candidates": 0,
                    "review_candidates": 0,
                    "avg_cashflow_estimate": 0.0,
                }
            )
        elif pane == "acquisition":
            base.update(
                {
                    "offer_stage_count": 0,
                    "acquired_stage_count": 0,
                    "blocked_acquisitions": 0,
                }
            )
        elif pane == "compliance":
            base.update(
                {
                    "inspection_pending_count": 0,
                    "failed_items_total": 0,
                    "jurisdiction_stale_count": 0,
                }
            )
        elif pane == "tenants":
            base.update(
                {
                    "marketing_count": 0,
                    "screening_count": 0,
                    "leased_count": 0,
                }
            )
        elif pane == "management":
            base.update(
                {
                    "occupied_count": 0,
                    "turnover_count": 0,
                    "maintenance_count": 0,
                }
            )
        elif pane == "admin":
            base.update(
                {
                    "org_property_count": 0,
                    "pane_count": len(PANES),
                    "blocked_items_total": 0,
                }
            )
        return base

    with_blockers = sum(1 for row in rows if row.get("blockers"))
    with_next_actions = sum(1 for row in rows if row.get("next_actions"))
    stale_count = len(_build_stale_items(rows, limit=100000))

    kpis: dict[str, Any] = {
        "total_properties": total,
        "with_blockers": with_blockers,
        "with_next_actions": with_next_actions,
        "stale_count": stale_count,
    }

    if pane == "investor":
        good_candidates = sum(1 for row in rows if str(row.get("normalized_decision") or "").upper() == "GOOD")
        review_candidates = sum(1 for row in rows if str(row.get("normalized_decision") or "").upper() == "REVIEW")
        cash_values = [row.get("projected_monthly_cashflow") for row in rows if row.get("projected_monthly_cashflow") is not None]
        kpis.update(
            {
                "good_candidates": good_candidates,
                "review_candidates": review_candidates,
                "avg_cashflow_estimate": round(sum(_safe_float(x) for x in cash_values) / len(cash_values), 2) if cash_values else 0.0,
            }
        )
    elif pane == "acquisition":
        kpis.update(
            {
                "offer_stage_count": sum(1 for row in rows if row.get("current_stage") == "offer"),
                "acquired_stage_count": sum(1 for row in rows if row.get("current_stage") == "acquired"),
                "blocked_acquisitions": sum(1 for row in rows if str(row.get("gate_status") or "").upper() == "BLOCKED"),
            }
        )
    elif pane == "compliance":
        kpis.update(
            {
                "inspection_pending_count": sum(1 for row in rows if row.get("current_stage") == "inspection_pending"),
                "failed_items_total": sum(_safe_int((row.get("compliance") or {}).get("failed_count"), 0) for row in rows),
                "jurisdiction_stale_count": sum(1 for row in rows if (row.get("jurisdiction") or {}).get("is_stale") is True),
            }
        )
    elif pane == "tenants":
        kpis.update(
            {
                "marketing_count": sum(1 for row in rows if row.get("current_stage") == "tenant_marketing"),
                "screening_count": sum(1 for row in rows if row.get("current_stage") == "tenant_screening"),
                "leased_count": sum(1 for row in rows if row.get("current_stage") == "leased"),
            }
        )
    elif pane == "management":
        kpis.update(
            {
                "occupied_count": sum(1 for row in rows if row.get("current_stage") == "occupied"),
                "turnover_count": sum(1 for row in rows if row.get("current_stage") == "turnover"),
                "maintenance_count": sum(1 for row in rows if row.get("current_stage") == "maintenance"),
            }
        )
    elif pane == "admin":
        blocked_items_total = sum(len(row.get("blockers") or []) for row in rows)
        kpis.update(
            {
                "org_property_count": total,
                "pane_count": len(PANES),
                "blocked_items_total": blocked_items_total,
            }
        )

    return kpis


def _filter_rows_for_pane(
    rows: list[dict[str, Any]],
    *,
    pane: str,
    status: Optional[str],
    jurisdiction: Optional[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    wanted_jurisdiction = _normalize_status(jurisdiction)

    for row in rows:
        current_pane = clamp_pane(row.get("current_pane"))
        if not _property_matches_pane(pane, current_pane=current_pane):
            continue

        if not _property_matches_status(
            {
                "current_stage": row.get("current_stage"),
                "gate_status": row.get("gate_status"),
                "normalized_decision": row.get("normalized_decision"),
            },
            pane=pane,
            status=status,
        ):
            continue

        if wanted_jurisdiction:
            jurisdiction_payload = row.get("jurisdiction") or {}
            profile_id = jurisdiction_payload.get("profile_id")
            completeness_status = _normalize_status(jurisdiction_payload.get("completeness_status"))
            if wanted_jurisdiction == "missing" and profile_id:
                continue
            if wanted_jurisdiction == "stale" and not jurisdiction_payload.get("is_stale"):
                continue
            if wanted_jurisdiction == "incomplete" and completeness_status == "complete":
                continue
            if wanted_jurisdiction not in {"missing", "stale", "incomplete"}:
                if completeness_status != wanted_jurisdiction:
                    continue

        out.append(row)

    return out


def build_pane_dashboard(
    db: Session,
    *,
    org_id: int,
    pane: str,
    principal: Any = None,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    jurisdiction: Optional[str] = None,
    status: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
    q: Optional[str] = None,
    limit: int = 200,
) -> dict[str, Any]:
    pane_key = clamp_pane(pane)
    allowed = allowed_panes_for_principal(principal)
    if pane_key not in allowed and pane_key != "admin":
        return {
            "ok": False,
            "pane": pane_key,
            "pane_label": pane_label(pane_key),
            "error": "pane_not_allowed",
            "allowed_panes": allowed,
        }

    stmt = _base_property_stmt(
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user_id=assigned_user_id,
    )

    props = list(db.scalars(stmt.limit(max(int(limit), 500))).all())

    raw_rows: list[dict[str, Any]] = []
    for prop in props:
        try:
            state_payload = get_state_payload(db, org_id=org_id, property_id=int(prop.id), recompute=True)
            raw_rows.append(
                _build_row(
                    db,
                    org_id=org_id,
                    prop=prop,
                    state_payload=state_payload,
                )
            )
        except Exception:
            continue

    pane_rows = _filter_rows_for_pane(
        raw_rows,
        pane=pane_key,
        status=status,
        jurisdiction=jurisdiction,
    )

    plan = _build_plan_context(principal)
    stale_items = _build_stale_items(pane_rows)
    blockers = _build_blocker_summary(pane_rows)
    recent_actions = _build_recent_actions(pane_rows)
    recommended_next_actions = _build_recommended_next_actions(pane_rows)
    kpis = _build_kpis_for_pane(pane_key, pane_rows)

    response: dict[str, Any] = {
        "ok": True,
        "pane": pane_key,
        "pane_label": pane_label(pane_key),
        "allowed_panes": allowed,
        "catalog": pane_catalog(),
        "filters": {
            "state": state,
            "county": county,
            "city": city,
            "jurisdiction": jurisdiction,
            "status": status,
            "assigned_user_id": assigned_user_id,
            "q": q,
            "limit": limit,
        },
        "plan": plan,
        "kpis": kpis,
        "blockers": blockers,
        "recent_actions": recent_actions,
        "recommended_next_actions": recommended_next_actions,
        "stale_items": stale_items,
        "rows": pane_rows[: int(limit)],
        "count": len(pane_rows),
    }

    if plan["advanced_fields_enabled"]:
        stage_mix: dict[str, int] = defaultdict(int)
        county_mix: dict[str, int] = defaultdict(int)
        for row in pane_rows:
            stage_mix[_safe_str(row.get("current_stage"), "unknown")] += 1
            county_mix[_safe_str(row.get("county"), "unknown")] += 1

        response["advanced"] = {
            "stage_mix": [
                {"key": key, "value": value}
                for key, value in sorted(stage_mix.items(), key=lambda item: item[0])
            ],
            "county_breakdown": [
                {"key": key, "value": value}
                for key, value in sorted(county_mix.items(), key=lambda item: (-item[1], item[0]))
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


def build_all_pane_summaries(
    db: Session,
    *,
    org_id: int,
    principal: Any = None,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    jurisdiction: Optional[str] = None,
    status: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
    q: Optional[str] = None,
    limit: int = 100,
) -> dict[str, Any]:
    allowed = allowed_panes_for_principal(principal)

    panes: list[dict[str, Any]] = []
    for pane in PANES:
        if pane not in allowed and pane != "admin":
            continue
        summary = build_pane_dashboard(
            db,
            org_id=org_id,
            pane=pane,
            principal=principal,
            state=state,
            county=county,
            city=city,
            jurisdiction=jurisdiction,
            status=status,
            assigned_user_id=assigned_user_id,
            q=q,
            limit=limit,
        )
        panes.append(
            {
                "pane": pane,
                "pane_label": pane_label(pane),
                "count": summary.get("count", 0),
                "kpis": summary.get("kpis", {}),
                "top_blockers": summary.get("blockers", [])[:3],
                "top_actions": summary.get("recommended_next_actions", [])[:3],
            }
        )

    return {
        "ok": True,
        "allowed_panes": allowed,
        "catalog": pane_catalog(),
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
    decision: Optional[str] = None,
    stage: Optional[str] = None,
    limit: int = 500,
) -> dict[str, Any]:
    data = compute_rollups(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        decision=decision,
        stage=stage,
        limit=limit,
    )

    pane_counts: dict[str, int] = defaultdict(int)
    properties_with_next_actions = 0

    for row in data.get("rows", []):
        pane_key = clamp_pane(row.get("current_pane"))
        pane_counts[pane_key] += 1
        if row.get("next_actions"):
            properties_with_next_actions += 1

    return {
        "properties": data.get("summary", {}).get("property_count", 0),
        "stage_counts": data.get("buckets", {}).get("stages", {}),
        "decision_counts": data.get("buckets", {}).get("decisions", {}),
        "pane_counts": dict(sorted(pane_counts.items(), key=lambda item: item[0])),
        "properties_with_next_actions": properties_with_next_actions,
        "averages": {
            "asking_price": data.get("summary", {}).get("avg_asking_price", 0.0),
            "projected_monthly_cashflow": data.get("summary", {}).get("avg_projected_monthly_cashflow", 0.0),
            "dscr": data.get("summary", {}).get("avg_dscr", 0.0),
        },
        "allowed_panes": allowed_panes_for_principal(principal),
    }
