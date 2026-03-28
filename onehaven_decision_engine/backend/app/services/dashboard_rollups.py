from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Optional

from .property_inventory_snapshot_service import build_inventory_snapshots_for_scope
from .runtime_metrics import METRICS

log = logging.getLogger("onehaven.dashboard_rollups")


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


def _normalize_text(v: Any) -> str:
    return str(v or "").strip()


def _normalize_lower(v: Any) -> Optional[str]:
    raw = _normalize_text(v).lower()
    return raw or None


def _normalize_decision(v: Any) -> str:
    raw = _normalize_text(v).upper()
    if raw in {"GOOD", "PASS", "BUY", "APPROVE", "APPROVED", "GOOD_DEAL"}:
        return "GOOD"
    if raw in {"REJECT", "FAIL", "FAILED", "NO_GO"}:
        return "REJECT"
    return "REVIEW"


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
        "city": _normalize_text(city) or None,
        "county": _normalize_text(county) or None,
        "assigned_user": assigned_user,
        "status": _normalize_lower(status),
        "stage": _normalize_lower(stage),
        "urgency": _normalize_lower(urgency),
    }


def _derive_urgency(row: dict[str, Any]) -> str:
    blockers = row.get("blockers") or []
    gate_status = _normalize_lower(row.get("gate_status"))
    failed_count = _safe_int(row.get("failed_count"), 0)
    blocked_count = _safe_int(row.get("blocked_count"), 0)
    is_stale = bool(row.get("is_stale"))

    if gate_status == "blocked" or blocked_count > 0:
        return "critical"
    if failed_count > 0 or len(blockers) >= 3:
        return "high"
    if is_stale or len(blockers) > 0:
        return "medium"
    return "low"


def _matches_status(row: dict[str, Any], wanted_status: Optional[str]) -> bool:
    if not wanted_status:
        return True

    raw = wanted_status.strip().lower()
    stage = _normalize_text(row.get("current_stage")).lower()
    decision = _normalize_decision(row.get("normalized_decision")).lower()
    gate_status = _normalize_text(row.get("gate_status")).lower()
    completeness = _normalize_text(row.get("completeness")).lower()
    pane = _normalize_text(row.get("current_pane")).lower()

    return raw in {stage, decision, gate_status, completeness, pane}


def _apply_row_filters(
    rows: list[dict[str, Any]],
    *,
    stage: Optional[str] = None,
    decision: Optional[str] = None,
    pane: Optional[str] = None,
    status: Optional[str] = None,
    urgency: Optional[str] = None,
) -> list[dict[str, Any]]:
    wanted_stage = _normalize_text(stage).lower() or None
    wanted_decision = _normalize_decision(decision) if decision else None
    wanted_pane = _normalize_text(pane).lower() or None
    wanted_status = _normalize_text(status).lower() or None
    wanted_urgency = _normalize_text(urgency).lower() or None

    out: list[dict[str, Any]] = []

    for row in rows:
        row_stage = _normalize_text(row.get("current_stage")).lower()
        row_decision = _normalize_decision(row.get("normalized_decision"))
        row_pane = _normalize_text(row.get("current_pane")).lower()
        row_urgency = _normalize_text(row.get("urgency")).lower()

        if wanted_stage and row_stage != wanted_stage:
            continue
        if wanted_decision and row_decision != wanted_decision:
            continue
        if wanted_pane and row_pane != wanted_pane:
            continue
        if wanted_urgency and row_urgency != wanted_urgency:
            continue
        if not _matches_status(row, wanted_status):
            continue

        out.append(row)

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
    out: list[dict[str, Any]] = []

    for row in rows:
        actions = row.get("next_actions") or []
        blockers = row.get("blockers") or []
        first_blocker = blockers[0] if blockers else None

        for idx, action in enumerate(actions[:2]):
            out.append(
                {
                    "property_id": row.get("property_id"),
                    "address": row.get("address"),
                    "city": row.get("city"),
                    "pane": row.get("current_pane"),
                    "stage": row.get("current_stage"),
                    "urgency": row.get("urgency"),
                    "blocker": first_blocker,
                    "action": action,
                    "priority": priority_order.get(_normalize_lower(row.get("urgency")) or "low", 99) + idx,
                }
            )

    out.sort(
        key=lambda x: (
            _safe_int(x.get("priority"), 99),
            str(x.get("city") or ""),
            str(x.get("address") or ""),
        )
    )
    return out[:limit]


def _build_stale_items(rows: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
    stale: list[dict[str, Any]] = []

    for row in rows:
        reasons: list[str] = []

        if bool(row.get("is_stale")):
            reasons.append("stale_record")
        if _safe_int(row.get("failed_count"), 0) > 0:
            reasons.append("failed_items")
        if _safe_int(row.get("blocked_count"), 0) > 0:
            reasons.append("blocked_items")
        if "missing_cash_transactions" in (row.get("blockers") or []):
            reasons.append("missing_cash_transactions")

        if not reasons:
            continue

        stale.append(
            {
                "property_id": row.get("property_id"),
                "address": row.get("address"),
                "city": row.get("city"),
                "pane": row.get("current_pane"),
                "stage": row.get("current_stage"),
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

    out: list[dict[str, Any]] = []
    for blocker, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        sample = dict(examples[blocker])
        sample["count"] = count
        out.append(sample)
    return out


def _build_queue_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_stage: dict[str, int] = defaultdict(int)
    by_status: dict[str, int] = defaultdict(int)
    by_urgency: dict[str, int] = defaultdict(int)
    by_pane: dict[str, int] = defaultdict(int)

    for row in rows:
        by_stage[_normalize_text(row.get("current_stage")) or "unknown"] += 1
        by_status[_normalize_text(row.get("gate_status")) or "unknown"] += 1
        by_urgency[_normalize_text(row.get("urgency")) or "unknown"] += 1
        by_pane[_normalize_text(row.get("current_pane")) or "unknown"] += 1

    return {
        "total": len(rows),
        "by_stage": dict(sorted(by_stage.items(), key=lambda item: item[0])),
        "by_status": dict(sorted(by_status.items(), key=lambda item: item[0])),
        "by_urgency": dict(sorted(by_urgency.items(), key=lambda item: item[0])),
        "by_pane": dict(sorted(by_pane.items(), key=lambda item: item[0])),
    }


def _build_kpis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_properties = len(rows)
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
    total_rank = 0.0
    rank_count = 0
    total_rent_gap = 0.0
    rent_gap_count = 0

    for row in rows:
        rank_value = row.get("rank_score")
        rent_gap_value = row.get("rent_gap")

        if rank_value is not None:
            total_rank += _safe_float(rank_value)
            rank_count += 1
        if rent_gap_value is not None:
            total_rent_gap += _safe_float(rent_gap_value)
            rent_gap_count += 1
        if row.get("asking_price") is not None:
            total_asking += _safe_float(row.get("asking_price"))
            asking_count += 1
        if row.get("projected_monthly_cashflow") is not None:
            total_cashflow += _safe_float(row.get("projected_monthly_cashflow"))
            cashflow_count += 1
        if row.get("dscr") is not None:
            total_dscr += _safe_float(row.get("dscr"))
            dscr_count += 1

    return {
        "total_properties": total_properties,
        "with_blockers": with_blockers,
        "with_next_actions": with_next_actions,
        "stale_items": stale_count,
        "critical_items": critical_count,
        "high_priority_items": high_count,
        "avg_asking_price": round(total_asking / asking_count, 2) if asking_count else 0.0,
        "avg_projected_monthly_cashflow": round(total_cashflow / cashflow_count, 2) if cashflow_count else 0.0,
        "avg_rank_score": round(total_rank / rank_count, 2) if rank_count else 0.0,
        "avg_rent_gap": round(total_rent_gap / rent_gap_count, 2) if rent_gap_count else 0.0,
        "avg_dscr": round(total_dscr / dscr_count, 3) if dscr_count else 0.0,
    }


def _leaderboards(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    candidate_rows = [r for r in rows if bool(r.get("is_deal_candidate"))]
    source_rows = candidate_rows or rows

    top_ranked = sorted(
        source_rows,
        key=lambda r: _safe_float(r.get("rank_score"), -10**12),
        reverse=True,
    )[:10]

    cashflow = sorted(
        source_rows,
        key=lambda r: _safe_float(r.get("projected_monthly_cashflow"), -10**12),
        reverse=True,
    )[:10]

    best_dscr = sorted(
        [r for r in source_rows if r.get("dscr") is not None],
        key=lambda r: _safe_float(r.get("dscr"), -10**12),
        reverse=True,
    )[:10]

    best_rent_gap = sorted(
        [r for r in source_rows if r.get("rent_gap") is not None],
        key=lambda r: _safe_float(r.get("rent_gap"), -10**12),
        reverse=True,
    )[:10]

    lowest_risk = sorted(
        [r for r in source_rows if r.get("risk_score") is not None],
        key=lambda r: _safe_float(r.get("risk_score"), 10**12),
    )[:10]

    return {
        "top_ranked": top_ranked,
        "cashflow": cashflow,
        "best_dscr": best_dscr,
        "best_rent_gap": best_rent_gap,
        "lowest_risk": lowest_risk,
    }


def compute_rollups(
    db,
    *,
    org_id: int,
    days: int = 90,
    limit: int = 500,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    stage: Optional[str] = None,
    decision: Optional[str] = None,
    pane: Optional[str] = None,
    status: Optional[str] = None,
    assigned_user: Optional[int] = None,
    urgency: Optional[str] = None,
    only_red_zone: bool = False,
    exclude_red_zone: bool = False,
    min_crime_score: Optional[float] = None,
    max_crime_score: Optional[float] = None,
    min_offender_count: Optional[int] = None,
    max_offender_count: Optional[int] = None,
    include_hidden: bool = False,
    deals_only: bool = False,
    include_suppressed: bool = False,
) -> dict[str, Any]:
    t0 = time.perf_counter()

    inventory = build_inventory_snapshots_for_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user_id=assigned_user,
        include_hidden=include_hidden,
        limit=max(int(limit), 500),
    )
    rows = list(inventory.get("rows") or [])
    prefilter_count = len(rows)

    normalized_rows: list[dict[str, Any]] = []

    for row in rows:
        normalized = dict(row)
        normalized["urgency"] = _derive_urgency(normalized)
        normalized_rows.append(normalized)
    rows = normalized_rows

    if not include_hidden:
        rows = [row for row in rows if not bool(row.get("listing_hidden"))]    
    if deals_only:
        if include_suppressed:
            rows = [row for row in rows if row.get("deal_filter_status") != "hidden"]
        else:
            rows = [row for row in rows if bool(row.get("is_deal_candidate"))]
    
    rows.sort(
        key=lambda row: (
            _safe_float(row.get("rank_score"), -10**12),
            _safe_float(row.get("projected_monthly_cashflow"), -10**12),
            _safe_float(row.get("dscr"), -10**12),
        ),
        reverse=True,
    )
    
    if only_red_zone:
        rows = [r for r in rows if bool(r.get("is_red_zone"))]
    elif exclude_red_zone:
        rows = [r for r in rows if not bool(r.get("is_red_zone"))]

    if min_crime_score is not None:
        rows = [
            r for r in rows
            if r.get("crime_score") is not None and _safe_float(r.get("crime_score")) >= float(min_crime_score)
        ]
    if max_crime_score is not None:
        rows = [
            r for r in rows
            if r.get("crime_score") is not None and _safe_float(r.get("crime_score")) <= float(max_crime_score)
        ]
    if min_offender_count is not None:
        rows = [
            r for r in rows
            if r.get("offender_count") is not None and _safe_int(r.get("offender_count")) >= int(min_offender_count)
        ]
    if max_offender_count is not None:
        rows = [
            r for r in rows
            if r.get("offender_count") is not None and _safe_int(r.get("offender_count")) <= int(max_offender_count)
        ]

    rows = _apply_row_filters(
        rows,
        stage=stage,
        decision=decision,
        pane=pane,
        status=status,
        urgency=urgency,
    )

    decision_counts: dict[str, int] = defaultdict(int)
    stage_counts: dict[str, int] = defaultdict(int)
    county_counts: dict[str, int] = defaultdict(int)
    pane_counts: dict[str, int] = defaultdict(int)

    for row in rows:
        decision_counts[_normalize_decision(row.get("normalized_decision"))] += 1
        stage_counts[_normalize_text(row.get("current_stage")) or "unknown"] += 1
        county_counts[_normalize_text(row.get("county")) or "unknown"] += 1
        pane_counts[_normalize_text(row.get("current_pane")) or "unknown"] += 1

    contract_filters = _standard_filters(
        org_id=org_id,
        city=city,
        county=county,
        assigned_user=assigned_user,
        status=status,
        stage=stage,
        urgency=urgency,
    )

    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    METRICS.observe_ms("dashboard_rollups_total_ms", duration_ms, labels={"org_id": org_id})
    METRICS.inc("dashboard_rollups_count", labels={"org_id": org_id})

    log.info(
        "dashboard_rollups_compute_complete",
        extra={
            "event": "dashboard_rollups_compute_complete",
            "org_id": org_id,
            "days": days,
            "limit": limit,
            "prefilter_count": prefilter_count,
            "returned_rows": len(rows),
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "stage": stage,
            "decision": decision,
            "pane": pane,
            "status": status,
            "assigned_user": assigned_user,
            "urgency": urgency,
            "only_red_zone": only_red_zone,
            "exclude_red_zone": exclude_red_zone,
            "min_crime_score": min_crime_score,
            "max_crime_score": max_crime_score,
            "min_offender_count": min_offender_count,
            "max_offender_count": max_offender_count,
            "total_ms": duration_ms,
        },
    )

    return {
        "ok": True,
        "filters": contract_filters,
        "kpis": _build_kpis(rows),
        "blockers": _build_blockers(rows),
        "recent_actions": _build_recent_actions(rows),
        "next_actions": _build_next_actions(rows),
        "stale_items": _build_stale_items(rows),
        "queue_counts": _build_queue_counts(rows),
        "summary": {
            "property_count": len(rows),
            "good_count": _safe_int(decision_counts.get("GOOD")),
            "review_count": _safe_int(decision_counts.get("REVIEW")),
            "reject_count": _safe_int(decision_counts.get("REJECT")),
        },
        "buckets": {
            "decisions": {k: int(v) for k, v in decision_counts.items()},
            "stages": {k: int(v) for k, v in stage_counts.items()},
            "panes": {k: int(v) for k, v in pane_counts.items()},
            "counties": {k: int(v) for k, v in county_counts.items()},
        },
        "leaderboards": _leaderboards(rows),
        "rows": rows[: int(limit)],
        "meta": {
            "inventory_meta": inventory.get("meta") or {},
            "window_days": int(days),
            "total_ms": duration_ms,
        },
    }