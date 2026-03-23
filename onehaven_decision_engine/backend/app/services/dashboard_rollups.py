from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Optional

from .property_inventory_snapshot_service import build_inventory_snapshots_for_scope
from .runtime_metrics import METRICS

log = logging.getLogger("onehaven.dashboard_rollups")


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


def _normalize_decision(v: Any) -> str:
    raw = _normalize_text(v).upper()
    if raw in {"GOOD", "PASS", "BUY", "APPROVE", "APPROVED", "GOOD_DEAL"}:
        return "GOOD"
    if raw in {"REJECT", "FAIL", "FAILED", "NO_GO"}:
        return "REJECT"
    return "REVIEW"


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
) -> list[dict[str, Any]]:
    wanted_stage = _normalize_text(stage).lower() or None
    wanted_decision = _normalize_decision(decision) if decision else None
    wanted_pane = _normalize_text(pane).lower() or None
    wanted_status = _normalize_text(status).lower() or None

    out: list[dict[str, Any]] = []

    for row in rows:
        row_stage = _normalize_text(row.get("current_stage")).lower()
        row_decision = _normalize_decision(row.get("normalized_decision"))
        row_pane = _normalize_text(row.get("current_pane")).lower()

        if wanted_stage and row_stage != wanted_stage:
            continue
        if wanted_decision and row_decision != wanted_decision:
            continue
        if wanted_pane and row_pane != wanted_pane:
            continue
        if not _matches_status(row, wanted_status):
            continue

        out.append(row)

    return out


def _leaderboards(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    cashflow = sorted(
        rows,
        key=lambda r: _safe_float(r.get("projected_monthly_cashflow"), -10**12),
        reverse=True,
    )[:10]

    lowest_crime = sorted(
        [r for r in rows if r.get("crime_score") is not None],
        key=lambda r: _safe_float(r.get("crime_score"), 10**12),
    )[:10]

    most_blocked = sorted(
        rows,
        key=lambda r: len(r.get("blockers") or []),
        reverse=True,
    )[:10]

    best_dscr = sorted(
        [r for r in rows if r.get("dscr") is not None],
        key=lambda r: _safe_float(r.get("dscr"), -10**12),
        reverse=True,
    )[:10]

    return {
        "cashflow": cashflow,
        "lowest_crime": lowest_crime,
        "most_blocked": most_blocked,
        "best_dscr": best_dscr,
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
    assigned_user_id: Optional[int] = None,
    only_red_zone: bool = False,
    exclude_red_zone: bool = False,
    min_crime_score: Optional[float] = None,
    max_crime_score: Optional[float] = None,
    min_offender_count: Optional[int] = None,
    max_offender_count: Optional[int] = None,
) -> dict[str, Any]:
    t0 = time.perf_counter()

    inventory = build_inventory_snapshots_for_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user_id=assigned_user_id,
        limit=max(int(limit), 500),
    )
    rows = list(inventory.get("rows") or [])

    prefilter_count = len(rows)

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
    )

    decision_counts: dict[str, int] = defaultdict(int)
    stage_counts: dict[str, int] = defaultdict(int)
    county_counts: dict[str, int] = defaultdict(int)
    pane_counts: dict[str, int] = defaultdict(int)
    completeness_counts: dict[str, int] = defaultdict(int)

    total_asking = 0.0
    total_cashflow = 0.0
    total_dscr = 0.0
    total_crime = 0.0

    asking_count = 0
    cashflow_count = 0
    dscr_count = 0
    crime_count = 0

    for row in rows:
        normalized_decision = _normalize_decision(row.get("normalized_decision"))
        current_stage = _normalize_text(row.get("current_stage")) or "unknown"
        current_pane = _normalize_text(row.get("current_pane")) or "unknown"
        county_key = _normalize_text(row.get("county")) or "unknown"
        completeness_key = _normalize_text(row.get("completeness")) or "UNKNOWN"

        decision_counts[normalized_decision] += 1
        stage_counts[current_stage] += 1
        pane_counts[current_pane] += 1
        county_counts[county_key] += 1
        completeness_counts[completeness_key] += 1

        asking_price = row.get("asking_price")
        projected_cashflow = row.get("projected_monthly_cashflow")
        dscr_value = row.get("dscr")
        crime_score = row.get("crime_score")

        if asking_price is not None:
            total_asking += _safe_float(asking_price)
            asking_count += 1
        if projected_cashflow is not None:
            total_cashflow += _safe_float(projected_cashflow)
            cashflow_count += 1
        if dscr_value is not None:
            total_dscr += _safe_float(dscr_value)
            dscr_count += 1
        if crime_score is not None:
            total_crime += _safe_float(crime_score)
            crime_count += 1

    total_properties = len(rows)

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
            "returned_rows": total_properties,
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "stage": stage,
            "decision": decision,
            "pane": pane,
            "status": status,
            "assigned_user_id": assigned_user_id,
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
        "as_of": None,
        "window_days": int(days),
        "filters": {
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "stage": _normalize_text(stage).lower() or None,
            "decision": _normalize_decision(decision) if decision else None,
            "pane": _normalize_text(pane).lower() or None,
            "status": _normalize_text(status).lower() or None,
            "assigned_user_id": assigned_user_id,
            "only_red_zone": only_red_zone,
            "exclude_red_zone": exclude_red_zone,
            "min_crime_score": min_crime_score,
            "max_crime_score": max_crime_score,
            "min_offender_count": min_offender_count,
            "max_offender_count": max_offender_count,
            "limit": limit,
        },
        "summary": {
            "property_count": total_properties,
            "good_count": _safe_int(decision_counts.get("GOOD")),
            "review_count": _safe_int(decision_counts.get("REVIEW")),
            "reject_count": _safe_int(decision_counts.get("REJECT")),
            "avg_asking_price": round(total_asking / asking_count, 2) if asking_count else 0.0,
            "avg_projected_monthly_cashflow": round(total_cashflow / cashflow_count, 2) if cashflow_count else 0.0,
            "avg_dscr": round(total_dscr / dscr_count, 3) if dscr_count else 0.0,
        },
        "kpis": {
            "total_homes": total_properties,
            "good_deals": _safe_int(decision_counts.get("GOOD")),
            "review_deals": _safe_int(decision_counts.get("REVIEW")),
            "rejected_deals": _safe_int(decision_counts.get("REJECT")),
            "avg_crime_score": round(total_crime / crime_count, 2) if crime_count else None,
            "avg_dscr": round(total_dscr / dscr_count, 3) if dscr_count else None,
            "avg_cashflow_estimate": round(total_cashflow / cashflow_count, 2) if cashflow_count else None,
            "enriched_count": _safe_int(completeness_counts.get("COMPLETE")),
            "partial_count": _safe_int(completeness_counts.get("PARTIAL")),
            "missing_count": _safe_int(completeness_counts.get("MISSING")),
        },
        "counts": {
            "properties": total_properties,
            "deals": total_properties,
            "rehab_tasks_total": 0,
            "rehab_tasks_open": 0,
            "transactions_window": 0,
            "valuations": 0,
        },
        "buckets": {
            "decisions": {k: int(v) for k, v in decision_counts.items()},
            "stages": {k: int(v) for k, v in stage_counts.items()},
            "panes": {k: int(v) for k, v in pane_counts.items()},
            "counties": {k: int(v) for k, v in county_counts.items()},
            "completeness": {k: int(v) for k, v in completeness_counts.items()},
        },
        "stage_counts": {k: int(v) for k, v in stage_counts.items()},
        "pane_counts": {k: int(v) for k, v in pane_counts.items()},
        "charts": {
            "decision_mix": [
                {"key": key, "label": key.title(), "value": int(value)}
                for key, value in sorted(decision_counts.items(), key=lambda item: item[0])
            ],
            "stage_mix": [
                {"key": key, "label": key.title(), "value": int(value)}
                for key, value in sorted(stage_counts.items(), key=lambda item: item[0])
            ],
            "pane_mix": [
                {"key": key, "label": key.title(), "value": int(value)}
                for key, value in sorted(pane_counts.items(), key=lambda item: item[0])
            ],
            "county_mix": [
                {"key": key, "label": key, "value": int(value)}
                for key, value in sorted(county_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
            ],
            "completeness_mix": [
                {"key": key, "label": key.title(), "value": int(value)}
                for key, value in sorted(completeness_counts.items(), key=lambda item: item[0])
            ],
        },
        "series": {
            "decision_mix": [
                {"key": key, "label": key.title(), "count": int(value)}
                for key, value in sorted(decision_counts.items(), key=lambda item: item[0])
            ],
            "workflow_mix": [
                {"key": key, "label": key.title(), "count": int(value)}
                for key, value in sorted(stage_counts.items(), key=lambda item: item[0])
            ],
            "pane_mix": [
                {"key": key, "label": key.title(), "count": int(value)}
                for key, value in sorted(pane_counts.items(), key=lambda item: item[0])
            ],
            "county_mix": [
                {"key": key, "label": key, "count": int(value)}
                for key, value in sorted(county_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
            ],
            "completeness_mix": [
                {"key": key, "label": key.title(), "count": int(value)}
                for key, value in sorted(completeness_counts.items(), key=lambda item: item[0])
            ],
            "cash_by_month": [],
        },
        "leaderboards": _leaderboards(rows),
        "rows": rows[: int(limit)],
        "meta": {
            "inventory_meta": inventory.get("meta") or {},
            "total_ms": duration_ms,
        },
    }