from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Optional

from .property_inventory_snapshot_service import build_inventory_snapshots_for_scope
from .runtime_metrics import METRICS

log = logging.getLogger("onehaven.pane_snapshot")

PANES = ["investor", "acquisition", "compliance", "tenants", "management", "admin"]


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _stage_matches_pane(pane: str, stage: str) -> bool:
    stage = str(stage or "").strip().lower()

    if pane == "investor":
        return stage in {"discovered", "shortlisted", "underwritten", "offer"}
    if pane == "acquisition":
        return stage in {"offer", "acquired"}
    if pane == "compliance":
        return stage in {"rehab", "compliance_readying", "inspection_pending"}
    if pane == "tenants":
        return stage in {"tenant_marketing", "tenant_screening", "leased"}
    if pane == "management":
        return stage in {"occupied", "turnover", "maintenance"}
    if pane == "admin":
        return True
    return False


def _filter_rows_for_pane(rows: list[dict[str, Any]], pane: str) -> list[dict[str, Any]]:
    if pane == "admin":
        return rows
    return [row for row in rows if _stage_matches_pane(pane, str(row.get("current_stage") or ""))]


def _blocker_summary(rows: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    counter: dict[str, int] = defaultdict(int)
    for row in rows:
      for blocker in row.get("blockers") or []:
          key = str(blocker or "").strip()
          if key:
              counter[key] += 1

    return [
        {"blocker": blocker, "count": count}
        for blocker, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def _action_summary(rows: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    counter: dict[str, int] = defaultdict(int)
    for row in rows:
        for action in (row.get("next_actions") or [])[:2]:
            key = str(action or "").strip()
            if key:
                counter[key] += 1

    return [
        {"action": action, "count": count}
        for action, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def _pane_kpis(pane: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    with_blockers = sum(1 for row in rows if row.get("blockers"))
    with_next_actions = sum(1 for row in rows if row.get("next_actions"))
    enriched = sum(1 for row in rows if row.get("completeness") == "COMPLETE")
    partial = sum(1 for row in rows if row.get("completeness") == "PARTIAL")
    missing = sum(1 for row in rows if row.get("completeness") == "MISSING")

    kpis: dict[str, Any] = {
        "total_properties": total,
        "with_blockers": with_blockers,
        "with_next_actions": with_next_actions,
        "enriched_count": enriched,
        "partial_count": partial,
        "missing_count": missing,
    }

    if pane == "investor":
        good = sum(1 for row in rows if str(row.get("normalized_decision") or "").upper() == "GOOD")
        review = sum(1 for row in rows if str(row.get("normalized_decision") or "").upper() == "REVIEW")
        cashflows = [row.get("projected_monthly_cashflow") for row in rows if row.get("projected_monthly_cashflow") is not None]
        kpis.update(
            {
                "good_candidates": good,
                "review_candidates": review,
                "avg_cashflow_estimate": round(
                    sum(_safe_float(v) for v in cashflows) / len(cashflows),
                    2,
                ) if cashflows else 0.0,
            }
        )

    if pane == "compliance":
        kpis.update(
            {
                "inspection_pending_count": sum(
                    1 for row in rows if row.get("current_stage") == "inspection_pending"
                ),
            }
        )

    if pane == "management":
        kpis.update(
            {
                "maintenance_count": sum(
                    1 for row in rows if row.get("current_stage") == "maintenance"
                ),
                "turnover_count": sum(
                    1 for row in rows if row.get("current_stage") == "turnover"
                ),
            }
        )

    return kpis


def build_pane_summary_snapshots(
    db,
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
    limit: int = 250,
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
        limit=limit,
    )

    rows = list(inventory.get("rows") or [])
    panes: list[dict[str, Any]] = []

    for pane in PANES:
        pane_rows = _filter_rows_for_pane(rows, pane)
        panes.append(
            {
                "pane": pane,
                "count": len(pane_rows),
                "kpis": _pane_kpis(pane, pane_rows),
                "top_blockers": _blocker_summary(pane_rows, limit=3),
                "top_actions": _action_summary(pane_rows, limit=3),
            }
        )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    METRICS.observe_ms("pane_snapshot_total_ms", total_ms, labels={"org_id": org_id})
    METRICS.inc("pane_snapshot_build_count", labels={"org_id": org_id})

    log.info(
        "pane_summary_snapshots_complete",
        extra={
            "event": "pane_summary_snapshots_complete",
            "org_id": org_id,
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "assigned_user_id": assigned_user_id,
            "limit": limit,
            "source_count": len(rows),
            "pane_count": len(panes),
            "total_ms": total_ms,
        },
    )

    return {
        "ok": True,
        "panes": panes,
        "meta": {
            "source_count": len(rows),
            "pane_count": len(panes),
            "total_ms": total_ms,
            "inventory_meta": inventory.get("meta") or {},
        },
    }