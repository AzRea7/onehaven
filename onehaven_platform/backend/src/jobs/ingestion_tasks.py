from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.db import SessionLocal, rollback_quietly
from onehaven_platform.backend.src.services.geo_enrichment import enrich_property_geo
from onehaven_platform.backend.src.adapters.acquire_adapter import (
    execute_post_ingestion_pipeline,
    refresh_property_rent_assumptions,
)
from onehaven_platform.backend.src.adapters.acquire_adapter import execute_source_sync
from onehaven_platform.backend.src.adapters.acquire_adapter import (
    build_jurisdiction_refresh_payload,
    build_location_refresh_payload,
    build_lock_owner,
    dispatch_daily_sync_for_org,
    list_jurisdictions_needing_refresh,
    list_org_ids_with_enabled_sources,
    list_properties_needing_location_refresh,
)
from onehaven_platform.backend.src.adapters.acquire_adapter import (
    ensure_default_manual_sources,
    get_source,
    list_sources,
    resolve_sources_for_market,
)
from onehaven_platform.backend.src.adapters.compliance_adapter import notify_stale_jurisdictions
from onehaven_platform.backend.src.adapters.compliance_adapter import (
    DEFAULT_JURISDICTION_STALE_DAYS,
    refresh_jurisdiction_profile,
)
from onehaven_platform.backend.src.services.locks_service import (
    acquire_lock,
    is_lock_active,
    release_lock,
    release_ingestion_execution_lock_in_new_session,
)
from onehaven_platform.backend.src.adapters.intelligence_adapter import (
    get_market_sync_state_by_id,
    mark_backfill_completed,
)
from products.intelligence.backend.src.services.rent_refresh_queue_service import (
    list_properties_for_budgeted_rent_refresh,
    should_queue_rent_refresh_after_sync,
)
from onehaven_platform.backend.src.jobs.celery_app import celery_app

logger = logging.getLogger(__name__)

DEFAULT_TASK_LOCK_TTL_SECONDS = 60 * 30
DEFAULT_TASK_DONE_TTL_SECONDS = 60 * 60 * 24
DEFAULT_BACKFILL_ENQUEUE_LOCK_TTL_SECONDS = 60 * 15


INCOMPLETE_COMPLETENESS_STATUSES = (
    "missing",
    "partial",
    "stale",
    "pending",
    "error",
    "failed",
)


def _property_columns(db) -> set[str]:
    cached = getattr(db, "_onehaven_properties_columns", None)
    if cached is not None:
        return cached

    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'properties'
            """
        )
    ).fetchall()
    cols = {str(row[0]).strip() for row in rows if row and row[0]}
    setattr(db, "_onehaven_properties_columns", cols)
    return cols


def _property_has_column(db, column_name: str) -> bool:
    return str(column_name).strip() in _property_columns(db)


def _optional_select_expr(db, column_name: str, *, alias: str | None = None, blank_string: bool = False) -> str:
    label = alias or column_name
    if _property_has_column(db, column_name):
        return f"{column_name} AS {label}" if label != column_name else column_name
    if blank_string:
        return f"'' AS {label}"
    return f"NULL AS {label}"


def _incomplete_property_sql(db) -> str:
    clauses = [
        "COALESCE(completeness_geo_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_rent_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_risk_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_jurisdiction_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_cashflow_status, 'missing') = ANY(:incomplete_statuses)",
    ]

    if _property_has_column(db, "lat"):
        clauses.append("lat IS NULL")
    if _property_has_column(db, "lng"):
        clauses.append("lng IS NULL")
    if _property_has_column(db, "normalized_address"):
        clauses.append("NULLIF(TRIM(COALESCE(normalized_address, '')), '') IS NULL")
    if _property_has_column(db, "jurisdiction_code"):
        clauses.append("NULLIF(TRIM(COALESCE(jurisdiction_code, '')), '') IS NULL")

    rent_cols = [
        c
        for c in ("market_rent_estimate", "section8_fmr", "approved_rent_ceiling")
        if _property_has_column(db, c)
    ]
    if rent_cols:
        clauses.append("(" + " AND ".join(f"{c} IS NULL" for c in rent_cols) + ")")

    if _property_has_column(db, "crime_score"):
        clauses.append("crime_score IS NULL")
    if _property_has_column(db, "offender_count"):
        clauses.append("offender_count IS NULL")

    return "(\n      " + "\n   OR ".join(clauses) + "\n)"


def _incomplete_status_values() -> list[str]:
    return [str(v) for v in INCOMPLETE_COMPLETENESS_STATUSES]


def _property_needs_enrichment(db, *, org_id: int, property_id: int) -> bool:
    row = db.execute(
        text(
            f"""
            SELECT id
            FROM properties
            WHERE org_id = :org_id
              AND id = :property_id
              AND {_incomplete_property_sql(db)}
            LIMIT 1
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "incomplete_statuses": _incomplete_status_values(),
        },
    ).fetchone()
    return row is not None


def _property_completion_snapshot(db, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    row = db.execute(
        text(
            f"""
            SELECT
                id,
                COALESCE(completeness_geo_status, 'missing') AS completeness_geo_status,
                COALESCE(completeness_rent_status, 'missing') AS completeness_rent_status,
                COALESCE(completeness_risk_status, 'missing') AS completeness_risk_status,
                COALESCE(completeness_jurisdiction_status, 'missing') AS completeness_jurisdiction_status,
                COALESCE(completeness_cashflow_status, 'missing') AS completeness_cashflow_status,
                {_optional_select_expr(db, 'lat')},
                {_optional_select_expr(db, 'lng')},
                {_optional_select_expr(db, 'normalized_address', blank_string=True)},
                {_optional_select_expr(db, 'jurisdiction_code', blank_string=True)},
                {_optional_select_expr(db, 'market_rent_estimate')},
                {_optional_select_expr(db, 'section8_fmr')},
                {_optional_select_expr(db, 'approved_rent_ceiling')},
                {_optional_select_expr(db, 'crime_score')},
                {_optional_select_expr(db, 'offender_count')},
                {'TRUE' if _property_has_column(db, 'lat') else 'FALSE'} AS __has_lat,
                {'TRUE' if _property_has_column(db, 'lng') else 'FALSE'} AS __has_lng,
                {'TRUE' if _property_has_column(db, 'normalized_address') else 'FALSE'} AS __has_normalized_address,
                {'TRUE' if _property_has_column(db, 'jurisdiction_code') else 'FALSE'} AS __has_jurisdiction_code,
                {'TRUE' if _property_has_column(db, 'market_rent_estimate') else 'FALSE'} AS __has_market_rent_estimate,
                {'TRUE' if _property_has_column(db, 'section8_fmr') else 'FALSE'} AS __has_section8_fmr,
                {'TRUE' if _property_has_column(db, 'approved_rent_ceiling') else 'FALSE'} AS __has_approved_rent_ceiling,
                {'TRUE' if _property_has_column(db, 'crime_score') else 'FALSE'} AS __has_crime_score,
                {'TRUE' if _property_has_column(db, 'offender_count') else 'FALSE'} AS __has_offender_count
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            LIMIT 1
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()
    return dict(row) if row is not None else None


def _property_is_complete(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False

    required_statuses = (
        "completeness_geo_status",
        "completeness_rent_status",
        "completeness_risk_status",
        "completeness_jurisdiction_status",
        "completeness_cashflow_status",
    )
    statuses_complete = all(
        str(snapshot.get(key) or "").strip().lower() == "complete"
        for key in required_statuses
    )
    if not statuses_complete:
        return False

    require_geo = bool(snapshot.get("__has_lat")) and bool(snapshot.get("__has_lng"))
    has_geo = (not require_geo) or (snapshot.get("lat") is not None and snapshot.get("lng") is not None)

    require_address = bool(snapshot.get("__has_normalized_address"))
    has_address = (not require_address) or bool(str(snapshot.get("normalized_address") or "").strip())

    require_jurisdiction = bool(snapshot.get("__has_jurisdiction_code"))
    has_jurisdiction = (not require_jurisdiction) or bool(str(snapshot.get("jurisdiction_code") or "").strip())

    rent_keys = [
        key
        for key, present in (
            ("market_rent_estimate", snapshot.get("__has_market_rent_estimate")),
            ("section8_fmr", snapshot.get("__has_section8_fmr")),
            ("approved_rent_ceiling", snapshot.get("__has_approved_rent_ceiling")),
        )
        if present
    ]
    has_rent = (not rent_keys) or any(snapshot.get(key) is not None for key in rent_keys)

    require_risk = bool(snapshot.get("__has_crime_score")) or bool(snapshot.get("__has_offender_count"))
    has_risk = (not require_risk) or (
        (not bool(snapshot.get("__has_crime_score")) or snapshot.get("crime_score") is not None)
        and (not bool(snapshot.get("__has_offender_count")) or snapshot.get("offender_count") is not None)
    )

    return has_geo and has_address and has_jurisdiction and has_rent and has_risk



INCOMPLETE_COMPLETENESS_STATUSES = (
    "missing",
    "partial",
    "stale",
    "pending",
    "error",
    "failed",
)


def _property_columns(db) -> set[str]:
    cached = getattr(db, "_onehaven_properties_columns", None)
    if cached is not None:
        return cached

    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'properties'
            """
        )
    ).fetchall()
    cols = {str(row[0]).strip() for row in rows if row and row[0]}
    setattr(db, "_onehaven_properties_columns", cols)
    return cols


def _property_has_column(db, column_name: str) -> bool:
    return str(column_name).strip() in _property_columns(db)


def _optional_select_expr(db, column_name: str, *, alias: str | None = None, blank_string: bool = False) -> str:
    label = alias or column_name
    if _property_has_column(db, column_name):
        return f"{column_name} AS {label}" if label != column_name else column_name
    if blank_string:
        return f"'' AS {label}"
    return f"NULL AS {label}"


def _incomplete_property_sql(db) -> str:
    clauses = [
        "COALESCE(completeness_geo_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_rent_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_risk_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_jurisdiction_status, 'missing') = ANY(:incomplete_statuses)",
        "COALESCE(completeness_cashflow_status, 'missing') = ANY(:incomplete_statuses)",
    ]

    if _property_has_column(db, "lat"):
        clauses.append("lat IS NULL")
    if _property_has_column(db, "lng"):
        clauses.append("lng IS NULL")
    if _property_has_column(db, "normalized_address"):
        clauses.append("NULLIF(TRIM(COALESCE(normalized_address, '')), '') IS NULL")
    if _property_has_column(db, "jurisdiction_code"):
        clauses.append("NULLIF(TRIM(COALESCE(jurisdiction_code, '')), '') IS NULL")

    rent_cols = [
        c
        for c in ("market_rent_estimate", "section8_fmr", "approved_rent_ceiling")
        if _property_has_column(db, c)
    ]
    if rent_cols:
        clauses.append("(" + " AND ".join(f"{c} IS NULL" for c in rent_cols) + ")")

    if _property_has_column(db, "crime_score"):
        clauses.append("crime_score IS NULL")
    if _property_has_column(db, "offender_count"):
        clauses.append("offender_count IS NULL")

    return "(\n      " + "\n   OR ".join(clauses) + "\n)"


def _incomplete_status_values() -> list[str]:
    return [str(v) for v in INCOMPLETE_COMPLETENESS_STATUSES]


def _property_needs_enrichment(db, *, org_id: int, property_id: int) -> bool:
    row = db.execute(
        text(
            f"""
            SELECT id
            FROM properties
            WHERE org_id = :org_id
              AND id = :property_id
              AND {_incomplete_property_sql(db)}
            LIMIT 1
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "incomplete_statuses": _incomplete_status_values(),
        },
    ).fetchone()
    return row is not None


def _property_completion_snapshot(db, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    row = db.execute(
        text(
            f"""
            SELECT
                id,
                COALESCE(completeness_geo_status, 'missing') AS completeness_geo_status,
                COALESCE(completeness_rent_status, 'missing') AS completeness_rent_status,
                COALESCE(completeness_risk_status, 'missing') AS completeness_risk_status,
                COALESCE(completeness_jurisdiction_status, 'missing') AS completeness_jurisdiction_status,
                COALESCE(completeness_cashflow_status, 'missing') AS completeness_cashflow_status,
                {_optional_select_expr(db, 'lat')},
                {_optional_select_expr(db, 'lng')},
                {_optional_select_expr(db, 'normalized_address', blank_string=True)},
                {_optional_select_expr(db, 'jurisdiction_code', blank_string=True)},
                {_optional_select_expr(db, 'market_rent_estimate')},
                {_optional_select_expr(db, 'section8_fmr')},
                {_optional_select_expr(db, 'approved_rent_ceiling')},
                {_optional_select_expr(db, 'crime_score')},
                {_optional_select_expr(db, 'offender_count')},
                {'TRUE' if _property_has_column(db, 'lat') else 'FALSE'} AS __has_lat,
                {'TRUE' if _property_has_column(db, 'lng') else 'FALSE'} AS __has_lng,
                {'TRUE' if _property_has_column(db, 'normalized_address') else 'FALSE'} AS __has_normalized_address,
                {'TRUE' if _property_has_column(db, 'jurisdiction_code') else 'FALSE'} AS __has_jurisdiction_code,
                {'TRUE' if _property_has_column(db, 'market_rent_estimate') else 'FALSE'} AS __has_market_rent_estimate,
                {'TRUE' if _property_has_column(db, 'section8_fmr') else 'FALSE'} AS __has_section8_fmr,
                {'TRUE' if _property_has_column(db, 'approved_rent_ceiling') else 'FALSE'} AS __has_approved_rent_ceiling,
                {'TRUE' if _property_has_column(db, 'crime_score') else 'FALSE'} AS __has_crime_score,
                {'TRUE' if _property_has_column(db, 'offender_count') else 'FALSE'} AS __has_offender_count
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            LIMIT 1
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()
    return dict(row) if row is not None else None


def _property_is_complete(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False

    required_statuses = (
        "completeness_geo_status",
        "completeness_rent_status",
        "completeness_risk_status",
        "completeness_jurisdiction_status",
        "completeness_cashflow_status",
    )
    statuses_complete = all(
        str(snapshot.get(key) or "").strip().lower() == "complete"
        for key in required_statuses
    )
    if not statuses_complete:
        return False

    require_geo = bool(snapshot.get("__has_lat")) and bool(snapshot.get("__has_lng"))
    has_geo = (not require_geo) or (snapshot.get("lat") is not None and snapshot.get("lng") is not None)

    require_address = bool(snapshot.get("__has_normalized_address"))
    has_address = (not require_address) or bool(str(snapshot.get("normalized_address") or "").strip())

    require_jurisdiction = bool(snapshot.get("__has_jurisdiction_code"))
    has_jurisdiction = (not require_jurisdiction) or bool(str(snapshot.get("jurisdiction_code") or "").strip())

    rent_keys = [
        key
        for key, present in (
            ("market_rent_estimate", snapshot.get("__has_market_rent_estimate")),
            ("section8_fmr", snapshot.get("__has_section8_fmr")),
            ("approved_rent_ceiling", snapshot.get("__has_approved_rent_ceiling")),
        )
        if present
    ]
    has_rent = (not rent_keys) or any(snapshot.get(key) is not None for key in rent_keys)

    require_risk = bool(snapshot.get("__has_crime_score")) or bool(snapshot.get("__has_offender_count"))
    has_risk = (not require_risk) or (
        (not bool(snapshot.get("__has_crime_score")) or snapshot.get("crime_score") is not None)
        and (not bool(snapshot.get("__has_offender_count")) or snapshot.get("offender_count") is not None)
    )

    return has_geo and has_address and has_jurisdiction and has_rent and has_risk



def _retry_enabled() -> bool:
    return bool(getattr(settings, "ingestion_retry_transient_failures", False))


def _max_retries() -> int:
    return max(0, int(getattr(settings, "ingestion_sync_task_max_retries", 1) or 0))


def _retry_delay_seconds() -> int:
    return max(1, int(getattr(settings, "ingestion_retry_delay_seconds", 30) or 30))


def _is_transient_error(exc: Exception) -> bool:
    return isinstance(exc, (OperationalError, DBAPIError, TimeoutError, ConnectionError))


def _org_scope(org_id: int | None, discovered: list[int]) -> list[int]:
    if org_id is not None:
        return [int(org_id)]
    return discovered or [1]


def _pipeline_outcome(summary_json: dict | None) -> dict:
    summary = dict(summary_json or {})
    return {
        "records_seen": int(summary.get("records_seen", 0) or 0),
        "records_imported": int(summary.get("records_imported", 0) or 0),
        "properties_created": int(summary.get("properties_created", 0) or 0),
        "properties_updated": int(summary.get("properties_updated", 0) or 0),
        "deals_created": int(summary.get("deals_created", 0) or 0),
        "deals_updated": int(summary.get("deals_updated", 0) or 0),
        "rent_rows_upserted": int(summary.get("rent_rows_upserted", 0) or 0),
        "photos_upserted": int(summary.get("photos_upserted", 0) or 0),
        "duplicates_skipped": int(summary.get("duplicates_skipped", 0) or 0),
        "invalid_rows": int(summary.get("invalid_rows", 0) or 0),
        "filtered_out": int(summary.get("filtered_out", 0) or 0),
        "geo_enriched": int(summary.get("geo_enriched", 0) or 0),
        "risk_scored": int(summary.get("risk_scored", 0) or 0),
        "rent_refreshed": int(summary.get("rent_refreshed", 0) or 0),
        "evaluated": int(summary.get("evaluated", 0) or 0),
        "state_synced": int(summary.get("state_synced", 0) or 0),
        "workflow_synced": int(summary.get("workflow_synced", 0) or 0),
        "next_actions_seeded": int(summary.get("next_actions_seeded", 0) or 0),
        "post_import_failures": int(summary.get("post_import_failures", 0) or 0),
        "post_import_partials": int(summary.get("post_import_partials", 0) or 0),
        "post_import_errors": list(summary.get("post_import_errors") or []),
        "filter_reason_counts": dict(summary.get("filter_reason_counts") or {}),
        "location_automation_enabled": bool(summary.get("location_automation_enabled", False)),
        "normal_path": bool(summary.get("normal_path", True)),
        "market_exhausted": bool(summary.get("market_exhausted", False)),
        "stop_reason": summary.get("stop_reason"),
        "post_sync_enrichment_queued": int(summary.get("post_sync_enrichment_queued", 0) or 0),
        "post_sync_property_ids": list(summary.get("post_sync_property_ids") or []),
    }


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    if hasattr(value, "model_dump"):
        try:
            return _json_safe(value.model_dump())
        except Exception:
            pass

    if hasattr(value, "dict"):
        try:
            return _json_safe(value.dict())
        except Exception:
            pass

    if hasattr(value, "__dict__"):
        try:
            return _json_safe(
                {
                    k: v
                    for k, v in vars(value).items()
                    if not str(k).startswith("_")
                }
            )
        except Exception:
            pass

    return str(value)


def _recover_source_for_task(
    db,
    *,
    org_id: int,
    source_id: int,
    runtime_config: dict | None,
):
    source = get_source(db, org_id=int(org_id), source_id=int(source_id))
    if source is not None:
        return source

    cfg = dict(runtime_config or {})
    market_slug = str(cfg.get("market_slug") or "").strip().lower()
    if not market_slug:
        return None

    matches = resolve_sources_for_market(
        db,
        org_id=int(org_id),
        market_slug=market_slug,
    )
    if not matches:
        return None

    enabled = [m for m in matches if bool(getattr(m, "is_enabled", False))]
    if not enabled:
        return None

    enabled.sort(
        key=lambda s: (
            str(getattr(s, "slug", "") or ""),
            int(getattr(s, "id", 0) or 0),
        )
    )
    return enabled[0]


def _task_lock_ttl_seconds() -> int:
    return int(
        getattr(settings, "ingestion_property_task_lock_ttl_seconds", DEFAULT_TASK_LOCK_TTL_SECONDS)
        or DEFAULT_TASK_LOCK_TTL_SECONDS
    )


def _task_done_ttl_seconds() -> int:
    return int(
        getattr(settings, "ingestion_property_task_done_ttl_seconds", DEFAULT_TASK_DONE_TTL_SECONDS)
        or DEFAULT_TASK_DONE_TTL_SECONDS
    )


def _backfill_enqueue_lock_ttl_seconds() -> int:
    return int(
        getattr(settings, "ingestion_backfill_enqueue_lock_ttl_seconds", DEFAULT_BACKFILL_ENQUEUE_LOCK_TTL_SECONDS)
        or DEFAULT_BACKFILL_ENQUEUE_LOCK_TTL_SECONDS
    )


def _task_lock_owner(prefix: str) -> str:
    return build_lock_owner(prefix=prefix)


def _property_enrich_lock_key(org_id: int, property_id: int) -> str:
    return f"property_enrich:{int(org_id)}:{int(property_id)}"


def _property_enrich_done_key(org_id: int, property_id: int) -> str:
    return f"property_enrich_done:{int(org_id)}:{int(property_id)}"


def _rent_refresh_lock_key(org_id: int, property_id: int) -> str:
    return f"rent_refresh:{int(org_id)}:{int(property_id)}"


def _location_refresh_lock_key(org_id: int, property_id: int) -> str:
    return f"location_refresh:{int(org_id)}:{int(property_id)}"


def _jurisdiction_refresh_lock_key(org_id: int, jurisdiction_code: str) -> str:
    return f"jurisdiction_refresh:{int(org_id)}:{str(jurisdiction_code).strip().lower()}"


def _city_backfill_lock_key(org_id: int, city: str, state: str, zip_codes: list[str], limit: int) -> str:
    normalized_zips = sorted(str(z).strip() for z in (zip_codes or []) if str(z).strip())
    digest = hashlib.sha256(
        json.dumps(
            {
                "org_id": int(org_id),
                "city": str(city).strip().lower(),
                "state": str(state).strip().upper(),
                "zip_codes": normalized_zips,
                "limit": int(limit),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"backfill_city:{int(org_id)}:{str(state).strip().upper()}:{str(city).strip().lower()}:{digest}"


def _jurisdiction_backfill_lock_key(org_id: int, jurisdiction_code: str, limit: int) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {
                "org_id": int(org_id),
                "jurisdiction_code": str(jurisdiction_code).strip().lower(),
                "limit": int(limit),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"backfill_jurisdiction:{int(org_id)}:{str(jurisdiction_code).strip().lower()}:{digest}"





def _property_geo_retry_snapshot(db, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    row = db.execute(
        text(
            f"""
            SELECT
                id,
                {_optional_select_expr(db, 'address', blank_string=True)},
                {_optional_select_expr(db, 'city', blank_string=True)},
                {_optional_select_expr(db, 'state', blank_string=True)},
                {_optional_select_expr(db, 'zip', blank_string=True)},
                {_optional_select_expr(db, 'county', blank_string=True)},
                {_optional_select_expr(db, 'normalized_address', blank_string=True)},
                {_optional_select_expr(db, 'geocode_source', blank_string=True)},
                {_optional_select_expr(db, 'lat')},
                {_optional_select_expr(db, 'lng')},
                {_optional_select_expr(db, 'geocode_last_refreshed')},
                COALESCE(completeness_geo_status, 'missing') AS completeness_geo_status
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            LIMIT 1
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()
    return dict(row) if row is not None else None


def _property_has_geo_retryable_input(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False
    address = str(snapshot.get("address") or "").strip()
    city = str(snapshot.get("city") or "").strip()
    state = str(snapshot.get("state") or "").strip()
    return bool(address and city and state and "test st" not in address.lower())


def _property_needs_geo_retry(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False
    geo_status = str(snapshot.get("completeness_geo_status") or "missing").strip().lower()
    normalized_address = str(snapshot.get("normalized_address") or "").strip()
    geocode_source = str(snapshot.get("geocode_source") or "").strip()
    lat = snapshot.get("lat")
    lng = snapshot.get("lng")
    return bool(
        geo_status in INCOMPLETE_COMPLETENESS_STATUSES
        or not normalized_address
        or not geocode_source
        or lat is None
        or lng is None
    )


def _prime_geo_retry(db, *, org_id: int, property_id: int) -> dict[str, Any]:
    snapshot = _property_geo_retry_snapshot(db, org_id=int(org_id), property_id=int(property_id))
    if not _property_has_geo_retryable_input(snapshot):
        return {"ok": False, "skipped": True, "reason": "geo_retry_input_unavailable", "snapshot": snapshot}
    if not _property_needs_geo_retry(snapshot):
        return {"ok": True, "skipped": True, "reason": "geo_already_present", "snapshot": snapshot}

    result = enrich_property_geo(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
        force=True,
    )
    result = result if isinstance(result, dict) else {"ok": bool(result)}
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise
    return result


def _run_geo_retry_in_isolated_session(*, org_id: int, property_id: int) -> dict[str, Any]:
    isolated_db = SessionLocal()
    try:
        result = _prime_geo_retry(
            isolated_db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        isolated_db.commit()
        return result if isinstance(result, dict) else {"ok": bool(result)}
    except Exception as exc:
        isolated_db.rollback()
        logger.exception(
            "task_geo_retry_isolated_failed",
            extra={"org_id": int(org_id), "property_id": int(property_id)},
        )
        return {
            "ok": False,
            "skipped": True,
            "reason": "geo_retry_isolated_failed",
            "error": f"{type(exc).__name__}:{exc}",
        }
    finally:
        isolated_db.close()


def _should_retry_again_after_pipeline(*, pipeline_result: dict[str, Any] | None, completion_snapshot: dict[str, Any] | None) -> bool:
    result = dict(pipeline_result or {})
    geo_payload = dict(result.get("geo") or {})
    if bool(result.get("geo_ok")):
        return False
    if _property_is_complete(completion_snapshot):
        return False
    if geo_payload.get("lat") is not None and geo_payload.get("lng") is not None:
        return False
    if str(geo_payload.get("geocode_source") or "").strip():
        return False
    if str(geo_payload.get("normalized_address") or "").strip():
        return False
    return True

def _clear_false_done_marker(db, *, org_id: int, property_id: int, owner: str) -> bool:
    done_key = _property_enrich_done_key(int(org_id), int(property_id))
    if not is_lock_active(db, org_id=int(org_id), lock_key=done_key):
        return False

    snapshot = _property_completion_snapshot(db, org_id=int(org_id), property_id=int(property_id))
    if _property_is_complete(snapshot):
        return False

    release_lock(
        db,
        org_id=int(org_id),
        lock_key=done_key,
        owner=owner,
        force=True,
    )
    return True


def _enqueue_enrichment_if_needed(
    *,
    db,
    org_id: int,
    property_id: int,
    source_id: int | None,
    run_id: int | None,
) -> bool:
    if not _property_needs_enrichment(db, org_id=int(org_id), property_id=int(property_id)):
        return False

    if is_lock_active(
        db,
        org_id=int(org_id),
        lock_key=_property_enrich_lock_key(int(org_id), int(property_id)),
    ):
        return False

    if is_lock_active(
        db,
        org_id=int(org_id),
        lock_key=_property_enrich_done_key(int(org_id), int(property_id)),
    ):
        if not _clear_false_done_marker(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            owner=_task_lock_owner("property_enrich"),
        ):
            return False

    enrich_property_after_sync_task.delay(
        org_id=int(org_id),
        property_id=int(property_id),
        source_id=source_id,
        run_id=run_id,
    )
    return True


def _finalize_json_result(
    *,
    ok: bool,
    base: dict[str, Any],
    result: Any = None,
) -> dict[str, Any]:
    payload = dict(base)
    payload["ok"] = bool(ok)
    if result is not None:
        payload["result"] = _json_safe(result)
    return _json_safe(payload)


@celery_app.task(
    name="ingestion.sync_source",
    bind=True,
    max_retries=int(getattr(settings, "ingestion_sync_task_max_retries", 1) or 1),
    default_retry_delay=int(getattr(settings, "ingestion_retry_delay_seconds", 30) or 30),
    soft_time_limit=int(getattr(settings, "ingestion_task_soft_time_limit_seconds", 240) or 240),
    time_limit=int(getattr(settings, "ingestion_task_hard_time_limit_seconds", 300) or 300),
)
def sync_source_task(
    self,
    org_id: int,
    source_id: int,
    trigger_type: str = "manual",
    runtime_config: dict | None = None,
):
    db = SessionLocal()
    try:
        source = _recover_source_for_task(
            db,
            org_id=int(org_id),
            source_id=int(source_id),
            runtime_config=runtime_config,
        )
        if source is None:
            return _finalize_json_result(
                ok=False,
                base={
                    "error": "source_not_found",
                    "source_id": int(source_id),
                    "market_slug": str((runtime_config or {}).get("market_slug") or "").strip().lower() or None,
                },
            )

        effective_runtime_config = dict(runtime_config or {})
        sync_mode = str(effective_runtime_config.get("sync_mode") or "refresh").strip().lower()

        run = execute_source_sync(
            db,
            org_id=int(org_id),
            source=source,
            trigger_type=str(trigger_type or "manual"),
            runtime_config=effective_runtime_config,
        )
        summary = dict(getattr(run, "summary_json", None) or {})

        if sync_mode == "backfill" and bool(effective_runtime_config.get("mark_backfill_complete_on_exhaustion")):
            sync_state = get_market_sync_state_by_id(
                db,
                sync_state_id=effective_runtime_config.get("market_sync_state_id"),
                org_id=int(org_id),
            )
            if sync_state is not None and bool(summary.get("market_exhausted")):
                mark_backfill_completed(db, sync_state=sync_state)

        post_sync_rent_queued = 0
        post_sync_rent_property_ids: list[int] = []

        if should_queue_rent_refresh_after_sync():
            burst_limit = int(getattr(settings, "ingestion_post_sync_rent_budget", 5) or 5)
            property_ids = list_properties_for_budgeted_rent_refresh(
                db,
                org_id=int(org_id),
                limit=burst_limit,
            )
            for property_id in property_ids:
                refresh_property_rent_task.delay(int(org_id), int(property_id))
                post_sync_rent_queued += 1
                post_sync_rent_property_ids.append(int(property_id))

        return _finalize_json_result(
            ok=True,
            base={
                "run_id": getattr(run, "id", None),
                "status": getattr(run, "status", None),
                "source_id": int(getattr(source, "id")),
                "sync_mode": sync_mode,
                "post_sync_rent_queued": post_sync_rent_queued,
                "post_sync_rent_property_ids": post_sync_rent_property_ids,
                "summary_json": _json_safe(summary),
                "pipeline_outcome": _json_safe(_pipeline_outcome(summary)),
            },
        )
    except Exception as exc:
        rollback_quietly(db)
        logger.exception("sync_source_task_failed", extra={"org_id": int(org_id), "source_id": int(source_id)})
        if _retry_enabled() and _is_transient_error(exc) and int(getattr(self.request, "retries", 0) or 0) < _max_retries():
            raise self.retry(exc=exc, countdown=_retry_delay_seconds())
        raise
    finally:
        db.close()


@celery_app.task(name="ingestion.enrich_property_after_sync")
def enrich_property_after_sync_task(
    *,
    org_id: int,
    property_id: int,
    source_id: int | None = None,
    run_id: int | None = None,
):
    db = SessionLocal()
    owner = _task_lock_owner("property_enrich")
    lock_key = _property_enrich_lock_key(int(org_id), int(property_id))
    done_key = _property_enrich_done_key(int(org_id), int(property_id))

    try:
        cleared_false_done = _clear_false_done_marker(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            owner=owner,
        )
        if cleared_false_done:
            db.commit()

        snapshot = _property_completion_snapshot(db, org_id=int(org_id), property_id=int(property_id))
        if _property_is_complete(snapshot):
            acquire_lock(
                db,
                org_id=int(org_id),
                lock_key=done_key,
                owner=owner,
                ttl_seconds=_task_done_ttl_seconds(),
            )
            db.commit()
            return _finalize_json_result(
                ok=True,
                base={
                    "org_id": int(org_id),
                    "property_id": int(property_id),
                    "source_id": source_id,
                    "run_id": run_id,
                    "skipped": True,
                    "reason": "property_already_complete",
                },
            )

        lock = acquire_lock(
            db,
            org_id=int(org_id),
            lock_key=lock_key,
            owner=owner,
            ttl_seconds=_task_lock_ttl_seconds(),
        )
        if not lock.acquired:
            return _finalize_json_result(
                ok=True,
                base={
                    "org_id": int(org_id),
                    "property_id": int(property_id),
                    "source_id": source_id,
                    "run_id": run_id,
                    "skipped": True,
                    "reason": "property_enrichment_lock_not_acquired",
                },
            )

        snapshot = _property_completion_snapshot(db, org_id=int(org_id), property_id=int(property_id))
        if _property_is_complete(snapshot):
            acquire_lock(
                db,
                org_id=int(org_id),
                lock_key=done_key,
                owner=owner,
                ttl_seconds=_task_done_ttl_seconds(),
            )
            release_lock(
                db,
                org_id=int(org_id),
                lock_key=lock_key,
                owner=owner,
                force=True,
            )
            db.commit()
            return _finalize_json_result(
                ok=True,
                base={
                    "org_id": int(org_id),
                    "property_id": int(property_id),
                    "source_id": source_id,
                    "run_id": run_id,
                    "skipped": True,
                    "reason": "property_already_complete",
                },
            )

        pre_geo_retry = _run_geo_retry_in_isolated_session(
            org_id=int(org_id),
            property_id=int(property_id),
        )

        result = execute_post_ingestion_pipeline(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            actor_user_id=None,
            emit_events=False,
        ) or {}

        if isinstance(result, dict):
            result["task_geo_retry_pre"] = pre_geo_retry

        snapshot = _property_completion_snapshot(db, org_id=int(org_id), property_id=int(property_id))
        if _should_retry_again_after_pipeline(
            pipeline_result=result if isinstance(result, dict) else None,
            completion_snapshot=snapshot,
        ):
            post_geo_retry = _run_geo_retry_in_isolated_session(
                org_id=int(org_id),
                property_id=int(property_id),
            )
            rerun_result = execute_post_ingestion_pipeline(
                db,
                org_id=int(org_id),
                property_id=int(property_id),
                actor_user_id=None,
                emit_events=False,
            ) or {}
            if isinstance(rerun_result, dict):
                rerun_result["task_geo_retry_pre"] = pre_geo_retry
                rerun_result["task_geo_retry_post"] = post_geo_retry
            result = rerun_result

        snapshot = _property_completion_snapshot(db, org_id=int(org_id), property_id=int(property_id))
        completed = _property_is_complete(snapshot)
        if completed:
            acquire_lock(
                db,
                org_id=int(org_id),
                lock_key=done_key,
                owner=owner,
                ttl_seconds=_task_done_ttl_seconds(),
            )
        else:
            release_lock(
                db,
                org_id=int(org_id),
                lock_key=done_key,
                owner=owner,
                force=True,
            )

        release_lock(
            db,
            org_id=int(org_id),
            lock_key=lock_key,
            owner=owner,
            force=True,
        )

        db.commit()

        return _finalize_json_result(
            ok=True,
            base={
                "org_id": int(org_id),
                "property_id": int(property_id),
                "source_id": source_id,
                "run_id": run_id,
                "completed": bool(completed),
                "needs_followup": not bool(completed),
            },
            result=result or {},
        )
    except Exception:
        db.rollback()
        logger.exception(
            "enrich_property_after_sync_task_failed",
            extra={"org_id": int(org_id), "property_id": int(property_id)},
        )
        raise
    finally:
        try:
            release_lock(
                db,
                org_id=int(org_id),
                lock_key=lock_key,
                owner=owner,
                force=True,
            )
            db.commit()
        except Exception:
            db.rollback()
        db.close()


@celery_app.task(name="ingestion.sync_due_sources")
def sync_due_sources_task(org_id: int | None = None):
    db = SessionLocal()
    try:
        org_ids = _org_scope(org_id, list_org_ids_with_enabled_sources(db))
        queued = 0
        for scoped_org_id in org_ids:
            ensure_default_manual_sources(db, org_id=int(scoped_org_id))
            db.commit()
            for source in list_sources(db, org_id=int(scoped_org_id)):
                if not bool(getattr(source, "is_enabled", False)):
                    continue
                sync_source_task.delay(int(scoped_org_id), int(source.id), "scheduled", {})
                queued += 1
        return _finalize_json_result(ok=True, base={"queued": queued})
    finally:
        db.close()


@celery_app.task(name="ingestion.daily_market_refresh")
def daily_market_refresh_task(org_id: int | None = None):
    db = SessionLocal()
    try:
        org_ids = _org_scope(org_id, list_org_ids_with_enabled_sources(db))
        queued = 0
        runs: list[dict[str, Any]] = []

        for scoped_org_id in org_ids:
            result = dispatch_daily_sync_for_org(
                db,
                org_id=int(scoped_org_id),
                sync_mode="refresh",
                enqueue_sync=lambda _org_id, _source_id, _trigger_type, _payload: sync_source_task.delay(
                    int(_org_id),
                    int(_source_id),
                    _trigger_type,
                    _payload,
                ),
            )
            queued += int(result.get("queued", 0) or 0)
            runs.append(_json_safe(result))

        return _finalize_json_result(
            ok=True,
            base={"queued": queued, "sync_mode": "refresh", "runs": runs},
        )
    finally:
        db.close()


@celery_app.task(name="rent.refresh_property")
def refresh_property_rent_task(
    org_id: int,
    property_id: int,
):
    db = SessionLocal()
    owner = _task_lock_owner("rent_refresh")
    lock_key = _rent_refresh_lock_key(int(org_id), int(property_id))
    try:
        lock = acquire_lock(
            db,
            org_id=int(org_id),
            lock_key=lock_key,
            owner=owner,
            ttl_seconds=_task_lock_ttl_seconds(),
        )
        if not lock.acquired:
            return _finalize_json_result(
                ok=True,
                base={
                    "org_id": int(org_id),
                    "property_id": int(property_id),
                    "skipped": True,
                    "reason": "rent_refresh_lock_not_acquired",
                },
            )

        result = refresh_property_rent_assumptions(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        db.commit()
        return _finalize_json_result(
            ok=bool((result or {}).get("ok", True)),
            base={
                "org_id": int(org_id),
                "property_id": int(property_id),
            },
            result=result or {},
        )
    finally:
        try:
            release_lock(
                db,
                org_id=int(org_id),
                lock_key=lock_key,
                owner=owner,
                force=False,
            )
            db.commit()
        except Exception:
            db.rollback()
        db.close()


@celery_app.task(name="rent.refresh_budgeted_batch")
def refresh_budgeted_rent_batch_task(
    org_id: int = 1,
    *,
    limit: int | None = None,
):
    db = SessionLocal()
    try:
        effective_limit = int(
            limit
            or getattr(settings, "ingestion_daily_rent_refresh_limit", 25)
            or 25
        )

        property_ids = list_properties_for_budgeted_rent_refresh(
            db,
            org_id=int(org_id),
            limit=effective_limit,
        )

        queued = 0
        queued_property_ids: list[int] = []
        for property_id in property_ids:
            refresh_property_rent_task.delay(int(org_id), int(property_id))
            queued += 1
            queued_property_ids.append(int(property_id))

        return _finalize_json_result(
            ok=True,
            base={
                "org_id": int(org_id),
                "queued": queued,
                "property_ids": queued_property_ids,
            },
        )
    finally:
        db.close()


@celery_app.task(name="location.refresh_stale_properties")
def refresh_stale_locations_task(org_id: int = 1, force: bool = False, batch_size: int | None = None):
    db = SessionLocal()
    try:
        payload = build_location_refresh_payload(force=force, batch_size=batch_size)
        rows = list_properties_needing_location_refresh(
            db,
            org_id=int(org_id),
            batch_size=int(payload["batch_size"]),
        )
        refreshed = 0
        property_ids: list[int] = []

        for row in rows:
            property_id = int(row.id)
            owner = _task_lock_owner("location_refresh")
            lock_key = _location_refresh_lock_key(int(org_id), property_id)

            lock = acquire_lock(
                db,
                org_id=int(org_id),
                lock_key=lock_key,
                owner=owner,
                ttl_seconds=_task_lock_ttl_seconds(),
            )
            if not lock.acquired:
                continue

            try:
                result = enrich_property_geo(
                    db,
                    org_id=int(org_id),
                    property_id=property_id,
                    force=bool(payload["force"]),
                )
                if bool((result or {}).get("updated")):
                    refreshed += 1
                property_ids.append(property_id)
                db.commit()
            finally:
                try:
                    release_lock(
                        db,
                        org_id=int(org_id),
                        lock_key=lock_key,
                        owner=owner,
                        force=False,
                    )
                    db.commit()
                except Exception:
                    db.rollback()

        return _finalize_json_result(
            ok=True,
            base={
                "org_id": int(org_id),
                "refreshed": refreshed,
                "property_ids": property_ids,
            },
        )
    finally:
        db.close()


@celery_app.task(name="jurisdiction.refresh_stale_profiles")
def refresh_stale_jurisdictions_task(org_id: int = 1, stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS):
    db = SessionLocal()
    try:
        payload = build_jurisdiction_refresh_payload(stale_days=stale_days)
        rows = list_jurisdictions_needing_refresh(
            db,
            org_id=int(org_id),
            stale_days=int(payload["stale_days"]),
        )
        refreshed = 0
        results: list[dict[str, Any]] = []

        for row in rows:
            jurisdiction_code = str(row.jurisdiction_code)
            owner = _task_lock_owner("jurisdiction_refresh")
            lock_key = _jurisdiction_refresh_lock_key(int(org_id), jurisdiction_code)

            lock = acquire_lock(
                db,
                org_id=int(org_id),
                lock_key=lock_key,
                owner=owner,
                ttl_seconds=_task_lock_ttl_seconds(),
            )
            if not lock.acquired:
                continue

            try:
                result = refresh_jurisdiction_profile(
                    db,
                    org_id=int(org_id),
                    jurisdiction_code=jurisdiction_code,
                )
                results.append(_json_safe(result))
                if bool((result or {}).get("ok")):
                    refreshed += 1
                db.commit()
            finally:
                try:
                    release_lock(
                        db,
                        org_id=int(org_id),
                        lock_key=lock_key,
                        owner=owner,
                        force=False,
                    )
                    db.commit()
                except Exception:
                    db.rollback()

        return _finalize_json_result(
            ok=True,
            base={"org_id": int(org_id), "refreshed": refreshed, "results": results},
        )
    finally:
        db.close()


@celery_app.task(name="jurisdiction.notify_stale_profiles")
def notify_stale_jurisdictions_task(org_id: int = 1, stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS):
    db = SessionLocal()
    try:
        result = notify_stale_jurisdictions(
            db,
            org_id=int(org_id),
            stale_days=int(stale_days),
        )
        return _finalize_json_result(ok=True, base={"org_id": int(org_id)}, result=result)
    finally:
        db.close()


@celery_app.task(name="ingestion.backfill_missing_enrichment_for_city")
def backfill_missing_enrichment_for_city_task(
    org_id: int = 1,
    *,
    city: str,
    state: str = "MI",
    zip_codes: list[str] | None = None,
    limit: int | None = None,
):
    db = SessionLocal()
    owner = _task_lock_owner("backfill_city")
    lock_key = ""
    try:
        effective_limit = int(limit or 500)
        zips = [str(z).strip() for z in (zip_codes or []) if str(z).strip()]

        lock_key = _city_backfill_lock_key(
            int(org_id),
            str(city),
            str(state),
            zips,
            effective_limit,
        )
        lock = acquire_lock(
            db,
            org_id=int(org_id),
            lock_key=lock_key,
            owner=owner,
            ttl_seconds=_backfill_enqueue_lock_ttl_seconds(),
        )
        if not lock.acquired:
            return _finalize_json_result(
                ok=True,
                base={
                    "org_id": int(org_id),
                    "city": city,
                    "state": state,
                    "queued": 0,
                    "property_ids": [],
                    "skipped": True,
                    "reason": "city_backfill_enqueue_lock_not_acquired",
                },
            )

        if zips:
            rows = list(
                db.execute(
                    text(
                        """
                        SELECT id
                        FROM properties
                        WHERE org_id = :org_id
                          AND lower(city) = lower(:city)
                          AND upper(state) = upper(:state)
                          AND zip = ANY(:zip_codes)
                          AND (
                                COALESCE(completeness_geo_status, 'missing') = 'missing'
                             OR COALESCE(completeness_rent_status, 'missing') = 'missing'
                             OR COALESCE(completeness_risk_status, 'missing') = 'missing'
                             OR COALESCE(completeness_jurisdiction_status, 'missing') = 'missing'
                             OR COALESCE(completeness_cashflow_status, 'missing') = 'missing'
                          )
                        ORDER BY id
                        LIMIT :limit
                        """
                    ),
                    {
                        "org_id": int(org_id),
                        "city": city,
                        "state": state,
                        "zip_codes": zips,
                        "limit": effective_limit,
                    },
                ).fetchall()
            )
        else:
            rows = list(
                db.execute(
                    text(
                        """
                        SELECT id
                        FROM properties
                        WHERE org_id = :org_id
                          AND lower(city) = lower(:city)
                          AND upper(state) = upper(:state)
                          AND (
                                COALESCE(completeness_geo_status, 'missing') = 'missing'
                             OR COALESCE(completeness_rent_status, 'missing') = 'missing'
                             OR COALESCE(completeness_risk_status, 'missing') = 'missing'
                             OR COALESCE(completeness_jurisdiction_status, 'missing') = 'missing'
                             OR COALESCE(completeness_cashflow_status, 'missing') = 'missing'
                          )
                        ORDER BY id
                        LIMIT :limit
                        """
                    ),
                    {
                        "org_id": int(org_id),
                        "city": city,
                        "state": state,
                        "limit": effective_limit,
                    },
                ).fetchall()
            )

        queued = 0
        property_ids: list[int] = []

        for row in rows:
            property_id = int(row[0] if not hasattr(row, "id") else row.id)
            if _enqueue_enrichment_if_needed(
                db=db,
                org_id=int(org_id),
                property_id=property_id,
                source_id=None,
                run_id=None,
            ):
                queued += 1
                property_ids.append(property_id)

        db.commit()

        return _finalize_json_result(
            ok=True,
            base={
                "org_id": int(org_id),
                "city": city,
                "state": state,
                "queued": queued,
                "property_ids": property_ids,
            },
        )
    finally:
        if lock_key:
            try:
                release_lock(
                    db,
                    org_id=int(org_id),
                    lock_key=lock_key,
                    owner=owner,
                    force=False,
                )
                db.commit()
            except Exception:
                db.rollback()
        db.close()


@celery_app.task(name="ingestion.backfill_missing_enrichment_for_jurisdiction")
def backfill_missing_enrichment_for_jurisdiction_task(
    org_id: int = 1,
    *,
    jurisdiction_code: str,
    limit: int | None = None,
):
    db = SessionLocal()
    owner = _task_lock_owner("backfill_jurisdiction")
    lock_key = ""
    try:
        effective_limit = int(limit or 500)
        code = str(jurisdiction_code).strip()

        lock_key = _jurisdiction_backfill_lock_key(int(org_id), code, effective_limit)
        lock = acquire_lock(
            db,
            org_id=int(org_id),
            lock_key=lock_key,
            owner=owner,
            ttl_seconds=_backfill_enqueue_lock_ttl_seconds(),
        )
        if not lock.acquired:
            return _finalize_json_result(
                ok=True,
                base={
                    "org_id": int(org_id),
                    "jurisdiction_code": code,
                    "queued": 0,
                    "property_ids": [],
                    "skipped": True,
                    "reason": "jurisdiction_backfill_enqueue_lock_not_acquired",
                },
            )

        rows = list(
            db.execute(
                text(
                    """
                    SELECT id
                    FROM properties
                    WHERE org_id = :org_id
                      AND lower(COALESCE(jurisdiction_code, '')) = lower(:jurisdiction_code)
                      AND (
                            COALESCE(completeness_geo_status, 'missing') = 'missing'
                         OR COALESCE(completeness_rent_status, 'missing') = 'missing'
                         OR COALESCE(completeness_risk_status, 'missing') = 'missing'
                         OR COALESCE(completeness_jurisdiction_status, 'missing') = 'missing'
                         OR COALESCE(completeness_cashflow_status, 'missing') = 'missing'
                      )
                    ORDER BY id
                    LIMIT :limit
                    """
                ),
                {
                    "org_id": int(org_id),
                    "jurisdiction_code": code,
                    "limit": effective_limit,
                },
            ).fetchall()
        )

        queued = 0
        property_ids: list[int] = []

        for row in rows:
            property_id = int(row[0] if not hasattr(row, "id") else row.id)
            if _enqueue_enrichment_if_needed(
                db=db,
                org_id=int(org_id),
                property_id=property_id,
                source_id=None,
                run_id=None,
            ):
                queued += 1
                property_ids.append(property_id)

        db.commit()

        return _finalize_json_result(
            ok=True,
            base={
                "org_id": int(org_id),
                "jurisdiction_code": code,
                "queued": queued,
                "property_ids": property_ids,
            },
        )
    finally:
        if lock_key:
            try:
                release_lock(
                    db,
                    org_id=int(org_id),
                    lock_key=lock_key,
                    owner=owner,
                    force=False,
                )
                db.commit()
            except Exception:
                db.rollback()
        db.close()
