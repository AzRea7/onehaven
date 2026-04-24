"""fix jurisdiction json default casts for drifted text/json columns

Revision ID: 0085_fix_jurisdiction_json_default_casts
Revises: 0084_repair_remaining_jurisdiction_schema_drift
Create Date: 2026-04-16
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0085_fix_jurisdiction_json_default_casts"
down_revision = "0084_repair_remaining_jurisdiction_schema_drift"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _table_exists(table_name: str) -> bool:
    try:
        return table_name in _inspector().get_table_names()
    except Exception:
        return False


def _column_info(table_name: str, column_name: str) -> dict | None:
    if not _table_exists(table_name):
        return None
    try:
        cols = _inspector().get_columns(table_name)
    except Exception:
        return None
    for col in cols:
        if str(col.get("name")) == column_name:
            return col
    return None


def _column_exists(table_name: str, column_name: str) -> bool:
    return _column_info(table_name, column_name) is not None


def _column_type_name(table_name: str, column_name: str) -> str:
    col = _column_info(table_name, column_name)
    if not col:
        return ""
    typ = col.get("type")
    if typ is None:
        return ""
    return str(typ).lower()


def _is_json_like_type(table_name: str, column_name: str) -> bool:
    t = _column_type_name(table_name, column_name)
    return "json" in t


def _set_null_default(table_name: str, column_name: str, json_kind: str) -> None:
    """
    json_kind: 'array' or 'object'
    Works whether the live DB column is TEXT, JSON, or JSONB.
    """
    if not _column_exists(table_name, column_name):
        return

    if json_kind == "array":
        text_default = "[]"
        json_expr = "'[]'::jsonb"
        text_expr = "'[]'"
    else:
        text_default = "{}"
        json_expr = "'{}'::jsonb"
        text_expr = "'{}'"

    if _is_json_like_type(table_name, column_name):
        op.execute(
            sa.text(
                f"""
                UPDATE {table_name}
                SET {column_name} = COALESCE({column_name}, {json_expr})
                """
            )
        )
    else:
        op.execute(
            sa.text(
                f"""
                UPDATE {table_name}
                SET {column_name} = COALESCE({column_name}, {text_expr})
                """
            )
        )


def _set_scalar_default(table_name: str, column_name: str, sql_default: str) -> None:
    if not _column_exists(table_name, column_name):
        return
    op.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET {column_name} = COALESCE({column_name}, {sql_default})
            """
        )
    )


def upgrade() -> None:
    # jurisdiction_profiles scalar defaults
    _set_scalar_default("jurisdiction_profiles", "completeness_status", "'missing'")
    _set_scalar_default("jurisdiction_profiles", "completeness_score", "0.0")
    _set_scalar_default("jurisdiction_profiles", "confidence_score", "0.0")
    _set_scalar_default("jurisdiction_profiles", "is_stale", "FALSE")
    _set_scalar_default("jurisdiction_profiles", "source_count", "0")
    _set_scalar_default("jurisdiction_profiles", "authoritative_source_count", "0")
    _set_scalar_default("jurisdiction_profiles", "refresh_retry_count", "0")

    # jurisdiction_profiles array-like json/text columns
    for col in [
        "missing_categories_json",
        "stale_categories_json",
        "inferred_categories_json",
        "conflicting_categories_json",
        "required_categories_json",
        "covered_categories_json",
        "unresolved_items_json",
        "unmet_categories_json",
        "undiscovered_categories_json",
        "weak_support_categories_json",
        "authority_unmet_categories_json",
    ]:
        _set_null_default("jurisdiction_profiles", col, "array")

    # jurisdiction_profiles object-like json/text columns
    for col in [
        "completeness_snapshot_json",
        "expected_rule_universe_json",
        "category_coverage_details_json",
        "category_unmet_reasons_json",
        "last_refresh_outcome_json",
        "refresh_requirements_json",
        "source_freshness_json",
        "discovery_metadata_json",
        "metadata_json",
    ]:
        _set_null_default("jurisdiction_profiles", col, "object")

    # jurisdiction_coverage_status scalar defaults
    _set_scalar_default("jurisdiction_coverage_status", "completeness_status", "'missing'")
    _set_scalar_default("jurisdiction_coverage_status", "completeness_score", "0.0")
    _set_scalar_default("jurisdiction_coverage_status", "confidence_score", "0.0")
    _set_scalar_default("jurisdiction_coverage_status", "authority_score", "0.0")
    _set_scalar_default("jurisdiction_coverage_status", "extraction_confidence", "0.0")
    _set_scalar_default("jurisdiction_coverage_status", "conflict_count", "0")
    _set_scalar_default("jurisdiction_coverage_status", "is_stale", "FALSE")

    # jurisdiction_coverage_status array-like json/text columns
    for col in [
        "covered_categories_json",
        "missing_categories_json",
        "stale_categories_json",
        "inferred_categories_json",
        "conflicting_categories_json",
        "required_categories_json",
        "unmet_categories_json",
        "undiscovered_categories_json",
        "weak_support_categories_json",
        "authority_unmet_categories_json",
        "source_ids_json",
    ]:
        _set_null_default("jurisdiction_coverage_status", col, "array")

    # jurisdiction_coverage_status object-like json/text columns
    for col in [
        "category_coverage_snapshot_json",
        "category_last_verified_json",
        "category_source_backing_json",
        "completeness_snapshot_json",
        "expected_rule_universe_json",
        "category_coverage_details_json",
        "category_unmet_reasons_json",
        "source_summary_json",
        "discovery_metadata_json",
        "metadata_json",
    ]:
        _set_null_default("jurisdiction_coverage_status", col, "object")


def downgrade() -> None:
    # No-op: this migration only backfills nulls safely based on live column types.
    pass