"""repair remaining jurisdiction schema drift after 0083

Revision ID: 0084_repair_remaining_jurisdiction_schema_drift
Revises: 0083_repair_jurisdiction_schema_drift
Create Date: 2026-04-16
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0084_repair_remaining_jurisdiction_schema_drift"
down_revision = "0083_repair_jurisdiction_schema_drift"
branch_labels = None
depends_on = None


JSONB = postgresql.JSONB(astext_type=sa.Text())


def _inspector():
    return sa.inspect(op.get_bind())


def _table_exists(table_name: str) -> bool:
    try:
        return table_name in _inspector().get_table_names()
    except Exception:
        return False


def _column_exists(table_name: str, column_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    try:
        cols = _inspector().get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _index_exists(index_name: str, table_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    try:
        indexes = _inspector().get_indexes(table_name)
    except Exception:
        return False
    return any(str(idx.get("name")) == index_name for idx in indexes)


def _add_column(table_name: str, column: sa.Column) -> None:
    if _table_exists(table_name) and not _column_exists(table_name, str(column.name)):
        op.add_column(table_name, column)


def _drop_column(table_name: str, column_name: str) -> None:
    if _table_exists(table_name) and _column_exists(table_name, column_name):
        op.drop_column(table_name, column_name)


def _create_index(index_name: str, table_name: str, columns: list[str], *, unique: bool = False) -> None:
    if _table_exists(table_name) and not _index_exists(index_name, table_name):
        op.create_index(index_name, table_name, columns, unique=unique)


def _drop_index(index_name: str, table_name: str) -> None:
    if _table_exists(table_name) and _index_exists(index_name, table_name):
        op.drop_index(index_name, table_name=table_name)


def upgrade() -> None:
    # ------------------------------------------------------------------
    # jurisdiction_profiles
    # Add remaining ORM-selected columns only.
    # Do NOT backfill JSON defaults here because drifted DBs may have TEXT
    # columns with *_json names, and COALESCE(text, jsonb) will fail.
    # ------------------------------------------------------------------
    profile_columns: list[sa.Column] = [
        sa.Column("rental_registration_frequency", sa.String(length=120), nullable=True),
        sa.Column("city_inspection_required", sa.Boolean(), nullable=True),
        sa.Column("inspection_frequency", sa.String(length=120), nullable=True),
        sa.Column("inspection_type", sa.String(length=120), nullable=True),
        sa.Column("certificate_required", sa.Boolean(), nullable=True),
        sa.Column("certificate_type", sa.String(length=120), nullable=True),
        sa.Column("lead_paint_affidavit_required", sa.Boolean(), nullable=True),
        sa.Column("local_contact_required", sa.Boolean(), nullable=True),
        sa.Column("criminal_background_policy", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),

        sa.Column("completeness_status", sa.String(length=64), nullable=True),
        sa.Column("completeness_score", sa.Float(), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column("is_stale", sa.Boolean(), nullable=True),
        sa.Column("stale_reason", sa.Text(), nullable=True),

        sa.Column("missing_categories_json", JSONB, nullable=True),
        sa.Column("stale_categories_json", JSONB, nullable=True),
        sa.Column("inferred_categories_json", JSONB, nullable=True),
        sa.Column("conflicting_categories_json", JSONB, nullable=True),
        sa.Column("required_categories_json", JSONB, nullable=True),
        sa.Column("covered_categories_json", JSONB, nullable=True),
        sa.Column("unresolved_items_json", JSONB, nullable=True),
        sa.Column("completeness_snapshot_json", JSONB, nullable=True),
        sa.Column("expected_rule_universe_json", JSONB, nullable=True),
        sa.Column("category_coverage_details_json", JSONB, nullable=True),
        sa.Column("category_unmet_reasons_json", JSONB, nullable=True),
        sa.Column("unmet_categories_json", JSONB, nullable=True),
        sa.Column("undiscovered_categories_json", JSONB, nullable=True),
        sa.Column("weak_support_categories_json", JSONB, nullable=True),
        sa.Column("authority_unmet_categories_json", JSONB, nullable=True),

        sa.Column("latest_rule_version", sa.String(length=120), nullable=True),
        sa.Column("category_norm_version", sa.String(length=120), nullable=True),
        sa.Column("source_count", sa.Integer(), nullable=True),
        sa.Column("authoritative_source_count", sa.Integer(), nullable=True),
        sa.Column("freshest_source_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("oldest_source_at", sa.DateTime(timezone=False), nullable=True),

        sa.Column("discovery_status", sa.String(length=64), nullable=True),
        sa.Column("last_discovery_run_id", sa.Integer(), nullable=True),
        sa.Column("last_discovered_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("next_discovery_due_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(timezone=False), nullable=True),

        sa.Column("last_refresh_started_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_refresh_attempt_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_refresh_success_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_refresh_error", sa.Text(), nullable=True),
        sa.Column("refresh_state", sa.String(length=64), nullable=True),
        sa.Column("refresh_status_reason", sa.Text(), nullable=True),
        sa.Column("refresh_blocked_reason", sa.Text(), nullable=True),
        sa.Column("last_refresh_state_transition_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_refresh_completed_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_refresh_outcome_json", JSONB, nullable=True),
        sa.Column("refresh_requirements_json", JSONB, nullable=True),
        sa.Column("refresh_retry_count", sa.Integer(), nullable=True),
        sa.Column("current_refresh_run_id", sa.Integer(), nullable=True),
        sa.Column("last_refresh_changed_source_count", sa.Integer(), nullable=True),
        sa.Column("last_refresh_changed_rule_count", sa.Integer(), nullable=True),
        sa.Column("source_freshness_json", JSONB, nullable=True),
        sa.Column("discovery_metadata_json", JSONB, nullable=True),
        sa.Column("metadata_json", JSONB, nullable=True),
    ]

    for col in profile_columns:
        _add_column("jurisdiction_profiles", col)

    _create_index("ix_jurisdiction_profiles_refresh_state", "jurisdiction_profiles", ["refresh_state"])
    _create_index("ix_jurisdiction_profiles_discovery_status", "jurisdiction_profiles", ["discovery_status"])
    _create_index("ix_jurisdiction_profiles_next_discovery_due_at", "jurisdiction_profiles", ["next_discovery_due_at"])

    # ------------------------------------------------------------------
    # jurisdiction_coverage_status
    # Add remaining ORM-selected columns only.
    # ------------------------------------------------------------------
    coverage_columns: list[sa.Column] = [
        sa.Column("coverage_version", sa.String(length=120), nullable=True),

        sa.Column("completeness_status", sa.String(length=64), nullable=True),
        sa.Column("completeness_score", sa.Float(), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),

        sa.Column("covered_categories_json", JSONB, nullable=True),
        sa.Column("missing_categories_json", JSONB, nullable=True),
        sa.Column("stale_categories_json", JSONB, nullable=True),
        sa.Column("inferred_categories_json", JSONB, nullable=True),
        sa.Column("conflicting_categories_json", JSONB, nullable=True),
        sa.Column("required_categories_json", JSONB, nullable=True),

        sa.Column("category_coverage_snapshot_json", JSONB, nullable=True),
        sa.Column("category_last_verified_json", JSONB, nullable=True),
        sa.Column("category_source_backing_json", JSONB, nullable=True),
        sa.Column("completeness_snapshot_json", JSONB, nullable=True),
        sa.Column("expected_rule_universe_json", JSONB, nullable=True),
        sa.Column("category_coverage_details_json", JSONB, nullable=True),
        sa.Column("category_unmet_reasons_json", JSONB, nullable=True),
        sa.Column("unmet_categories_json", JSONB, nullable=True),
        sa.Column("undiscovered_categories_json", JSONB, nullable=True),
        sa.Column("weak_support_categories_json", JSONB, nullable=True),
        sa.Column("authority_unmet_categories_json", JSONB, nullable=True),

        sa.Column("source_ids_json", JSONB, nullable=True),
        sa.Column("source_summary_json", JSONB, nullable=True),
        sa.Column("authority_score", sa.Float(), nullable=True),
        sa.Column("extraction_confidence", sa.Float(), nullable=True),
        sa.Column("conflict_count", sa.Integer(), nullable=True),
        sa.Column("production_readiness", sa.String(length=64), nullable=True),

        sa.Column("discovery_status", sa.String(length=64), nullable=True),
        sa.Column("last_discovery_run_id", sa.Integer(), nullable=True),
        sa.Column("is_stale", sa.Boolean(), nullable=True),
        sa.Column("stale_reason", sa.Text(), nullable=True),
        sa.Column("stale_since", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_computed_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_source_change_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("last_discovery_run_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("next_discovery_due_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("projection_notes", sa.Text(), nullable=True),
        sa.Column("discovery_metadata_json", JSONB, nullable=True),
        sa.Column("metadata_json", JSONB, nullable=True),
    ]

    for col in coverage_columns:
        _add_column("jurisdiction_coverage_status", col)

    _create_index(
        "ix_jurisdiction_coverage_status_discovery_status",
        "jurisdiction_coverage_status",
        ["discovery_status"],
    )
    _create_index(
        "ix_jurisdiction_coverage_status_next_discovery_due_at",
        "jurisdiction_coverage_status",
        ["next_discovery_due_at"],
    )


def downgrade() -> None:
    _drop_index("ix_jurisdiction_coverage_status_next_discovery_due_at", "jurisdiction_coverage_status")
    _drop_index("ix_jurisdiction_coverage_status_discovery_status", "jurisdiction_coverage_status")

    coverage_drop_order = [
        "metadata_json",
        "discovery_metadata_json",
        "projection_notes",
        "next_discovery_due_at",
        "last_discovery_run_at",
        "last_source_change_at",
        "last_computed_at",
        "stale_since",
        "stale_reason",
        "is_stale",
        "last_discovery_run_id",
        "discovery_status",
        "production_readiness",
        "conflict_count",
        "extraction_confidence",
        "authority_score",
        "source_summary_json",
        "source_ids_json",
        "authority_unmet_categories_json",
        "weak_support_categories_json",
        "undiscovered_categories_json",
        "unmet_categories_json",
        "category_unmet_reasons_json",
        "category_coverage_details_json",
        "expected_rule_universe_json",
        "completeness_snapshot_json",
        "category_source_backing_json",
        "category_last_verified_json",
        "category_coverage_snapshot_json",
        "required_categories_json",
        "conflicting_categories_json",
        "inferred_categories_json",
        "stale_categories_json",
        "missing_categories_json",
        "covered_categories_json",
        "confidence_score",
        "completeness_score",
        "completeness_status",
        "coverage_version",
    ]
    for col in coverage_drop_order:
        _drop_column("jurisdiction_coverage_status", col)

    _drop_index("ix_jurisdiction_profiles_next_discovery_due_at", "jurisdiction_profiles")
    _drop_index("ix_jurisdiction_profiles_discovery_status", "jurisdiction_profiles")
    _drop_index("ix_jurisdiction_profiles_refresh_state", "jurisdiction_profiles")

    profile_drop_order = [
        "metadata_json",
        "discovery_metadata_json",
        "source_freshness_json",
        "last_refresh_changed_rule_count",
        "last_refresh_changed_source_count",
        "current_refresh_run_id",
        "refresh_retry_count",
        "refresh_requirements_json",
        "last_refresh_outcome_json",
        "last_refresh_completed_at",
        "last_refresh_state_transition_at",
        "refresh_blocked_reason",
        "refresh_status_reason",
        "refresh_state",
        "last_refresh_error",
        "last_refresh_success_at",
        "last_refresh_attempt_at",
        "last_refresh_started_at",
        "last_verified_at",
        "next_discovery_due_at",
        "last_discovered_at",
        "last_discovery_run_id",
        "discovery_status",
        "oldest_source_at",
        "freshest_source_at",
        "authoritative_source_count",
        "source_count",
        "category_norm_version",
        "latest_rule_version",
        "authority_unmet_categories_json",
        "weak_support_categories_json",
        "undiscovered_categories_json",
        "unmet_categories_json",
        "category_unmet_reasons_json",
        "category_coverage_details_json",
        "expected_rule_universe_json",
        "completeness_snapshot_json",
        "unresolved_items_json",
        "covered_categories_json",
        "required_categories_json",
        "conflicting_categories_json",
        "inferred_categories_json",
        "stale_categories_json",
        "missing_categories_json",
        "stale_reason",
        "is_stale",
        "confidence_score",
        "completeness_score",
        "completeness_status",
        "source_url",
        "notes",
        "criminal_background_policy",
        "local_contact_required",
        "lead_paint_affidavit_required",
        "certificate_type",
        "certificate_required",
        "inspection_type",
        "inspection_frequency",
        "city_inspection_required",
        "rental_registration_frequency",
    ]
    for col in profile_drop_order:
        _drop_column("jurisdiction_profiles", col)