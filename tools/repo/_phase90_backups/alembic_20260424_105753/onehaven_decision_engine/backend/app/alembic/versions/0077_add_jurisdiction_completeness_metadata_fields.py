# backend/app/alembic/versions/0077_add_jurisdiction_completeness_metadata_fields.py
"""add jurisdiction completeness metadata fields

Revision ID: 0077_add_jurisdiction_completeness_metadata_fields
Revises: 0076_add_property_evidence_proof_fields
Create Date: 2026-04-12
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0077_add_jurisdiction_completeness_metadata_fields"
down_revision = "0076_add_property_evidence_proof_fields"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        return table_name in inspector.get_table_names()
    except Exception:
        return False


def _column_exists(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        cols = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _index_exists(index_name: str, table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        indexes = inspector.get_indexes(table_name)
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
    if _table_exists("policy_sources"):
        _add_column(
            "policy_sources",
            sa.Column("source_origin", sa.String(length=40), nullable=False, server_default=sa.text("'curated'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("authority_score", sa.Float(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "policy_sources",
            sa.Column("category_hints_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("discovery_metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("last_discovery_run_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("next_discovery_due_at", sa.DateTime(), nullable=True),
        )
        _create_index("ix_policy_sources_source_origin", "policy_sources", ["source_origin"])
        _create_index("ix_policy_sources_next_discovery_due_at", "policy_sources", ["next_discovery_due_at"])

    if _table_exists("policy_assertions"):
        _add_column(
            "policy_assertions",
            sa.Column("extraction_confidence", sa.Float(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("authority_score", sa.Float(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("conflict_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _create_index("ix_policy_assertions_conflict_count", "policy_assertions", ["conflict_count"])

    if _table_exists("jurisdiction_profiles"):
        for col in [
            sa.Column("stale_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("inferred_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("conflicting_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("required_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("covered_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("completeness_snapshot_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("expected_rule_universe_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("category_coverage_details_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("category_unmet_reasons_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("unmet_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("undiscovered_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("weak_support_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("authority_unmet_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("category_norm_version", sa.String(length=40), nullable=True),
            sa.Column("source_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("authoritative_source_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("freshest_source_at", sa.DateTime(), nullable=True),
            sa.Column("oldest_source_at", sa.DateTime(), nullable=True),
            sa.Column("discovery_status", sa.String(length=40), nullable=False, server_default=sa.text("'not_started'")),
            sa.Column("last_discovery_run_id", sa.String(length=120), nullable=True),
            sa.Column("last_discovered_at", sa.DateTime(), nullable=True),
            sa.Column("next_discovery_due_at", sa.DateTime(), nullable=True),
            sa.Column("last_refresh_attempt_at", sa.DateTime(), nullable=True),
            sa.Column("source_freshness_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("discovery_metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        ]:
            _add_column("jurisdiction_profiles", col)
        _create_index("ix_jp_discovery_status", "jurisdiction_profiles", ["discovery_status"])
        _create_index("ix_jp_next_discovery_due_at", "jurisdiction_profiles", ["next_discovery_due_at"])

    if _table_exists("jurisdiction_coverage_status"):
        for col in [
            sa.Column("stale_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("inferred_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("conflicting_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("required_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("category_coverage_snapshot_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("category_last_verified_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("category_source_backing_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("completeness_snapshot_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("expected_rule_universe_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("category_coverage_details_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("category_unmet_reasons_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("unmet_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("undiscovered_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("weak_support_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("authority_unmet_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("authority_score", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("extraction_confidence", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("conflict_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("production_readiness", sa.String(length=40), nullable=True),
            sa.Column("discovery_status", sa.String(length=40), nullable=False, server_default=sa.text("'not_started'")),
            sa.Column("last_discovery_run_id", sa.String(length=120), nullable=True),
            sa.Column("last_discovery_run_at", sa.DateTime(), nullable=True),
            sa.Column("next_discovery_due_at", sa.DateTime(), nullable=True),
            sa.Column("discovery_metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        ]:
            _add_column("jurisdiction_coverage_status", col)
        _create_index("ix_jurisdiction_coverage_status_discovery_status", "jurisdiction_coverage_status", ["discovery_status"])
        _create_index("ix_jurisdiction_coverage_status_next_discovery_due_at", "jurisdiction_coverage_status", ["next_discovery_due_at"])


def downgrade() -> None:
    _drop_index("ix_jurisdiction_coverage_status_next_discovery_due_at", "jurisdiction_coverage_status")
    _drop_index("ix_jurisdiction_coverage_status_discovery_status", "jurisdiction_coverage_status")
    for name in [
        "discovery_metadata_json",
        "next_discovery_due_at",
        "last_discovery_run_at",
        "last_discovery_run_id",
        "discovery_status",
        "production_readiness",
        "conflict_count",
        "extraction_confidence",
        "authority_score",
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
    ]:
        _drop_column("jurisdiction_coverage_status", name)

    _drop_index("ix_jp_next_discovery_due_at", "jurisdiction_profiles")
    _drop_index("ix_jp_discovery_status", "jurisdiction_profiles")
    for name in [
        "discovery_metadata_json",
        "source_freshness_json",
        "last_refresh_attempt_at",
        "next_discovery_due_at",
        "last_discovered_at",
        "last_discovery_run_id",
        "discovery_status",
        "oldest_source_at",
        "freshest_source_at",
        "authoritative_source_count",
        "source_count",
        "category_norm_version",
        "authority_unmet_categories_json",
        "weak_support_categories_json",
        "undiscovered_categories_json",
        "unmet_categories_json",
        "category_unmet_reasons_json",
        "category_coverage_details_json",
        "expected_rule_universe_json",
        "completeness_snapshot_json",
        "covered_categories_json",
        "required_categories_json",
        "conflicting_categories_json",
        "inferred_categories_json",
        "stale_categories_json",
    ]:
        _drop_column("jurisdiction_profiles", name)

    _drop_index("ix_policy_assertions_conflict_count", "policy_assertions")
    for name in ["conflict_count", "authority_score", "extraction_confidence"]:
        _drop_column("policy_assertions", name)

    _drop_index("ix_policy_sources_next_discovery_due_at", "policy_sources")
    _drop_index("ix_policy_sources_source_origin", "policy_sources")
    for name in [
        "next_discovery_due_at",
        "last_discovery_run_at",
        "discovery_metadata_json",
        "category_hints_json",
        "authority_score",
        "source_origin",
    ]:
        _drop_column("policy_sources", name)