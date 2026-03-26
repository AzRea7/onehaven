"""
0055 add jurisdiction finalize foundation fields

Revision ID: 0055_juris_finalize_fdn
Revises: 0054_add_location_automation_foundation
Create Date: 2026-03-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0055_juris_finalize_fdn"
down_revision = "0054_location_auto_fdn"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, col: str) -> bool:
    if not _has_table(table):
        return False
    return col in {c["name"] for c in _insp().get_columns(table)}


def _has_index(table: str, name: str) -> bool:
    if not _has_table(table):
        return False
    return name in {idx["name"] for idx in _insp().get_indexes(table)}


def _add_column_if_missing(table: str, column: sa.Column) -> None:
    if not _has_table(table):
        return
    if _has_column(table, column.name):
        return
    op.add_column(table, column)


def _create_index_if_missing(name: str, table: str, cols: list[str]) -> None:
    if not _has_table(table):
        return
    if _has_index(table, name):
        return
    op.create_index(name, table, cols)


def upgrade() -> None:
    bind = op.get_bind()

    # ------------------------------------------------------------------
    # jurisdiction_profiles
    # ------------------------------------------------------------------
    if _has_table("jurisdiction_profiles"):
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("completeness_score", sa.Float(), nullable=False, server_default="0.0"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("completeness_status", sa.String(length=40), nullable=False, server_default="missing"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("required_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("covered_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("missing_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("category_norm_version", sa.String(length=40), nullable=False, server_default="v1"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("last_verified_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("is_stale", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("stale_reason", sa.String(length=160), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("source_count", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("authoritative_source_count", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("freshest_source_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("oldest_source_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("source_freshness_json", sa.Text(), nullable=False, server_default="{}"),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("last_refresh_attempt_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_profiles",
            sa.Column("last_refresh_success_at", sa.DateTime(), nullable=True),
        )

        bind.execute(
            text(
                """
                UPDATE jurisdiction_profiles
                SET completeness_score = COALESCE(completeness_score, 0.0),
                    completeness_status = COALESCE(NULLIF(completeness_status, ''), 'missing'),
                    required_categories_json = COALESCE(required_categories_json, '[]'),
                    covered_categories_json = COALESCE(covered_categories_json, '[]'),
                    missing_categories_json = COALESCE(missing_categories_json, '[]'),
                    category_norm_version = COALESCE(NULLIF(category_norm_version, ''), 'v1'),
                    is_stale = COALESCE(is_stale, true),
                    source_count = COALESCE(source_count, 0),
                    authoritative_source_count = COALESCE(authoritative_source_count, 0),
                    source_freshness_json = COALESCE(source_freshness_json, '{}')
                """
            )
        )

        with op.batch_alter_table("jurisdiction_profiles") as batch:
            for col in (
                "completeness_score",
                "completeness_status",
                "required_categories_json",
                "covered_categories_json",
                "missing_categories_json",
                "category_norm_version",
                "is_stale",
                "source_count",
                "authoritative_source_count",
                "source_freshness_json",
            ):
                batch.alter_column(col, server_default=None)

        _create_index_if_missing("ix_jp_scope_lookup", "jurisdiction_profiles", ["state", "county", "city"])
        _create_index_if_missing(
            "ix_jp_completeness_status",
            "jurisdiction_profiles",
            ["completeness_status"],
        )
        _create_index_if_missing("ix_jp_is_stale", "jurisdiction_profiles", ["is_stale"])
        _create_index_if_missing(
            "ix_jp_last_verified_at",
            "jurisdiction_profiles",
            ["last_verified_at"],
        )
        _create_index_if_missing(
            "ix_jp_last_refresh_success_at",
            "jurisdiction_profiles",
            ["last_refresh_success_at"],
        )

    # ------------------------------------------------------------------
    # policy_sources
    # ------------------------------------------------------------------
    if _has_table("policy_sources"):
        _add_column_if_missing(
            "policy_sources",
            sa.Column("normalized_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "policy_sources",
            sa.Column("freshness_status", sa.String(length=40), nullable=False, server_default="unknown"),
        )
        _add_column_if_missing(
            "policy_sources",
            sa.Column("freshness_reason", sa.String(length=160), nullable=True),
        )
        _add_column_if_missing(
            "policy_sources",
            sa.Column("freshness_checked_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "policy_sources",
            sa.Column("published_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "policy_sources",
            sa.Column("effective_date", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "policy_sources",
            sa.Column("last_verified_at", sa.DateTime(), nullable=True),
        )

        bind.execute(
            text(
                """
                UPDATE policy_sources
                SET normalized_categories_json = COALESCE(normalized_categories_json, '[]'),
                    freshness_status = COALESCE(NULLIF(freshness_status, ''), 'unknown')
                """
            )
        )

        with op.batch_alter_table("policy_sources") as batch:
            batch.alter_column("normalized_categories_json", server_default=None)
            batch.alter_column("freshness_status", server_default=None)

        _create_index_if_missing(
            "ix_policy_sources_freshness_status",
            "policy_sources",
            ["freshness_status"],
        )
        _create_index_if_missing(
            "ix_policy_sources_last_verified_at",
            "policy_sources",
            ["last_verified_at"],
        )

    # ------------------------------------------------------------------
    # policy_assertions
    # ------------------------------------------------------------------
    if _has_table("policy_assertions"):
        _add_column_if_missing(
            "policy_assertions",
            sa.Column("normalized_category", sa.String(length=80), nullable=True),
        )
        _add_column_if_missing(
            "policy_assertions",
            sa.Column("coverage_status", sa.String(length=40), nullable=False, server_default="candidate"),
        )
        _add_column_if_missing(
            "policy_assertions",
            sa.Column("source_freshness_status", sa.String(length=40), nullable=True),
        )

        bind.execute(
            text(
                """
                UPDATE policy_assertions
                SET coverage_status = COALESCE(NULLIF(coverage_status, ''), 'candidate')
                """
            )
        )

        with op.batch_alter_table("policy_assertions") as batch:
            batch.alter_column("coverage_status", server_default=None)

        _create_index_if_missing(
            "ix_policy_assertions_normalized_category",
            "policy_assertions",
            ["normalized_category"],
        )
        _create_index_if_missing(
            "ix_policy_assertions_coverage_status",
            "policy_assertions",
            ["coverage_status"],
        )

    # ------------------------------------------------------------------
    # jurisdiction_coverage_status
    # ------------------------------------------------------------------
    if _has_table("jurisdiction_coverage_status"):
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("completeness_score", sa.Float(), nullable=False, server_default="0.0"),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("completeness_status", sa.String(length=40), nullable=False, server_default="missing"),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("required_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("covered_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("missing_categories_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("category_norm_version", sa.String(length=40), nullable=False, server_default="v1"),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("last_verified_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("is_stale", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("stale_reason", sa.String(length=160), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("freshest_source_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("oldest_source_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "jurisdiction_coverage_status",
            sa.Column("source_freshness_json", sa.Text(), nullable=False, server_default="{}"),
        )

        bind.execute(
            text(
                """
                UPDATE jurisdiction_coverage_status
                SET completeness_score = COALESCE(completeness_score, 0.0),
                    completeness_status = COALESCE(NULLIF(completeness_status, ''), 'missing'),
                    required_categories_json = COALESCE(required_categories_json, '[]'),
                    covered_categories_json = COALESCE(covered_categories_json, '[]'),
                    missing_categories_json = COALESCE(missing_categories_json, '[]'),
                    category_norm_version = COALESCE(NULLIF(category_norm_version, ''), 'v1'),
                    is_stale = COALESCE(is_stale, true),
                    source_freshness_json = COALESCE(source_freshness_json, '{}')
                """
            )
        )

        with op.batch_alter_table("jurisdiction_coverage_status") as batch:
            for col in (
                "completeness_score",
                "completeness_status",
                "required_categories_json",
                "covered_categories_json",
                "missing_categories_json",
                "category_norm_version",
                "is_stale",
                "source_freshness_json",
            ):
                batch.alter_column(col, server_default=None)

        _create_index_if_missing(
            "ix_jurisdiction_coverage_completeness_status",
            "jurisdiction_coverage_status",
            ["completeness_status"],
        )
        _create_index_if_missing(
            "ix_jurisdiction_coverage_is_stale",
            "jurisdiction_coverage_status",
            ["is_stale"],
        )


def downgrade() -> None:
    # Conservative on purpose. This repo already uses drift-safe migrations,
    # and dropping these columns could destroy live policy/jurisdiction data.
    pass