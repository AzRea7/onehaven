"""
0056 add real inspection compliance foundation

Revision ID: 0056_add_real_inspection_compliance_foundation
Revises: 0055_add_jurisdiction_finalize_foundation
Create Date: 2026-03-21
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0056_add_real_inspection_compliance_foundation"
down_revision = "0055_add_jurisdiction_finalize_foundation"
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
    # hqs_rules
    # ------------------------------------------------------------------
    if _has_table("hqs_rules"):
        _add_column_if_missing(
            "hqs_rules",
            sa.Column("template_key", sa.String(length=80), nullable=False, server_default="hqs"),
        )
        _add_column_if_missing(
            "hqs_rules",
            sa.Column("template_version", sa.String(length=40), nullable=False, server_default="hqs_v1"),
        )
        _add_column_if_missing(
            "hqs_rules",
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "hqs_rules",
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )

        bind.execute(
            text(
                """
                UPDATE hqs_rules
                SET template_key = COALESCE(NULLIF(template_key, ''), 'hqs'),
                    template_version = COALESCE(NULLIF(template_version, ''), 'hqs_v1'),
                    sort_order = COALESCE(sort_order, 0),
                    is_active = COALESCE(is_active, true)
                """
            )
        )

        with op.batch_alter_table("hqs_rules") as batch:
            batch.alter_column("template_key", server_default=None)
            batch.alter_column("template_version", server_default=None)
            batch.alter_column("sort_order", server_default=None)
            batch.alter_column("is_active", server_default=None)

        _create_index_if_missing("ix_hqs_rules_template_key", "hqs_rules", ["template_key"])
        _create_index_if_missing("ix_hqs_rules_template_version", "hqs_rules", ["template_version"])
        _create_index_if_missing(
            "ix_hqs_rules_template_key_version",
            "hqs_rules",
            ["template_key", "template_version"],
        )

    # ------------------------------------------------------------------
    # hqs_addendum_rules
    # ------------------------------------------------------------------
    if _has_table("hqs_addendum_rules"):
        _add_column_if_missing(
            "hqs_addendum_rules",
            sa.Column("template_key", sa.String(length=80), nullable=False, server_default="hqs"),
        )
        _add_column_if_missing(
            "hqs_addendum_rules",
            sa.Column("template_version", sa.String(length=40), nullable=False, server_default="hqs_v1"),
        )
        _add_column_if_missing(
            "hqs_addendum_rules",
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "hqs_addendum_rules",
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )

        bind.execute(
            text(
                """
                UPDATE hqs_addendum_rules
                SET template_key = COALESCE(NULLIF(template_key, ''), 'hqs'),
                    template_version = COALESCE(NULLIF(template_version, ''), 'hqs_v1'),
                    sort_order = COALESCE(sort_order, 0),
                    is_active = COALESCE(is_active, true)
                """
            )
        )

        with op.batch_alter_table("hqs_addendum_rules") as batch:
            batch.alter_column("template_key", server_default=None)
            batch.alter_column("template_version", server_default=None)
            batch.alter_column("sort_order", server_default=None)
            batch.alter_column("is_active", server_default=None)

        _create_index_if_missing("ix_hqs_addendum_template_key", "hqs_addendum_rules", ["template_key"])
        _create_index_if_missing(
            "ix_hqs_addendum_template_version",
            "hqs_addendum_rules",
            ["template_version"],
        )
        _create_index_if_missing(
            "ix_hqs_addendum_template_key_version",
            "hqs_addendum_rules",
            ["template_key", "template_version"],
        )

    # ------------------------------------------------------------------
    # inspections
    # ------------------------------------------------------------------
    if _has_table("inspections"):
        _add_column_if_missing(
            "inspections",
            sa.Column("template_key", sa.String(length=80), nullable=False, server_default="hqs"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("template_version", sa.String(length=40), nullable=False, server_default="hqs_v1"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("inspection_status", sa.String(length=32), nullable=False, server_default="completed"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("result_status", sa.String(length=32), nullable=False, server_default="pending"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("inspection_method", sa.String(length=32), nullable=True),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("standards_version", sa.String(length=40), nullable=True),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("readiness_score", sa.Float(), nullable=False, server_default="0.0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("readiness_status", sa.String(length=32), nullable=False, server_default="unknown"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("total_items", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("passed_items", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("failed_items", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("blocked_items", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("na_items", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("failed_critical_items", sa.Integer(), nullable=False, server_default="0"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("evidence_summary_json", sa.Text(), nullable=False, server_default="{}"),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("last_scored_at", sa.DateTime(), nullable=True),
        )
        _add_column_if_missing(
            "inspections",
            sa.Column("completed_at", sa.DateTime(), nullable=True),
        )

        bind.execute(
            text(
                """
                UPDATE inspections i
                SET template_key = COALESCE(NULLIF(i.template_key, ''), 'hqs'),
                    template_version = COALESCE(NULLIF(i.template_version, ''), 'hqs_v1'),
                    inspection_status = COALESCE(NULLIF(i.inspection_status, ''), 'completed'),
                    result_status = CASE
                        WHEN COALESCE(i.passed, false) THEN 'pass'
                        ELSE 'fail'
                    END,
                    readiness_score = CASE
                        WHEN items.total_items > 0
                            THEN ROUND((items.passed_items::numeric / items.total_items::numeric) * 100.0, 2)
                        WHEN COALESCE(i.passed, false)
                            THEN 100.0
                        ELSE 0.0
                    END,
                    readiness_status = CASE
                        WHEN items.total_items = 0 AND COALESCE(i.passed, false) THEN 'ready'
                        WHEN items.total_items = 0 AND NOT COALESCE(i.passed, false) THEN 'not_ready'
                        WHEN (items.failed_items = 0 AND items.blocked_items = 0) THEN 'ready'
                        WHEN items.failed_critical_items > 0 THEN 'critical'
                        WHEN items.failed_items > 0 THEN 'needs_work'
                        ELSE 'unknown'
                    END,
                    total_items = COALESCE(items.total_items, 0),
                    passed_items = COALESCE(items.passed_items, 0),
                    failed_items = COALESCE(items.failed_items, 0),
                    blocked_items = COALESCE(items.blocked_items, 0),
                    na_items = COALESCE(items.na_items, 0),
                    failed_critical_items = COALESCE(items.failed_critical_items, 0),
                    evidence_summary_json = COALESCE(i.evidence_summary_json, '{}'),
                    last_scored_at = COALESCE(i.last_scored_at, i.inspection_date, i.created_at),
                    completed_at = COALESCE(i.completed_at, i.inspection_date)
                FROM (
                    SELECT
                        ii.inspection_id,
                        COUNT(*)::int AS total_items,
                        SUM(CASE
                            WHEN COALESCE(ii.result_status, CASE WHEN COALESCE(ii.failed, true) THEN 'fail' ELSE 'pass' END) = 'pass'
                                THEN 1 ELSE 0
                        END)::int AS passed_items,
                        SUM(CASE
                            WHEN COALESCE(ii.result_status, CASE WHEN COALESCE(ii.failed, true) THEN 'fail' ELSE 'pass' END) = 'fail'
                                THEN 1 ELSE 0
                        END)::int AS failed_items,
                        SUM(CASE
                            WHEN COALESCE(ii.result_status, '') = 'blocked'
                                THEN 1 ELSE 0
                        END)::int AS blocked_items,
                        SUM(CASE
                            WHEN COALESCE(ii.result_status, '') = 'not_applicable'
                                THEN 1 ELSE 0
                        END)::int AS na_items,
                        SUM(CASE
                            WHEN COALESCE(ii.result_status, CASE WHEN COALESCE(ii.failed, true) THEN 'fail' ELSE 'pass' END) = 'fail'
                             AND COALESCE(ii.severity, 0) >= 4
                                THEN 1 ELSE 0
                        END)::int AS failed_critical_items
                    FROM inspection_items ii
                    GROUP BY ii.inspection_id
                ) AS items
                WHERE i.id = items.inspection_id
                """
            )
        )

        bind.execute(
            text(
                """
                UPDATE inspections
                SET template_key = COALESCE(NULLIF(template_key, ''), 'hqs'),
                    template_version = COALESCE(NULLIF(template_version, ''), 'hqs_v1'),
                    inspection_status = COALESCE(NULLIF(inspection_status, ''), 'completed'),
                    result_status = COALESCE(NULLIF(result_status, ''), CASE WHEN COALESCE(passed, false) THEN 'pass' ELSE 'fail' END),
                    readiness_score = COALESCE(readiness_score, 0.0),
                    readiness_status = COALESCE(NULLIF(readiness_status, ''), 'unknown'),
                    total_items = COALESCE(total_items, 0),
                    passed_items = COALESCE(passed_items, 0),
                    failed_items = COALESCE(failed_items, 0),
                    blocked_items = COALESCE(blocked_items, 0),
                    na_items = COALESCE(na_items, 0),
                    failed_critical_items = COALESCE(failed_critical_items, 0),
                    evidence_summary_json = COALESCE(evidence_summary_json, '{}'),
                    last_scored_at = COALESCE(last_scored_at, inspection_date, created_at),
                    completed_at = COALESCE(completed_at, inspection_date)
                """
            )
        )

        bind.execute(
            text(
                """
                UPDATE inspections
                SET readiness_status = CASE
                    WHEN total_items = 0 AND result_status = 'pass' THEN 'ready'
                    WHEN total_items = 0 AND result_status = 'fail' THEN 'not_ready'
                    WHEN failed_critical_items > 0 THEN 'critical'
                    WHEN failed_items > 0 OR blocked_items > 0 THEN 'needs_work'
                    WHEN result_status = 'pass' THEN 'ready'
                    ELSE COALESCE(NULLIF(readiness_status, ''), 'unknown')
                END
                """
            )
        )

        with op.batch_alter_table("inspections") as batch:
            for col in (
                "template_key",
                "template_version",
                "inspection_status",
                "result_status",
                "readiness_score",
                "readiness_status",
                "total_items",
                "passed_items",
                "failed_items",
                "blocked_items",
                "na_items",
                "failed_critical_items",
                "evidence_summary_json",
            ):
                batch.alter_column(col, server_default=None)

        _create_index_if_missing("ix_inspections_template_key", "inspections", ["template_key"])
        _create_index_if_missing("ix_inspections_template_version", "inspections", ["template_version"])
        _create_index_if_missing("ix_inspections_result_status", "inspections", ["result_status"])
        _create_index_if_missing("ix_inspections_readiness_status", "inspections", ["readiness_status"])
        _create_index_if_missing(
            "ix_inspections_property_template_version",
            "inspections",
            ["property_id", "template_version"],
        )

    # ------------------------------------------------------------------
    # inspection_items
    # ------------------------------------------------------------------
    if _has_table("inspection_items"):
        _add_column_if_missing(
            "inspection_items",
            sa.Column("category", sa.String(length=80), nullable=True),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("result_status", sa.String(length=32), nullable=False, server_default="pending"),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("fail_reason", sa.String(length=255), nullable=True),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("remediation_guidance", sa.Text(), nullable=True),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("evidence_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("photo_references_json", sa.Text(), nullable=False, server_default="[]"),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("standard_label", sa.String(length=255), nullable=True),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("standard_citation", sa.String(length=255), nullable=True),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("readiness_impact", sa.Float(), nullable=False, server_default="0.0"),
        )
        _add_column_if_missing(
            "inspection_items",
            sa.Column("requires_reinspection", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )

        bind.execute(
            text(
                """
                UPDATE inspection_items
                SET result_status = CASE
                        WHEN COALESCE(failed, true) THEN 'fail'
                        ELSE 'pass'
                    END,
                    evidence_json = COALESCE(evidence_json, '[]'),
                    photo_references_json = COALESCE(photo_references_json, '[]'),
                    readiness_impact = CASE
                        WHEN COALESCE(failed, true) AND COALESCE(severity, 0) >= 4 THEN 25.0
                        WHEN COALESCE(failed, true) AND COALESCE(severity, 0) = 3 THEN 15.0
                        WHEN COALESCE(failed, true) AND COALESCE(severity, 0) = 2 THEN 8.0
                        WHEN COALESCE(failed, true) AND COALESCE(severity, 0) = 1 THEN 4.0
                        ELSE 0.0
                    END,
                    requires_reinspection = COALESCE(failed, true)
                """
            )
        )

        with op.batch_alter_table("inspection_items") as batch:
            batch.alter_column("result_status", server_default=None)
            batch.alter_column("evidence_json", server_default=None)
            batch.alter_column("photo_references_json", server_default=None)
            batch.alter_column("readiness_impact", server_default=None)
            batch.alter_column("requires_reinspection", server_default=None)

        _create_index_if_missing("ix_inspection_items_result_status", "inspection_items", ["result_status"])
        _create_index_if_missing(
            "ix_inspection_items_inspection_result_status",
            "inspection_items",
            ["inspection_id", "result_status"],
        )
        _create_index_if_missing("ix_inspection_items_category", "inspection_items", ["category"])
        _create_index_if_missing("ix_inspection_items_requires_reinspection", "inspection_items", ["requires_reinspection"])


def downgrade() -> None:
    # Conservative on purpose.
    # This repo already uses drift-safe migrations, and these inspection-grade
    # fields will hold live operational compliance data once adopted.
    pass