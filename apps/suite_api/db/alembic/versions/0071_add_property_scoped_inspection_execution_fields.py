# backend/app/alembic/versions/0071_add_property_scoped_inspection_execution_fields.py
"""add property scoped inspection execution fields

Revision ID: 0071_add_property_scoped_inspection_execution_fields
Revises: 0070_repair_acquisition_contacts_columns
Create Date: 2026-04-06 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0071_add_property_scoped_inspection_execution_fields"
down_revision = "0070_repair_acquisition_contacts_columns"
branch_labels = None
depends_on = None


def _col_exists(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        cols = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _add_col(table_name: str, column: sa.Column) -> None:
    if not _col_exists(table_name, str(column.name)):
        op.add_column(table_name, column)


def _drop_col(table_name: str, column_name: str) -> None:
    if _col_exists(table_name, column_name):
        op.drop_column(table_name, column_name)


def upgrade() -> None:
    _add_col("inspections", sa.Column("inspector", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("jurisdiction", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("template_key", sa.String(length=120), nullable=True))
    _add_col("inspections", sa.Column("template_version", sa.String(length=120), nullable=True))
    _add_col("inspections", sa.Column("inspection_status", sa.String(length=64), nullable=True))
    _add_col("inspections", sa.Column("result_status", sa.String(length=64), nullable=True))
    _add_col("inspections", sa.Column("readiness_score", sa.Float(), nullable=True))
    _add_col("inspections", sa.Column("readiness_status", sa.String(length=64), nullable=True))
    _add_col("inspections", sa.Column("total_items", sa.Integer(), nullable=True))
    _add_col("inspections", sa.Column("passed_items", sa.Integer(), nullable=True))
    _add_col("inspections", sa.Column("failed_items", sa.Integer(), nullable=True))
    _add_col("inspections", sa.Column("blocked_items", sa.Integer(), nullable=True))
    _add_col("inspections", sa.Column("na_items", sa.Integer(), nullable=True))
    _add_col("inspections", sa.Column("failed_critical_items", sa.Integer(), nullable=True))
    _add_col("inspections", sa.Column("submitted_at", sa.DateTime(), nullable=True))
    _add_col("inspections", sa.Column("completed_at", sa.DateTime(), nullable=True))
    _add_col("inspections", sa.Column("last_scored_at", sa.DateTime(), nullable=True))
    _add_col("inspections", sa.Column("evidence_summary_json", sa.Text(), nullable=True))

    _add_col("inspection_items", sa.Column("category", sa.String(length=120), nullable=True))
    _add_col("inspection_items", sa.Column("result_status", sa.String(length=64), nullable=True))
    _add_col("inspection_items", sa.Column("fail_reason", sa.Text(), nullable=True))
    _add_col("inspection_items", sa.Column("remediation_guidance", sa.Text(), nullable=True))
    _add_col("inspection_items", sa.Column("evidence_json", sa.Text(), nullable=True))
    _add_col("inspection_items", sa.Column("photo_references_json", sa.Text(), nullable=True))
    _add_col("inspection_items", sa.Column("standard_label", sa.String(length=255), nullable=True))
    _add_col("inspection_items", sa.Column("standard_citation", sa.String(length=255), nullable=True))
    _add_col("inspection_items", sa.Column("readiness_impact", sa.Float(), nullable=True))
    _add_col("inspection_items", sa.Column("requires_reinspection", sa.Boolean(), nullable=True))
    _add_col("inspection_items", sa.Column("updated_at", sa.DateTime(), nullable=True))

    _add_col("property_checklist_items", sa.Column("result_status", sa.String(length=64), nullable=True))
    _add_col("property_checklist_items", sa.Column("fail_reason", sa.Text(), nullable=True))
    _add_col("property_checklist_items", sa.Column("remediation_guidance", sa.Text(), nullable=True))
    _add_col("property_checklist_items", sa.Column("evidence_json", sa.Text(), nullable=True))
    _add_col("property_checklist_items", sa.Column("photo_references_json", sa.Text(), nullable=True))
    _add_col("property_checklist_items", sa.Column("latest_inspection_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    _drop_col("property_checklist_items", "latest_inspection_id")
    _drop_col("property_checklist_items", "photo_references_json")
    _drop_col("property_checklist_items", "evidence_json")
    _drop_col("property_checklist_items", "remediation_guidance")
    _drop_col("property_checklist_items", "fail_reason")
    _drop_col("property_checklist_items", "result_status")

    _drop_col("inspection_items", "updated_at")
    _drop_col("inspection_items", "requires_reinspection")
    _drop_col("inspection_items", "readiness_impact")
    _drop_col("inspection_items", "standard_citation")
    _drop_col("inspection_items", "standard_label")
    _drop_col("inspection_items", "photo_references_json")
    _drop_col("inspection_items", "evidence_json")
    _drop_col("inspection_items", "remediation_guidance")
    _drop_col("inspection_items", "fail_reason")
    _drop_col("inspection_items", "result_status")
    _drop_col("inspection_items", "category")

    _drop_col("inspections", "evidence_summary_json")
    _drop_col("inspections", "last_scored_at")
    _drop_col("inspections", "completed_at")
    _drop_col("inspections", "submitted_at")
    _drop_col("inspections", "failed_critical_items")
    _drop_col("inspections", "na_items")
    _drop_col("inspections", "blocked_items")
    _drop_col("inspections", "failed_items")
    _drop_col("inspections", "passed_items")
    _drop_col("inspections", "total_items")
    _drop_col("inspections", "readiness_status")
    _drop_col("inspections", "readiness_score")
    _drop_col("inspections", "result_status")
    _drop_col("inspections", "inspection_status")
    _drop_col("inspections", "template_version")
    _drop_col("inspections", "template_key")
    _drop_col("inspections", "jurisdiction")
    _drop_col("inspections", "inspector")