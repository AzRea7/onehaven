# backend/app/alembic/versions/0072_add_inspection_appointment_fields_and_reminders.py
"""add inspection appointment fields and reminders

Revision ID: 0072_add_inspection_appointment_fields_and_reminders
Revises: 0071_add_property_scoped_inspection_execution_fields
Create Date: 2026-04-06 00:30:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0072_add_inspection_appointment_fields_and_reminders"
down_revision = "0071_add_property_scoped_inspection_execution_fields"
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
    _add_col("inspections", sa.Column("scheduled_for", sa.DateTime(), nullable=True))
    _add_col("inspections", sa.Column("inspector_name", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("inspector_company", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("inspector_email", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("inspector_phone", sa.String(length=120), nullable=True))
    _add_col("inspections", sa.Column("calendar_event_id", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("reminder_offsets_json", sa.Text(), nullable=True))
    _add_col("inspections", sa.Column("appointment_status", sa.String(length=64), nullable=True))
    _add_col("inspections", sa.Column("appointment_notes", sa.Text(), nullable=True))
    _add_col("inspections", sa.Column("last_reminder_sent_at", sa.DateTime(), nullable=True))
    _add_col("inspections", sa.Column("next_reminder_due_at", sa.DateTime(), nullable=True))
    _add_col("inspections", sa.Column("ics_uid", sa.String(length=255), nullable=True))
    _add_col("inspections", sa.Column("ics_text", sa.Text(), nullable=True))


def downgrade() -> None:
    for col in [
        "ics_text",
        "ics_uid",
        "next_reminder_due_at",
        "last_reminder_sent_at",
        "appointment_notes",
        "appointment_status",
        "reminder_offsets_json",
        "calendar_event_id",
        "inspector_phone",
        "inspector_email",
        "inspector_company",
        "inspector_name",
        "scheduled_for",
    ]:
        _drop_col("inspections", col)
