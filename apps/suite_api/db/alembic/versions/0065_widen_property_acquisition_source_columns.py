"""widen property acquisition source columns

Revision ID: 0065_widen_property_acquisition_source_columns
Revises: 0064_fix_org_locks_schema
Create Date: 2026-03-27 13:15:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "0065_widen_property_acquisition_source_columns"
down_revision = "0064_fix_org_locks_schema"
branch_labels = None
depends_on = None


def _has_table(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _columns_by_name(inspector, table_name: str) -> dict[str, dict]:
    return {col["name"]: col for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not _has_table(inspector, "properties"):
        return

    cols = _columns_by_name(inspector, "properties")

    if "acquisition_source_provider" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_provider",
            existing_type=sa.String(length=40),
            type_=sa.String(length=80),
            existing_nullable=True,
        )

    if "acquisition_source_slug" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_slug",
            existing_type=sa.String(length=40),
            type_=sa.String(length=255),
            existing_nullable=True,
        )

    if "acquisition_source_record_id" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_record_id",
            existing_type=sa.String(length=40),
            type_=sa.String(length=255),
            existing_nullable=True,
        )

    if "acquisition_source_url" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_url",
            existing_type=sa.String(length=40),
            type_=sa.String(length=1024),
            existing_nullable=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not _has_table(inspector, "properties"):
        return

    cols = _columns_by_name(inspector, "properties")

    if "acquisition_source_url" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_url",
            existing_type=sa.String(length=1024),
            type_=sa.String(length=40),
            existing_nullable=True,
        )

    if "acquisition_source_record_id" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_record_id",
            existing_type=sa.String(length=255),
            type_=sa.String(length=40),
            existing_nullable=True,
        )

    if "acquisition_source_slug" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_slug",
            existing_type=sa.String(length=255),
            type_=sa.String(length=40),
            existing_nullable=True,
        )

    if "acquisition_source_provider" in cols:
        op.alter_column(
            "properties",
            "acquisition_source_provider",
            existing_type=sa.String(length=80),
            type_=sa.String(length=40),
            existing_nullable=True,
        )