"""add updated_at to properties

Revision ID: 0048_add_properties_updated_at
Revises: 0047_property_state_stage_index_and_transition_ts
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0048_add_properties_updated_at"
down_revision = "0047_prop_stage_idx_ts"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _cols(table: str) -> set[str]:
    if not _has_table(table):
        return set()
    return {c["name"] for c in _insp().get_columns(table)}


def upgrade() -> None:
    if not _has_table("properties"):
        return

    cols = _cols("properties")

    if "updated_at" not in cols:
        op.add_column(
            "properties",
            sa.Column(
                "updated_at",
                sa.DateTime(),
                nullable=True,
            ),
        )

        op.execute(
            """
            UPDATE properties
            SET updated_at = created_at
            WHERE updated_at IS NULL
            """
        )

        op.alter_column(
            "properties",
            "updated_at",
            nullable=False,
        )


def downgrade() -> None:
    if not _has_table("properties"):
        return

    cols = _cols("properties")
    if "updated_at" in cols:
        op.drop_column("properties", "updated_at")