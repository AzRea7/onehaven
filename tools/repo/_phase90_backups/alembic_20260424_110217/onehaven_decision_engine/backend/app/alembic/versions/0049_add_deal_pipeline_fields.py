"""add deal pipeline fields

Revision ID: 0049_add_deal_pipeline_fields
Revises: 0048_add_properties_updated_at
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0049_add_deal_pipeline_fields"
down_revision = "0048_add_properties_updated_at"
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
    if not _has_table("deals"):
        return

    cols = _cols("deals")

    if "decision" not in cols:
        op.add_column(
            "deals",
            sa.Column("decision", sa.String(length=20), nullable=True),
        )

    if "purchase_price" not in cols:
        op.add_column(
            "deals",
            sa.Column("purchase_price", sa.Float(), nullable=True),
        )

    if "closing_date" not in cols:
        op.add_column(
            "deals",
            sa.Column("closing_date", sa.DateTime(), nullable=True),
        )

    if "loan_amount" not in cols:
        op.add_column(
            "deals",
            sa.Column("loan_amount", sa.Float(), nullable=True),
        )

    if "updated_at" not in cols:
        op.add_column(
            "deals",
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )

        op.execute(
            """
            UPDATE deals
            SET updated_at = created_at
            WHERE updated_at IS NULL
            """
        )

        op.alter_column("deals", "updated_at", nullable=False)


def downgrade() -> None:
    if not _has_table("deals"):
        return

    cols = _cols("deals")

    if "updated_at" in cols:
        op.drop_column("deals", "updated_at")

    if "loan_amount" in cols:
        op.drop_column("deals", "loan_amount")

    if "closing_date" in cols:
        op.drop_column("deals", "closing_date")

    if "purchase_price" in cols:
        op.drop_column("deals", "purchase_price")

    if "decision" in cols:
        op.drop_column("deals", "decision")