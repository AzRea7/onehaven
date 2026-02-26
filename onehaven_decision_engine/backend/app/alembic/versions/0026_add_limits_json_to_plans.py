"""add limits_json to plans

Revision ID: 0026_add_limits_json_to_plans
Revises: 0025_saas_core_tables
Create Date: 2026-02-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0026_add_limits_json_to_plans"
down_revision = "0025_saas_core_tables"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, column: str) -> bool:
    cols = [c["name"] for c in _insp().get_columns(table)]
    return column in cols


def upgrade() -> None:
    # Add the missing column expected by app.models.Plan
    if _has_table("plans") and not _has_column("plans", "limits_json"):
        op.add_column(
            "plans",
            sa.Column("limits_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        # Clean default so inserts can supply explicit values (optional hygiene)
        try:
            op.alter_column("plans", "limits_json", server_default=None, existing_type=sa.Text(), existing_nullable=False)
        except Exception:
            pass


def downgrade() -> None:
    if _has_table("plans") and _has_column("plans", "limits_json"):
        op.drop_column("plans", "limits_json")