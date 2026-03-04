# onehaven_decision_engine/backend/app/alembic/versions/0039_safe_add_org_updated_at.py
"""
0039 safe add updated_at to organizations (add if missing)

Revision ID: 0039_safe_add_org_updated_at
Revises: 0038_safe_repair_app_users_columns
Create Date: 2026-03-04
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0039_safe_add_org_updated_at"
down_revision = "0038_safe_repair_app_users_columns"
branch_labels = None
depends_on = None


def _col_exists(table: str, col: str) -> bool:
    bind = op.get_bind()
    q = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
        """
    )
    return bind.execute(q, {"t": table, "c": col}).first() is not None


def upgrade() -> None:
    # Add updated_at only if missing.
    if not _col_exists("organizations", "updated_at"):
        op.add_column(
            "organizations",
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )

        # Optional: remove server default after backfill so app can manage it
        with op.batch_alter_table("organizations") as batch:
            batch.alter_column("updated_at", server_default=None)


def downgrade() -> None:
    # Conservative: don't drop; it's safe to keep.
    pass