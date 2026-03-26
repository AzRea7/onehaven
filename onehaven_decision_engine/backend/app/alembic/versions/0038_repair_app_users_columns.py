# onehaven_decision_engine/backend/app/alembic/versions/0038_safe_repair_app_users_columns.py
"""
0038 safe repair app_users columns (add if missing)

Revision ID: 0038_repair_users_cols
Revises: 0037_add_org_id_to_app_users
Create Date: 2026-03-04
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0038_repair_users_cols"
down_revision = "0037_add_org_id_to_app_users"
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


def _add_column_if_missing(table: str, column: sa.Column) -> None:
    if not _col_exists(table, column.name):
        op.add_column(table, column)


def _index_exists(index_name: str) -> bool:
    bind = op.get_bind()
    q = text("SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=:i LIMIT 1")
    return bind.execute(q, {"i": index_name}).first() is not None


def upgrade() -> None:
    # Add columns only if missing.
    # Keep these aligned with backend/app/models.py AppUser.

    _add_column_if_missing("app_users", sa.Column("role", sa.String(length=20), nullable=False, server_default="user"))
    _add_column_if_missing("app_users", sa.Column("display_name", sa.String(length=160), nullable=True))
    _add_column_if_missing("app_users", sa.Column("password_hash", sa.String(length=255), nullable=True))
    _add_column_if_missing(
        "app_users", sa.Column("email_verified", sa.Boolean(), nullable=False, server_default=sa.text("false"))
    )
    _add_column_if_missing("app_users", sa.Column("last_login_at", sa.DateTime(), nullable=True))
    _add_column_if_missing(
        "app_users", sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()"))
    )
    _add_column_if_missing(
        "app_users", sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()"))
    )

    # Ensure index/uniqueness on email is present (skip if already exists).
    # Note: your table might already have a UNIQUE constraint instead of an index; this is "nice-to-have".
    if not _index_exists("ix_app_users_email"):
        try:
            op.create_index("ix_app_users_email", "app_users", ["email"], unique=True)
        except Exception:
            # If a unique constraint already exists, creating a unique index can fail.
            pass

    # Drop server defaults we added (optional) so DB doesn't keep them forever.
    # Only attempt if columns exist.
    with op.batch_alter_table("app_users") as batch:
        if _col_exists("app_users", "role"):
            batch.alter_column("role", server_default=None)
        if _col_exists("app_users", "email_verified"):
            batch.alter_column("email_verified", server_default=None)
        if _col_exists("app_users", "created_at"):
            batch.alter_column("created_at", server_default=None)
        if _col_exists("app_users", "updated_at"):
            batch.alter_column("updated_at", server_default=None)


def downgrade() -> None:
    # Downgrade is intentionally conservative: do nothing.
    # In drift-repair migrations, dropping columns can destroy real data.
    pass