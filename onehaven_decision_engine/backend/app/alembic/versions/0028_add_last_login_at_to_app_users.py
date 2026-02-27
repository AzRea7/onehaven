"""add last_login_at to app_users

Revision ID: 0028_add_last_login_at_to_app_users
Revises: 0027_widen_alembic_version
Create Date: 2026-02-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0028_add_last_login_at_to_app_users"
down_revision = "0027_widen_alembic_version"
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
    # AppUser model expects app_users.last_login_at
    if _has_table("app_users") and not _has_column("app_users", "last_login_at"):
        op.add_column(
            "app_users",
            sa.Column("last_login_at", sa.DateTime(), nullable=True),
        )


def downgrade() -> None:
    if _has_table("app_users") and _has_column("app_users", "last_login_at"):
        op.drop_column("app_users", "last_login_at")