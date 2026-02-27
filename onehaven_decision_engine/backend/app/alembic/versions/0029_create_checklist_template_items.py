"""create checklist_template_items

Revision ID: 0029_create_checklist_template_items
Revises: 0028_add_last_login_at_to_app_users
Create Date: 2026-02-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0029_create_checklist_template_items"
down_revision = "0028_add_last_login_at_to_app_users"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def upgrade() -> None:
    # Only create if missing (idempotent-ish for messy dev DBs)
    if _has_table("checklist_template_items"):
        return

    op.create_table(
        "checklist_template_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=True, index=True),
        sa.Column("strategy", sa.String(length=32), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("description", sa.String(length=512), nullable=False),
        sa.Column("applies_if_json", sa.Text(), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=True),
        sa.Column("common_fail", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    # Uniqueness across org-scoped template versions
    op.create_index(
        "ix_checklist_template_items_unique",
        "checklist_template_items",
        ["org_id", "strategy", "version", "code"],
        unique=True,
    )


def downgrade() -> None:
    if not _has_table("checklist_template_items"):
        return
    op.drop_index("ix_checklist_template_items_unique", table_name="checklist_template_items")
    op.drop_table("checklist_template_items")