"""add trust tables

Revision ID: 0030_add_trust_tables
Revises: 0029_create_checklist_template_items
Create Date: 2026-02-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0030_add_trust_tables"
down_revision = "0029_create_checklist_template_items"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def upgrade() -> None:
    # Idempotent-ish for messy dev DBs
    if not _has_table("trust_signals"):
        op.create_table(
            "trust_signals",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("entity_type", sa.String(length=40), nullable=False, index=True),
            sa.Column("entity_id", sa.String(length=80), nullable=False, index=True),
            sa.Column("signal_key", sa.String(length=120), nullable=False, index=True),
            sa.Column("value", sa.Float(), nullable=False),
            sa.Column("weight", sa.Float(), nullable=False, server_default=sa.text("1.0")),
            sa.Column("meta_json", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index(
            "ix_trust_signals_org_entity",
            "trust_signals",
            ["org_id", "entity_type", "entity_id", "created_at"],
            unique=False,
        )
        op.create_index(
            "ix_trust_signals_org_entity_key",
            "trust_signals",
            ["org_id", "entity_type", "entity_id", "signal_key"],
            unique=False,
        )

    if not _has_table("trust_scores"):
        op.create_table(
            "trust_scores",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("entity_type", sa.String(length=40), nullable=False, index=True),
            sa.Column("entity_id", sa.String(length=80), nullable=False, index=True),
            sa.Column("score", sa.Float(), nullable=False, server_default=sa.text("0.0")),
            sa.Column("confidence", sa.Float(), nullable=False, server_default=sa.text("0.0")),
            sa.Column("components_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index(
            "ix_trust_scores_unique",
            "trust_scores",
            ["org_id", "entity_type", "entity_id"],
            unique=True,
        )


def downgrade() -> None:
    if _has_table("trust_scores"):
        op.drop_index("ix_trust_scores_unique", table_name="trust_scores")
        op.drop_table("trust_scores")

    if _has_table("trust_signals"):
        op.drop_index("ix_trust_signals_org_entity_key", table_name="trust_signals")
        op.drop_index("ix_trust_signals_org_entity", table_name="trust_signals")
        op.drop_table("trust_signals")