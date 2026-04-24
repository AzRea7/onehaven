"""0019 agent messages link run_id

Revision ID: 0019_agent_messages_link_run_id
Revises: 0018_seed_jurisdiction_defaults
Create Date: 2026-02-20
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0019_agent_messages_link_run_id"
down_revision = "0018_seed_jurisdiction_defaults"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_messages", sa.Column("run_id", sa.Integer(), nullable=True))

    # Backfill: best-effort if thread_key looks like "run:{id}"
    # Keep nullable during backfill. You can harden later after cleaning old rows.
    # (If your DB is fresh, there are no old rows anyway.)
    op.create_index("ix_agent_messages_run_id", "agent_messages", ["run_id"], unique=False)

    op.create_foreign_key(
        "fk_agent_messages_run_id_agent_runs",
        "agent_messages",
        "agent_runs",
        ["run_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("fk_agent_messages_run_id_agent_runs", "agent_messages", type_="foreignkey")
    op.drop_index("ix_agent_messages_run_id", table_name="agent_messages")
    op.drop_column("agent_messages", "run_id")