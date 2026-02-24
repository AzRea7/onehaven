# backend/app/alembic/versions/0022_agent_runs_reliability_fields.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0022_agent_runs_reliability_fields"
down_revision = "0021_inspections_add_org_id"
branch_labels = None
depends_on = None


def upgrade():
    # 1) Add columns (nullable-first where safe)
    op.add_column("agent_runs", sa.Column("idempotency_key", sa.String(length=128), nullable=True))
    op.create_index("ix_agent_runs_idempotency_key", "agent_runs", ["idempotency_key"], unique=False)

    op.add_column("agent_runs", sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("agent_runs", sa.Column("last_error", sa.Text(), nullable=True))

    op.add_column("agent_runs", sa.Column("started_at", sa.DateTime(), nullable=True))
    op.add_column("agent_runs", sa.Column("finished_at", sa.DateTime(), nullable=True))
    op.add_column("agent_runs", sa.Column("heartbeat_at", sa.DateTime(), nullable=True))

    op.add_column("agent_runs", sa.Column("created_by_user_id", sa.Integer(), nullable=True))

    op.add_column(
        "agent_runs",
        sa.Column("approval_status", sa.String(length=20), nullable=False, server_default="not_required"),
    )
    op.add_column("agent_runs", sa.Column("approved_by_user_id", sa.Integer(), nullable=True))
    op.add_column("agent_runs", sa.Column("approved_at", sa.DateTime(), nullable=True))

    op.add_column("agent_runs", sa.Column("proposed_actions_json", sa.Text(), nullable=True))

    # 2) Foreign keys for user refs (safe nullable)
    op.create_foreign_key(
        "fk_agent_runs_created_by_user_id",
        "agent_runs",
        "app_users",
        ["created_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_agent_runs_approved_by_user_id",
        "agent_runs",
        "app_users",
        ["approved_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # 3) Unique constraint to enforce idempotency per org
    # NOTE: In Postgres, UNIQUE allows multiple NULLs, which is what we want.
    op.create_unique_constraint(
        "uq_agent_runs_org_idempotency_key",
        "agent_runs",
        ["org_id", "idempotency_key"],
    )

    # 4) Remove server defaults (keeps schema clean, values already backfilled)
    op.alter_column("agent_runs", "attempts", server_default=None)
    op.alter_column("agent_runs", "approval_status", server_default=None)


def downgrade():
    op.drop_constraint("uq_agent_runs_org_idempotency_key", "agent_runs", type_="unique")

    op.drop_constraint("fk_agent_runs_approved_by_user_id", "agent_runs", type_="foreignkey")
    op.drop_constraint("fk_agent_runs_created_by_user_id", "agent_runs", type_="foreignkey")

    op.drop_column("agent_runs", "proposed_actions_json")
    op.drop_column("agent_runs", "approved_at")
    op.drop_column("agent_runs", "approved_by_user_id")
    op.drop_column("agent_runs", "approval_status")
    op.drop_column("agent_runs", "created_by_user_id")
    op.drop_column("agent_runs", "heartbeat_at")
    op.drop_column("agent_runs", "finished_at")
    op.drop_column("agent_runs", "started_at")
    op.drop_column("agent_runs", "last_error")
    op.drop_column("agent_runs", "attempts")

    op.drop_index("ix_agent_runs_idempotency_key", table_name="agent_runs")
    op.drop_column("agent_runs", "idempotency_key")
    