# backend/app/alembic/versions/0024_add_agent_trace_events.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0024_add_agent_trace_events"
down_revision = "0023_agent_messages_add_run_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_trace_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=True),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("agent_key", sa.String(length=80), nullable=False),
        sa.Column("event_type", sa.String(length=40), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    op.create_index("ix_agent_trace_events_org_id", "agent_trace_events", ["org_id"])
    op.create_index("ix_agent_trace_events_property_id", "agent_trace_events", ["property_id"])
    op.create_index("ix_agent_trace_events_run_id", "agent_trace_events", ["run_id"])
    op.create_index("ix_agent_trace_events_agent_key", "agent_trace_events", ["agent_key"])
    op.create_index("ix_agent_trace_events_event_type", "agent_trace_events", ["event_type"])
    op.create_index("ix_agent_trace_events_created_at", "agent_trace_events", ["created_at"])

    # Hot path for streaming: org + run + monotonic id scan
    op.create_index("ix_agent_trace_events_org_run_id_id", "agent_trace_events", ["org_id", "run_id", "id"])


def downgrade() -> None:
    op.drop_index("ix_agent_trace_events_org_run_id_id", table_name="agent_trace_events")
    op.drop_index("ix_agent_trace_events_created_at", table_name="agent_trace_events")
    op.drop_index("ix_agent_trace_events_event_type", table_name="agent_trace_events")
    op.drop_index("ix_agent_trace_events_agent_key", table_name="agent_trace_events")
    op.drop_index("ix_agent_trace_events_run_id", table_name="agent_trace_events")
    op.drop_index("ix_agent_trace_events_property_id", table_name="agent_trace_events")
    op.drop_index("ix_agent_trace_events_org_id", table_name="agent_trace_events")
    op.drop_table("agent_trace_events")