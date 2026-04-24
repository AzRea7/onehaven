"""ops tabs + agents

Revision ID: 0008_ops_and_agents
Revises: 0007_add_deal_strategy
Create Date: 2026-02-11
"""

from alembic import op
import sqlalchemy as sa

revision = "0008_ops_and_agents"
down_revision = "0007_add_deal_strategy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -------------------------
    # inspection_events
    # -------------------------
    op.create_table(
        "inspection_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("inspector_name", sa.String(length=120), nullable=True),
        sa.Column("inspection_date", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="scheduled"),
        sa.Column("fail_items_json", sa.Text(), nullable=True),
        sa.Column("resolution_notes", sa.Text(), nullable=True),
        sa.Column("days_to_resolve", sa.Integer(), nullable=True),
        sa.Column("reinspection_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_inspection_events_property_id", "inspection_events", ["property_id"])
    op.create_index("ix_inspection_events_created_at", "inspection_events", ["created_at"])

    # -------------------------
    # rehab_tasks
    # -------------------------
    op.create_table(
        "rehab_tasks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False, server_default="rehab"),
        sa.Column("inspection_relevant", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="todo"),
        sa.Column("cost_estimate", sa.Float(), nullable=True),
        sa.Column("vendor", sa.String(length=120), nullable=True),
        sa.Column("deadline", sa.DateTime(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_rehab_tasks_property_id", "rehab_tasks", ["property_id"])
    op.create_index("ix_rehab_tasks_status", "rehab_tasks", ["status"])
    op.create_index("ix_rehab_tasks_created_at", "rehab_tasks", ["created_at"])

    # -------------------------
    # tenants
    # -------------------------
    op.create_table(
        "tenants",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("full_name", sa.String(length=200), nullable=False),
        sa.Column("phone", sa.String(length=50), nullable=True),
        sa.Column("email", sa.String(length=200), nullable=True),
        sa.Column("voucher_status", sa.String(length=80), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_tenants_full_name", "tenants", ["full_name"])
    op.create_index("ix_tenants_created_at", "tenants", ["created_at"])

    # -------------------------
    # leases
    # -------------------------
    op.create_table(
        "leases",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "tenant_id",
            sa.Integer(),
            sa.ForeignKey("tenants.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("start_date", sa.DateTime(), nullable=False),
        sa.Column("end_date", sa.DateTime(), nullable=True),
        sa.Column("total_rent", sa.Float(), nullable=False, server_default="0"),
        sa.Column("tenant_portion", sa.Float(), nullable=True),
        sa.Column("housing_authority_portion", sa.Float(), nullable=True),
        sa.Column("hap_contract_status", sa.String(length=80), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_leases_property_id", "leases", ["property_id"])
    op.create_index("ix_leases_tenant_id", "leases", ["tenant_id"])
    op.create_index("ix_leases_start_date", "leases", ["start_date"])

    # -------------------------
    # transactions (cash flow)
    # -------------------------
    op.create_table(
        "transactions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("txn_date", sa.DateTime(), nullable=False),
        sa.Column("txn_type", sa.String(length=50), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("memo", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_transactions_property_id", "transactions", ["property_id"])
    op.create_index("ix_transactions_txn_date", "transactions", ["txn_date"])
    op.create_index("ix_transactions_txn_type", "transactions", ["txn_type"])

    # -------------------------
    # valuations (equity)
    # -------------------------
    op.create_table(
        "valuations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("as_of", sa.DateTime(), nullable=False),
        sa.Column("estimated_value", sa.Float(), nullable=False, server_default="0"),
        sa.Column("loan_balance", sa.Float(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_valuations_property_id", "valuations", ["property_id"])
    op.create_index("ix_valuations_as_of", "valuations", ["as_of"])

    # -------------------------
    # agent_runs
    # -------------------------
    op.create_table(
        "agent_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("agent_key", sa.String(length=80), nullable=False),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="created"),
        sa.Column("result_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_agent_runs_agent_key", "agent_runs", ["agent_key"])
    op.create_index("ix_agent_runs_property_id", "agent_runs", ["property_id"])
    op.create_index("ix_agent_runs_created_at", "agent_runs", ["created_at"])

    # -------------------------
    # agent_messages
    # -------------------------
    op.create_table(
        "agent_messages",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("thread_key", sa.String(length=120), nullable=False),
        sa.Column("sender", sa.String(length=120), nullable=False),
        sa.Column("recipient", sa.String(length=120), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_agent_messages_thread_key", "agent_messages", ["thread_key"])
    op.create_index("ix_agent_messages_created_at", "agent_messages", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_agent_messages_created_at", table_name="agent_messages")
    op.drop_index("ix_agent_messages_thread_key", table_name="agent_messages")
    op.drop_table("agent_messages")

    op.drop_index("ix_agent_runs_created_at", table_name="agent_runs")
    op.drop_index("ix_agent_runs_property_id", table_name="agent_runs")
    op.drop_index("ix_agent_runs_agent_key", table_name="agent_runs")
    op.drop_table("agent_runs")

    op.drop_index("ix_valuations_as_of", table_name="valuations")
    op.drop_index("ix_valuations_property_id", table_name="valuations")
    op.drop_table("valuations")

    op.drop_index("ix_transactions_txn_type", table_name="transactions")
    op.drop_index("ix_transactions_txn_date", table_name="transactions")
    op.drop_index("ix_transactions_property_id", table_name="transactions")
    op.drop_table("transactions")

    op.drop_index("ix_leases_start_date", table_name="leases")
    op.drop_index("ix_leases_tenant_id", table_name="leases")
    op.drop_index("ix_leases_property_id", table_name="leases")
    op.drop_table("leases")

    op.drop_index("ix_tenants_created_at", table_name="tenants")
    op.drop_index("ix_tenants_full_name", table_name="tenants")
    op.drop_table("tenants")

    op.drop_index("ix_rehab_tasks_created_at", table_name="rehab_tasks")
    op.drop_index("ix_rehab_tasks_status", table_name="rehab_tasks")
    op.drop_index("ix_rehab_tasks_property_id", table_name="rehab_tasks")
    op.drop_table("rehab_tasks")

    op.drop_index("ix_inspection_events_created_at", table_name="inspection_events")
    op.drop_index("ix_inspection_events_property_id", table_name="inspection_events")
    op.drop_table("inspection_events")
