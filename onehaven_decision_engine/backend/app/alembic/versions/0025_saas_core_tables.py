# backend/app/alembic/versions/0025_saas_core_tables.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0025_saas_core_tables"
down_revision = "0024_add_agent_trace_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Users: add password hash + email verified flags (dev-friendly)
    op.add_column("app_users", sa.Column("password_hash", sa.String(length=255), nullable=True))
    op.add_column("app_users", sa.Column("email_verified", sa.Boolean(), nullable=False, server_default=sa.text("0")))
    op.add_column("app_users", sa.Column("last_login_at", sa.DateTime(), nullable=True))

    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=80), nullable=False),
        sa.Column("key_prefix", sa.String(length=12), nullable=False),
        sa.Column("key_hash", sa.String(length=255), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_api_keys_org_id", "api_keys", ["org_id"])
    op.create_index("ix_api_keys_prefix", "api_keys", ["key_prefix"], unique=True)

    op.create_table(
        "plans",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=40), nullable=False),
        sa.Column("name", sa.String(length=80), nullable=False),
        sa.Column("limits_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_plans_code", "plans", ["code"], unique=True)

    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("plan_code", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        sa.Column("stripe_customer_id", sa.String(length=80), nullable=True),
        sa.Column("stripe_subscription_id", sa.String(length=80), nullable=True),
        sa.Column("current_period_start", sa.DateTime(), nullable=True),
        sa.Column("current_period_end", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_subscriptions_org_id", "subscriptions", ["org_id"])
    op.create_index("ix_subscriptions_org_active", "subscriptions", ["org_id", "status"])

    # Usage ledger: records every metered action (agent_run, external_call, etc.)
    op.create_table(
        "usage_ledger",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=True),
        sa.Column("units", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("ref_id", sa.String(length=80), nullable=True),
        sa.Column("day_key", sa.String(length=10), nullable=False),  # YYYY-MM-DD UTC
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_usage_ledger_org_day_kind", "usage_ledger", ["org_id", "day_key", "kind"])
    op.create_index("ix_usage_ledger_org_day_provider", "usage_ledger", ["org_id", "day_key", "provider"])
    op.create_index("ix_usage_ledger_ref_id", "usage_ledger", ["ref_id"])

    # Concurrency locks: enforce “only 1 compliance agent per org” etc.
    op.create_table(
        "org_locks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("lock_key", sa.String(length=80), nullable=False),
        sa.Column("owner", sa.String(length=80), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_org_locks_org_key", "org_locks", ["org_id", "lock_key"], unique=True)
    op.create_index("ix_org_locks_expires", "org_locks", ["expires_at"])

    # Deadletters: poison runs record
    op.create_table(
        "agent_run_deadletters",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("agent_key", sa.String(length=80), nullable=False),
        sa.Column("reason", sa.String(length=255), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_deadletters_org_id", "agent_run_deadletters", ["org_id"])
    op.create_index("ix_deadletters_run_id", "agent_run_deadletters", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_deadletters_run_id", table_name="agent_run_deadletters")
    op.drop_index("ix_deadletters_org_id", table_name="agent_run_deadletters")
    op.drop_table("agent_run_deadletters")

    op.drop_index("ix_org_locks_expires", table_name="org_locks")
    op.drop_index("ix_org_locks_org_key", table_name="org_locks")
    op.drop_table("org_locks")

    op.drop_index("ix_usage_ledger_ref_id", table_name="usage_ledger")
    op.drop_index("ix_usage_ledger_org_day_provider", table_name="usage_ledger")
    op.drop_index("ix_usage_ledger_org_day_kind", table_name="usage_ledger")
    op.drop_table("usage_ledger")

    op.drop_index("ix_subscriptions_org_active", table_name="subscriptions")
    op.drop_index("ix_subscriptions_org_id", table_name="subscriptions")
    op.drop_table("subscriptions")

    op.drop_index("ix_plans_code", table_name="plans")
    op.drop_table("plans")

    op.drop_index("ix_api_keys_prefix", table_name="api_keys")
    op.drop_index("ix_api_keys_org_id", table_name="api_keys")
    op.drop_table("api_keys")

    op.drop_column("app_users", "last_login_at")
    op.drop_column("app_users", "email_verified")
    op.drop_column("app_users", "password_hash")