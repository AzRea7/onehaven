"""add saas tables (plans, org_subscriptions, ledgers, auth, api keys)

Revision ID: 0031_add_saas_tables
Revises: 0030_add_trust_tables
Create Date: 2026-02-28
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0031_add_saas_tables"
down_revision = "0030_add_trust_tables"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def upgrade() -> None:
    # -----------------------------
    # plans
    # -----------------------------
    if not _has_table("plans"):
        op.create_table(
            "plans",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("code", sa.String(length=50), nullable=False),
            sa.Column("name", sa.String(length=100), nullable=False),
            sa.Column("limits_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("code", name="uq_plans_code"),
        )

    # -----------------------------
    # org_subscriptions
    # -----------------------------
    if not _has_table("org_subscriptions"):
        op.create_table(
            "org_subscriptions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("plan_code", sa.String(length=50), nullable=False, server_default=sa.text("'free'")),
            sa.Column("status", sa.String(length=30), nullable=False, server_default=sa.text("'active'")),
            sa.Column("stripe_customer_id", sa.String(length=80), nullable=True),
            sa.Column("stripe_subscription_id", sa.String(length=80), nullable=True),
            sa.Column("current_period_end", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("org_id", name="uq_org_subscriptions_org"),
        )
        op.create_index("ix_org_subscriptions_org_id", "org_subscriptions", ["org_id"], unique=False)

    # -----------------------------
    # usage_ledger
    # -----------------------------
    if not _has_table("usage_ledger"):
        op.create_table(
            "usage_ledger",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("metric", sa.String(length=80), nullable=False),
            sa.Column("units", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("meta_json", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index("ix_usage_ledger_org_id", "usage_ledger", ["org_id"], unique=False)
        op.create_index("ix_usage_ledger_org_metric", "usage_ledger", ["org_id", "metric"], unique=False)

    # -----------------------------
    # external_budget_ledger
    # -----------------------------
    if not _has_table("external_budget_ledger"):
        op.create_table(
            "external_budget_ledger",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("provider", sa.String(length=50), nullable=False),
            sa.Column("cost_units", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("meta_json", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index(
            "ix_external_budget_ledger_org_provider",
            "external_budget_ledger",
            ["org_id", "provider", "created_at"],
            unique=False,
        )

    # -----------------------------
    # agent_run_deadletters
    # -----------------------------
    if not _has_table("agent_run_deadletters"):
        op.create_table(
            "agent_run_deadletters",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("run_id", sa.Integer(), nullable=False),
            sa.Column("agent_key", sa.String(length=80), nullable=False),
            sa.Column("reason", sa.String(length=120), nullable=False),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index(
            "ix_agent_run_deadletters_org_run",
            "agent_run_deadletters",
            ["org_id", "run_id", "id"],
            unique=False,
        )

    # -----------------------------
    # auth_identities
    # -----------------------------
    if not _has_table("auth_identities"):
        op.create_table(
            "auth_identities",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("email", sa.String(length=255), nullable=False),
            sa.Column("password_hash", sa.String(length=255), nullable=False),
            sa.Column("email_verified_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("email", name="uq_auth_identities_email"),
        )
        op.create_index("ix_auth_identities_email", "auth_identities", ["email"], unique=True)

    # -----------------------------
    # email_tokens
    # -----------------------------
    if not _has_table("email_tokens"):
        op.create_table(
            "email_tokens",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("email", sa.String(length=255), nullable=False),
            sa.Column("purpose", sa.String(length=50), nullable=False),
            sa.Column("token_hash", sa.String(length=255), nullable=False),
            sa.Column("expires_at", sa.DateTime(), nullable=False),
            sa.Column("used_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("token_hash", name="uq_email_tokens_token_hash"),
        )
        op.create_index("ix_email_tokens_org_email", "email_tokens", ["org_id", "email"], unique=False)

    # -----------------------------
    # api_keys
    # -----------------------------
    if not _has_table("api_keys"):
        op.create_table(
            "api_keys",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("name", sa.String(length=80), nullable=False),
            sa.Column("key_prefix", sa.String(length=16), nullable=False),
            sa.Column("key_hash", sa.String(length=255), nullable=False),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("revoked_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("org_id", "name", name="uq_api_keys_org_name"),
        )
        op.create_index("ix_api_keys_org_id", "api_keys", ["org_id"], unique=False)
        op.create_index("ix_api_keys_prefix", "api_keys", ["key_prefix"], unique=False)


def downgrade() -> None:
    # Drop in reverse-ish order
    if _has_table("api_keys"):
        op.drop_index("ix_api_keys_prefix", table_name="api_keys")
        op.drop_index("ix_api_keys_org_id", table_name="api_keys")
        op.drop_table("api_keys")

    if _has_table("email_tokens"):
        op.drop_index("ix_email_tokens_org_email", table_name="email_tokens")
        op.drop_table("email_tokens")

    if _has_table("auth_identities"):
        op.drop_index("ix_auth_identities_email", table_name="auth_identities")
        op.drop_table("auth_identities")

    if _has_table("agent_run_deadletters"):
        op.drop_index("ix_agent_run_deadletters_org_run", table_name="agent_run_deadletters")
        op.drop_table("agent_run_deadletters")

    if _has_table("external_budget_ledger"):
        op.drop_index("ix_external_budget_ledger_org_provider", table_name="external_budget_ledger")
        op.drop_table("external_budget_ledger")

    if _has_table("usage_ledger"):
        op.drop_index("ix_usage_ledger_org_metric", table_name="usage_ledger")
        op.drop_index("ix_usage_ledger_org_id", table_name="usage_ledger")
        op.drop_table("usage_ledger")

    if _has_table("org_subscriptions"):
        op.drop_index("ix_org_subscriptions_org_id", table_name="org_subscriptions")
        op.drop_table("org_subscriptions")

    if _has_table("plans"):
        op.drop_table("plans")