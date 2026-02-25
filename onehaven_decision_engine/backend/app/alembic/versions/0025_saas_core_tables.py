from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0025_saas_core_tables"
down_revision = "0024_add_agent_trace_events"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, column: str) -> bool:
    cols = [c["name"] for c in _insp().get_columns(table)]
    return column in cols


def _has_index(table: str, index_name: str) -> bool:
    idx = [i["name"] for i in _insp().get_indexes(table)]
    return index_name in idx


def upgrade() -> None:
    # -------------------------
    # Users: add password hash + email verified flags (dev-friendly)
    # -------------------------
    if _has_table("app_users"):
        if not _has_column("app_users", "password_hash"):
            op.add_column(
                "app_users",
                sa.Column("password_hash", sa.String(length=255), nullable=True),
            )

        # ✅ Postgres needs true/false for boolean defaults (not 0/1)
        # If this column exists already (from a partial run), we do not re-add it.
        if not _has_column("app_users", "email_verified"):
            op.add_column(
                "app_users",
                sa.Column(
                    "email_verified",
                    sa.Boolean(),
                    nullable=False,
                    server_default=sa.text("false"),
                ),
            )
        else:
            # Best-effort: normalize default in case an earlier attempt used 0/1.
            # (If DB already has correct default, this is harmless.)
            try:
                op.alter_column(
                    "app_users",
                    "email_verified",
                    existing_type=sa.Boolean(),
                    server_default=sa.text("false"),
                    existing_nullable=False,
                )
            except Exception:
                # Don't fail the whole migration on a default tweak.
                pass

    # -------------------------
    # Orgs
    # -------------------------
    if not _has_table("orgs"):
        op.create_table(
            "orgs",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("slug", sa.String(length=80), nullable=False, unique=True),
            sa.Column("name", sa.String(length=120), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )

    if _has_table("orgs") and not _has_index("orgs", "ix_orgs_slug"):
        op.create_index("ix_orgs_slug", "orgs", ["slug"], unique=True)

    # -------------------------
    # Memberships
    # -------------------------
    if not _has_table("org_memberships"):
        op.create_table(
            "org_memberships",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=False),
            sa.Column("role", sa.String(length=30), nullable=False, server_default=sa.text("'member'")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("org_id", "user_id", name="uq_org_membership_org_user"),
        )

    if _has_table("org_memberships") and not _has_index("org_memberships", "ix_org_memberships_org_id"):
        op.create_index("ix_org_memberships_org_id", "org_memberships", ["org_id"])
    if _has_table("org_memberships") and not _has_index("org_memberships", "ix_org_memberships_user_id"):
        op.create_index("ix_org_memberships_user_id", "org_memberships", ["user_id"])

    # -------------------------
    # Plans
    # -------------------------
    if not _has_table("plans"):
        op.create_table(
            "plans",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("code", sa.String(length=40), nullable=False, unique=True),
            sa.Column("name", sa.String(length=80), nullable=False),
            sa.Column("price_cents", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("max_properties", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("max_agent_runs_per_day", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )

    if _has_table("plans") and not _has_index("plans", "ix_plans_code"):
        op.create_index("ix_plans_code", "plans", ["code"], unique=True)

    # -------------------------
    # Subscriptions
    # -------------------------
    if not _has_table("subscriptions"):
        op.create_table(
            "subscriptions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("plan_code", sa.String(length=40), nullable=False, server_default=sa.text("'free'")),
            sa.Column("status", sa.String(length=20), nullable=False, server_default=sa.text("'active'")),
            # ✅ boolean default must be true/false in Postgres
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("current_period_end", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )

    if _has_table("subscriptions") and not _has_index("subscriptions", "ix_subscriptions_org_id"):
        op.create_index("ix_subscriptions_org_id", "subscriptions", ["org_id"])
    if _has_table("subscriptions") and not _has_index("subscriptions", "ix_subscriptions_org_active"):
        op.create_index("ix_subscriptions_org_active", "subscriptions", ["org_id", "status"])

    # -------------------------
    # Usage ledger: records every metered action (agent_run, external_call, etc.)
    # -------------------------
    if not _has_table("usage_ledger"):
        op.create_table(
            "usage_ledger",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("kind", sa.String(length=40), nullable=False),
            sa.Column("provider", sa.String(length=40), nullable=True),
            # ✅ integer default stays integer-ish
            sa.Column("units", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("ref_id", sa.String(length=80), nullable=True),
            sa.Column("day_key", sa.String(length=10), nullable=False),  # YYYY-MM-DD UTC
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )

    if _has_table("usage_ledger") and not _has_index("usage_ledger", "ix_usage_ledger_org_day_kind"):
        op.create_index("ix_usage_ledger_org_day_kind", "usage_ledger", ["org_id", "day_key", "kind"])
    if _has_table("usage_ledger") and not _has_index("usage_ledger", "ix_usage_ledger_org_day_provider"):
        op.create_index(
            "ix_usage_ledger_org_day_provider",
            "usage_ledger",
            ["org_id", "day_key", "provider"],
        )
    if _has_table("usage_ledger") and not _has_index("usage_ledger", "ix_usage_ledger_ref_id"):
        op.create_index("ix_usage_ledger_ref_id", "usage_ledger", ["ref_id"])

    # -------------------------
    # Concurrency locks: enforce “only 1 compliance agent per org” etc.
    # -------------------------
    if not _has_table("org_locks"):
        op.create_table(
            "org_locks",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("lock_key", sa.String(length=80), nullable=False),
            sa.Column("locked_until", sa.DateTime(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("org_id", "lock_key", name="uq_org_lock_org_key"),
        )

    if _has_table("org_locks") and not _has_index("org_locks", "ix_org_locks_org_id"):
        op.create_index("ix_org_locks_org_id", "org_locks", ["org_id"])
    if _has_table("org_locks") and not _has_index("org_locks", "ix_org_locks_key"):
        op.create_index("ix_org_locks_key", "org_locks", ["lock_key"])


def downgrade() -> None:
    # Best-effort guarded downgrade to avoid “does not exist” failures on drifted DBs.

    if _has_table("org_locks"):
        if _has_index("org_locks", "ix_org_locks_key"):
            op.drop_index("ix_org_locks_key", table_name="org_locks")
        if _has_index("org_locks", "ix_org_locks_org_id"):
            op.drop_index("ix_org_locks_org_id", table_name="org_locks")
        op.drop_table("org_locks")

    if _has_table("usage_ledger"):
        if _has_index("usage_ledger", "ix_usage_ledger_ref_id"):
            op.drop_index("ix_usage_ledger_ref_id", table_name="usage_ledger")
        if _has_index("usage_ledger", "ix_usage_ledger_org_day_provider"):
            op.drop_index("ix_usage_ledger_org_day_provider", table_name="usage_ledger")
        if _has_index("usage_ledger", "ix_usage_ledger_org_day_kind"):
            op.drop_index("ix_usage_ledger_org_day_kind", table_name="usage_ledger")
        op.drop_table("usage_ledger")

    if _has_table("subscriptions"):
        if _has_index("subscriptions", "ix_subscriptions_org_active"):
            op.drop_index("ix_subscriptions_org_active", table_name="subscriptions")
        if _has_index("subscriptions", "ix_subscriptions_org_id"):
            op.drop_index("ix_subscriptions_org_id", table_name="subscriptions")
        op.drop_table("subscriptions")

    if _has_table("plans"):
        if _has_index("plans", "ix_plans_code"):
            op.drop_index("ix_plans_code", table_name="plans")
        op.drop_table("plans")

    if _has_table("org_memberships"):
        if _has_index("org_memberships", "ix_org_memberships_user_id"):
            op.drop_index("ix_org_memberships_user_id", table_name="org_memberships")
        if _has_index("org_memberships", "ix_org_memberships_org_id"):
            op.drop_index("ix_org_memberships_org_id", table_name="org_memberships")
        op.drop_table("org_memberships")

    if _has_table("orgs"):
        if _has_index("orgs", "ix_orgs_slug"):
            op.drop_index("ix_orgs_slug", table_name="orgs")
        op.drop_table("orgs")

    if _has_table("app_users"):
        if _has_column("app_users", "email_verified"):
            op.drop_column("app_users", "email_verified")
        if _has_column("app_users", "password_hash"):
            op.drop_column("app_users", "password_hash")