"""fix org_locks schema for dataset locks

Revision ID: 0064_fix_org_locks_schema
Revises: 0063_add_market_sync_states
Create Date: 2026-03-27 00:55:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


# revision identifiers, used by Alembic.
revision = "0064_fix_org_locks_schema"
down_revision = "0063_add_market_sync_states"
branch_labels = None
depends_on = None


def _has_table(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _columns_by_name(inspector, table_name: str) -> dict[str, dict]:
    return {col["name"]: col for col in inspector.get_columns(table_name)}


def _index_names(inspector, table_name: str) -> set[str]:
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


def _unique_constraint_names(inspector, table_name: str) -> set[str]:
    return {uq["name"] for uq in inspector.get_unique_constraints(table_name) if uq.get("name")}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not _has_table(inspector, "org_locks"):
        op.create_table(
            "org_locks",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("lock_key", sa.String(length=120), nullable=False),
            sa.Column("owner_token", sa.String(length=120), nullable=False, server_default="system"),
            sa.Column("acquired_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("locked_until", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )
        op.create_unique_constraint(
            "uq_org_locks_org_lock_key",
            "org_locks",
            ["org_id", "lock_key"],
        )
        op.create_index("ix_org_locks_org_lock_key", "org_locks", ["org_id", "lock_key"], unique=False)
        op.create_index("ix_org_locks_locked_until", "org_locks", ["locked_until"], unique=False)
        return

    cols = _columns_by_name(inspector, "org_locks")
    idx_names = _index_names(inspector, "org_locks")
    uq_names = _unique_constraint_names(inspector, "org_locks")

    # owner_token
    if "owner_token" not in cols:
        op.add_column(
            "org_locks",
            sa.Column("owner_token", sa.String(length=120), nullable=True),
        )

    bind.execute(text("UPDATE org_locks SET owner_token = COALESCE(NULLIF(owner_token, ''), 'system')"))
    op.alter_column(
        "org_locks",
        "owner_token",
        existing_type=sa.String(length=120),
        nullable=False,
        server_default="system",
    )

    # acquired_at
    if "acquired_at" not in cols:
        op.add_column(
            "org_locks",
            sa.Column("acquired_at", sa.DateTime(), nullable=True),
        )

    bind.execute(text("UPDATE org_locks SET acquired_at = COALESCE(acquired_at, created_at, now())"))
    op.alter_column(
        "org_locks",
        "acquired_at",
        existing_type=sa.DateTime(),
        nullable=False,
        server_default=sa.text("now()"),
    )

    # locked_until: make this the canonical expiry column
    if "locked_until" not in cols:
        op.add_column(
            "org_locks",
            sa.Column("locked_until", sa.DateTime(), nullable=True),
        )

    if "expires_at" in cols:
        bind.execute(
            text(
                """
                UPDATE org_locks
                SET locked_until = COALESCE(locked_until, expires_at, created_at, now())
                """
            )
        )
    else:
        bind.execute(
            text(
                """
                UPDATE org_locks
                SET locked_until = COALESCE(locked_until, created_at, now())
                """
            )
        )

    op.alter_column(
        "org_locks",
        "locked_until",
        existing_type=sa.DateTime(),
        nullable=False,
        server_default=sa.text("now()"),
    )

    # timestamps
    if "created_at" not in cols:
        op.add_column(
            "org_locks",
            sa.Column("created_at", sa.DateTime(), nullable=True),
        )
        bind.execute(text("UPDATE org_locks SET created_at = now() WHERE created_at IS NULL"))
        op.alter_column(
            "org_locks",
            "created_at",
            existing_type=sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        )

    if "updated_at" not in cols:
        op.add_column(
            "org_locks",
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )
        bind.execute(text("UPDATE org_locks SET updated_at = COALESCE(updated_at, created_at, now())"))
        op.alter_column(
            "org_locks",
            "updated_at",
            existing_type=sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        )

    # unique constraint
    if "uq_org_locks_org_lock_key" not in uq_names:
        # best effort: drop legacy duplicate uniques if present
        for legacy_name in [
            "uq_org_lock_org_id_lock_key",
            "uq_org_locks_org_id_lock_key",
        ]:
            if legacy_name in uq_names:
                op.drop_constraint(legacy_name, "org_locks", type_="unique")
        op.create_unique_constraint(
            "uq_org_locks_org_lock_key",
            "org_locks",
            ["org_id", "lock_key"],
        )

    # indexes
    if "ix_org_locks_org_lock_key" not in idx_names:
        op.create_index("ix_org_locks_org_lock_key", "org_locks", ["org_id", "lock_key"], unique=False)

    if "ix_org_locks_locked_until" not in idx_names:
        op.create_index("ix_org_locks_locked_until", "org_locks", ["locked_until"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not _has_table(inspector, "org_locks"):
        return

    idx_names = _index_names(inspector, "org_locks")
    uq_names = _unique_constraint_names(inspector, "org_locks")
    cols = _columns_by_name(inspector, "org_locks")

    if "ix_org_locks_locked_until" in idx_names:
        op.drop_index("ix_org_locks_locked_until", table_name="org_locks")

    if "ix_org_locks_org_lock_key" in idx_names:
        op.drop_index("ix_org_locks_org_lock_key", table_name="org_locks")

    if "uq_org_locks_org_lock_key" in uq_names:
        op.drop_constraint("uq_org_locks_org_lock_key", "org_locks", type_="unique")

    # Keep the table, but remove only the columns introduced here when possible.
    # Do not drop locked_until because older deployments may still depend on it.
    if "owner_token" in cols:
        op.alter_column(
            "org_locks",
            "owner_token",
            existing_type=sa.String(length=120),
            server_default=None,
            nullable=True,
        )

    if "acquired_at" in cols:
        op.alter_column(
            "org_locks",
            "acquired_at",
            existing_type=sa.DateTime(),
            server_default=None,
            nullable=True,
        )

    if "created_at" in cols:
        op.alter_column(
            "org_locks",
            "created_at",
            existing_type=sa.DateTime(),
            server_default=None,
            nullable=True,
        )

    if "updated_at" in cols:
        op.alter_column(
            "org_locks",
            "updated_at",
            existing_type=sa.DateTime(),
            server_default=None,
            nullable=True,
        )

    if "locked_until" in cols:
        op.alter_column(
            "org_locks",
            "locked_until",
            existing_type=sa.DateTime(),
            server_default=None,
            nullable=True,
        )