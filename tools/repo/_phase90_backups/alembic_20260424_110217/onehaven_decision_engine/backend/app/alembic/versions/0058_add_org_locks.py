"""add org_locks table"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0058_add_org_locks"
down_revision = "0057_auth_policy_src"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, col: str) -> bool:
    if not _has_table(table):
        return False
    return col in {c["name"] for c in _insp().get_columns(table)}


def _has_index(table: str, index_name: str) -> bool:
    if not _has_table(table):
        return False
    return index_name in {idx["name"] for idx in _insp().get_indexes(table)}


def _has_unique_constraint(table: str, constraint_name: str) -> bool:
    if not _has_table(table):
        return False
    return constraint_name in {
        c["name"] for c in _insp().get_unique_constraints(table) if c.get("name")
    }


def upgrade() -> None:
    if not _has_table("org_locks"):
        op.create_table(
            "org_locks",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "org_id",
                sa.Integer(),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("lock_key", sa.String(length=120), nullable=False),
            sa.Column("owner_token", sa.String(length=120), nullable=False),
            sa.Column(
                "acquired_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.Column("expires_at", sa.DateTime(), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=True),
            sa.UniqueConstraint(
                "org_id",
                "lock_key",
                name="uq_org_locks_org_lock_key",
            ),
        )
    else:
        if not _has_column("org_locks", "org_id"):
            op.add_column(
                "org_locks",
                sa.Column("org_id", sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                "fk_org_locks_org_id_organizations",
                "org_locks",
                "organizations",
                ["org_id"],
                ["id"],
                ondelete="CASCADE",
            )

        if not _has_column("org_locks", "lock_key"):
            op.add_column(
                "org_locks",
                sa.Column("lock_key", sa.String(length=120), nullable=True),
            )

        if not _has_column("org_locks", "owner_token"):
            op.add_column(
                "org_locks",
                sa.Column("owner_token", sa.String(length=120), nullable=True),
            )

        if not _has_column("org_locks", "acquired_at"):
            op.add_column(
                "org_locks",
                sa.Column(
                    "acquired_at",
                    sa.DateTime(),
                    nullable=True,
                    server_default=sa.text("CURRENT_TIMESTAMP"),
                ),
            )

        if not _has_column("org_locks", "expires_at"):
            op.add_column(
                "org_locks",
                sa.Column("expires_at", sa.DateTime(), nullable=True),
            )

        if not _has_column("org_locks", "metadata_json"):
            op.add_column(
                "org_locks",
                sa.Column("metadata_json", sa.JSON(), nullable=True),
            )

        # backfill required nullable->non-null fields where possible
        if _has_column("org_locks", "acquired_at"):
            op.execute(
                sa.text(
                    """
                    UPDATE org_locks
                    SET acquired_at = COALESCE(acquired_at, CURRENT_TIMESTAMP)
                    """
                )
            )

        # make required fields non-null only after they exist and are backfilled
        if (
            _has_column("org_locks", "org_id")
            and _has_column("org_locks", "lock_key")
            and _has_column("org_locks", "owner_token")
            and _has_column("org_locks", "acquired_at")
        ):
            with op.batch_alter_table("org_locks") as batch:
                batch.alter_column("org_id", nullable=False)
                batch.alter_column("lock_key", nullable=False)
                batch.alter_column("owner_token", nullable=False)
                batch.alter_column("acquired_at", nullable=False)

    if _has_table("org_locks") and not _has_unique_constraint(
        "org_locks",
        "uq_org_locks_org_lock_key",
    ):
        with op.batch_alter_table("org_locks") as batch:
            batch.create_unique_constraint(
                "uq_org_locks_org_lock_key",
                ["org_id", "lock_key"],
            )

    if (
        _has_table("org_locks")
        and _has_column("org_locks", "org_id")
        and _has_column("org_locks", "lock_key")
        and not _has_index("org_locks", "ix_org_locks_org_lock_key")
    ):
        op.create_index(
            "ix_org_locks_org_lock_key",
            "org_locks",
            ["org_id", "lock_key"],
            unique=False,
        )

    if (
        _has_table("org_locks")
        and _has_column("org_locks", "expires_at")
        and not _has_index("org_locks", "ix_org_locks_expires_at")
    ):
        op.create_index(
            "ix_org_locks_expires_at",
            "org_locks",
            ["expires_at"],
            unique=False,
        )

    if _has_table("org_locks") and _has_column("org_locks", "acquired_at"):
        with op.batch_alter_table("org_locks") as batch:
            batch.alter_column("acquired_at", server_default=None)


def downgrade() -> None:
    if _has_index("org_locks", "ix_org_locks_expires_at"):
        op.drop_index("ix_org_locks_expires_at", table_name="org_locks")

    if _has_index("org_locks", "ix_org_locks_org_lock_key"):
        op.drop_index("ix_org_locks_org_lock_key", table_name="org_locks")

    if _has_table("org_locks"):
        op.drop_table("org_locks")