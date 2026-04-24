"""
0044 fix jurisdiction_profiles created_at default/backfill so simplified seed works

Revision ID: 0044_fix_jp_created_at_default
Revises: 0043_make_jp_legacy_nullable
Create Date: 2026-03-05
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0044_fix_jp_created_at_default"
down_revision = "0043_make_jp_legacy_nullable"
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


def _col_nullable(table: str, col: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        text(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name=:t
              AND column_name=:c
            """
        ),
        {"t": table, "c": col},
    ).first()
    if not row:
        return True
    return (row[0] or "").upper() == "YES"


def _col_default(table: str, col: str) -> str | None:
    bind = op.get_bind()
    row = bind.execute(
        text(
            """
            SELECT column_default
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name=:t
              AND column_name=:c
            """
        ),
        {"t": table, "c": col},
    ).first()
    if not row:
        return None
    return row[0]


def upgrade() -> None:
    if not _has_table("jurisdiction_profiles"):
        return

    bind = op.get_bind()

    # created_at is legacy but MUST be safe for inserts that don't pass it.
    if _has_column("jurisdiction_profiles", "created_at"):
        # 1) Backfill any existing NULLs
        bind.execute(
            text(
                """
                UPDATE jurisdiction_profiles
                SET created_at = COALESCE(created_at, now())
                WHERE created_at IS NULL
                """
            )
        )

        # 2) Ensure server default exists (now()) so future inserts succeed
        default = _col_default("jurisdiction_profiles", "created_at")
        # column_default formats vary; just ensure *some* now()/CURRENT_TIMESTAMP is present
        needs_default = not default or ("now" not in default.lower() and "current_timestamp" not in default.lower())

        # 3) Enforce NOT NULL (after backfill)
        with op.batch_alter_table("jurisdiction_profiles") as batch:
            if needs_default:
                batch.alter_column(
                    "created_at",
                    existing_type=sa.DateTime(),
                    server_default=sa.text("now()"),
                    nullable=False,
                )
            else:
                batch.alter_column(
                    "created_at",
                    existing_type=sa.DateTime(),
                    nullable=False,
                )

    # Optional: updated_at should be nullable; leave it as-is (your seed supplies updated_at anyway).


def downgrade() -> None:
    # Conservative: don't remove defaults / nullability constraints in downgrade.
    pass