"""
0043 make legacy jurisdiction_profiles columns nullable so simplified seed works

Revision ID: 0043_make_jp_legacy_nullable
Revises: 0042_repair_jurisdiction_profiles
Create Date: 2026-03-05
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0043_make_jp_legacy_nullable"
down_revision = "0042_repair_jp"
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


def upgrade() -> None:
    if not _has_table("jurisdiction_profiles"):
        return

    bind = op.get_bind()

    # name (legacy)
    if _has_column("jurisdiction_profiles", "name"):
        bind.execute(text("UPDATE jurisdiction_profiles SET name = COALESCE(name, 'legacy') WHERE name IS NULL"))
        if not _col_nullable("jurisdiction_profiles", "name"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("name", existing_type=sa.String(length=180), nullable=True)

    # effective_date (legacy)
    if _has_column("jurisdiction_profiles", "effective_date"):
        bind.execute(
            text("UPDATE jurisdiction_profiles SET effective_date = COALESCE(effective_date, DATE '2026-01-01') WHERE effective_date IS NULL")
        )
        if not _col_nullable("jurisdiction_profiles", "effective_date"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("effective_date", existing_type=sa.Date(), nullable=True)

    # program_type (legacy)
    if _has_column("jurisdiction_profiles", "program_type"):
        bind.execute(text("UPDATE jurisdiction_profiles SET program_type = COALESCE(program_type, 'hcv') WHERE program_type IS NULL"))
        if not _col_nullable("jurisdiction_profiles", "program_type"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("program_type", existing_type=sa.String(length=40), nullable=True)


def downgrade() -> None:
    pass