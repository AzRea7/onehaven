"""
0042 repair jurisdiction_profiles to match simplified policy_models

This migration "neutralizes" legacy NOT NULL columns created by the earlier
fat schema (key/name/effective_date/program_type/etc.), so the simplified
JurisdictionProfile model + policy_seed inserts can succeed.

Revision ID: 0042_repair_jp
Revises: 0041_fix_policy_models_schema
Create Date: 2026-03-05
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0042_repair_jp"
down_revision = "0041_fix_policy_models_schema"
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


def _constraint_names(table: str) -> set[str]:
    bind = op.get_bind()
    rows = bind.execute(
        text(
            """
            SELECT conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE t.relname = :t
            """
        ),
        {"t": table},
    ).fetchall()
    return {r[0] for r in rows}


def upgrade() -> None:
    if not _has_table("jurisdiction_profiles"):
        return

    bind = op.get_bind()
    constraints = _constraint_names("jurisdiction_profiles")

    # 1) org_id must be nullable for global defaults
    if _has_column("jurisdiction_profiles", "org_id") and not _col_nullable("jurisdiction_profiles", "org_id"):
        with op.batch_alter_table("jurisdiction_profiles") as batch:
            batch.alter_column("org_id", existing_type=sa.Integer(), nullable=True)

    # 2) Neutralize legacy NOT NULL columns that your simplified model does not write
    #    - Backfill NULLs
    #    - Alter to nullable
    #
    # key (legacy)
    if _has_column("jurisdiction_profiles", "key"):
        bind.execute(text("UPDATE jurisdiction_profiles SET key = COALESCE(key, 'legacy') WHERE key IS NULL"))
        if not _col_nullable("jurisdiction_profiles", "key"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("key", existing_type=sa.String(length=120), nullable=True)

    # name (legacy)  <-- this is what is currently failing you
    if _has_column("jurisdiction_profiles", "name"):
        bind.execute(text("UPDATE jurisdiction_profiles SET name = COALESCE(name, 'legacy') WHERE name IS NULL"))
        if not _col_nullable("jurisdiction_profiles", "name"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("name", existing_type=sa.String(length=180), nullable=True)

    # effective_date (legacy) - many fat schemas required it
    if _has_column("jurisdiction_profiles", "effective_date"):
        bind.execute(
            text(
                """
                UPDATE jurisdiction_profiles
                SET effective_date = COALESCE(effective_date, DATE '2026-01-01')
                WHERE effective_date IS NULL
                """
            )
        )
        if not _col_nullable("jurisdiction_profiles", "effective_date"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("effective_date", existing_type=sa.Date(), nullable=True)

    # program_type (legacy) - default to 'hcv'
    if _has_column("jurisdiction_profiles", "program_type"):
        bind.execute(
            text(
                """
                UPDATE jurisdiction_profiles
                SET program_type = COALESCE(program_type, 'hcv')
                WHERE program_type IS NULL
                """
            )
        )
        if not _col_nullable("jurisdiction_profiles", "program_type"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("program_type", existing_type=sa.String(length=40), nullable=True)

    # uses_safmr (legacy) - default to 0
    if _has_column("jurisdiction_profiles", "uses_safmr"):
        bind.execute(text("UPDATE jurisdiction_profiles SET uses_safmr = COALESCE(uses_safmr, 0) WHERE uses_safmr IS NULL"))
        if not _col_nullable("jurisdiction_profiles", "uses_safmr"):
            with op.batch_alter_table("jurisdiction_profiles") as batch:
                batch.alter_column("uses_safmr", existing_type=sa.Integer(), nullable=True)

    # created_at (legacy) - if not null, make sure it can default; if nulls exist, backfill
    if _has_column("jurisdiction_profiles", "created_at"):
        bind.execute(text("UPDATE jurisdiction_profiles SET created_at = COALESCE(created_at, now()) WHERE created_at IS NULL"))
        # If created_at is NOT NULL, that's fine as long as it has default; but we don't rely on it.
        # We leave nullability as-is to avoid surprising constraints changes.

    # 3) Drop old unique constraint (fat schema) if present
    if "uq_jp_org_key_effective" in constraints:
        op.drop_constraint("uq_jp_org_key_effective", "jurisdiction_profiles", type_="unique")

    # 4) Ensure your simplified unique constraint exists
    constraints = _constraint_names("jurisdiction_profiles")
    if "uq_jp_scope_state_county_city" not in constraints:
        op.create_unique_constraint(
            "uq_jp_scope_state_county_city",
            "jurisdiction_profiles",
            ["org_id", "state", "county", "city"],
        )

    # 5) Ensure simplified columns exist (add-only; no drops)
    with op.batch_alter_table("jurisdiction_profiles") as batch:
        if not _has_column("jurisdiction_profiles", "friction_multiplier"):
            batch.add_column(sa.Column("friction_multiplier", sa.Float(), nullable=False, server_default="1.0"))
        if not _has_column("jurisdiction_profiles", "pha_name"):
            batch.add_column(sa.Column("pha_name", sa.String(length=180), nullable=True))
        if not _has_column("jurisdiction_profiles", "policy_json"):
            batch.add_column(sa.Column("policy_json", sa.Text(), nullable=True))
        if not _has_column("jurisdiction_profiles", "notes"):
            batch.add_column(sa.Column("notes", sa.Text(), nullable=True))
        if not _has_column("jurisdiction_profiles", "updated_at"):
            batch.add_column(sa.Column("updated_at", sa.DateTime(), nullable=True))

    # Remove server_default for friction_multiplier after backfill
    with op.batch_alter_table("jurisdiction_profiles") as batch:
        if _has_column("jurisdiction_profiles", "friction_multiplier"):
            batch.alter_column("friction_multiplier", server_default=None)


def downgrade() -> None:
    # Conservative: no destructive downgrade.
    pass