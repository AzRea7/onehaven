"""
0041 fix policy_models schema to support global defaults (org_id NULL)

Revision ID: 0041_fix_policy_models_schema
Revises: 0040_policy_models_bootstrap
Create Date: 2026-03-05
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0041_fix_policy_models_schema"
down_revision = "0040_policy_models_bootstrap"
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


def _has_uc(table: str, name: str) -> bool:
    if not _has_table(table):
        return False
    return name in {c["name"] for c in _insp().get_unique_constraints(table)}


def upgrade() -> None:
    if not _has_table("jurisdiction_profiles"):
        return

    # 1) Allow global defaults (org_id NULL)
    with op.batch_alter_table("jurisdiction_profiles") as batch:
        # Only alter if column exists
        if _has_column("jurisdiction_profiles", "org_id"):
            batch.alter_column("org_id", existing_type=sa.Integer(), nullable=True)

    # 2) Ensure the uniqueness constraint matches your model:
    #    UniqueConstraint("org_id","state","county","city", name="uq_jp_scope_state_county_city")
    #
    # Your 0040 migration created uq_jp_org_key_effective (and extra columns like key/effective_date).
    # We can keep extra columns, but we must enforce correct uniqueness for resolution/upsert safety.
    #
    # Drop old constraint if present (safe: only if exists)
    if _has_uc("jurisdiction_profiles", "uq_jp_org_key_effective"):
        with op.batch_alter_table("jurisdiction_profiles") as batch:
            batch.drop_constraint("uq_jp_org_key_effective", type_="unique")

    # Add correct constraint if missing
    if not _has_uc("jurisdiction_profiles", "uq_jp_scope_state_county_city"):
        with op.batch_alter_table("jurisdiction_profiles") as batch:
            batch.create_unique_constraint(
                "uq_jp_scope_state_county_city",
                ["org_id", "state", "county", "city"],
            )

    # 3) Optional: add index on state for faster resolve (safe add)
    # (If it already exists, Postgres will throw; so we only create if missing.)
    idx_names = {i["name"] for i in _insp().get_indexes("jurisdiction_profiles")}
    if "ix_jurisdiction_profiles_state" not in idx_names:
        op.create_index("ix_jurisdiction_profiles_state", "jurisdiction_profiles", ["state"])


def downgrade() -> None:
    # conservative
    pass