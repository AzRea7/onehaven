"""property state stage index + last_transitioned_at

Revision ID: 0047_prop_stage_idx_ts
Revises: 0046_policy_governance_and_coverage
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0047_prop_stage_idx_ts"
down_revision = "0046_policy_gov_cov"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _cols(table: str) -> set[str]:
    if not _has_table(table):
        return set()
    return {c["name"] for c in _insp().get_columns(table)}


def _indexes(table: str) -> set[str]:
    if not _has_table(table):
        return set()
    return {i["name"] for i in _insp().get_indexes(table)}


def upgrade() -> None:
    if not _has_table("property_states"):
        return

    cols = _cols("property_states")
    idxs = _indexes("property_states")

    # 1) add last_transitioned_at
    if "last_transitioned_at" not in cols:
        op.add_column(
            "property_states",
            sa.Column("last_transitioned_at", sa.DateTime(), nullable=True),
        )

    # 2) backfill from updated_at so existing rows have sane transition timestamps
    if "last_transitioned_at" in _cols("property_states"):
        op.execute(
            """
            UPDATE property_states
            SET last_transitioned_at = updated_at
            WHERE last_transitioned_at IS NULL
            """
        )

    # 3) add multitenant stage lookup index
    if "ix_property_states_org_stage" not in idxs:
        op.create_index(
            "ix_property_states_org_stage",
            "property_states",
            ["org_id", "current_stage"],
            unique=False,
        )

    # 4) optional single-column current_stage index because model now has index=True
    # guard it so reruns or slightly different DB states do not explode
    if "ix_property_states_current_stage" not in _indexes("property_states"):
        op.create_index(
            "ix_property_states_current_stage",
            "property_states",
            ["current_stage"],
            unique=False,
        )


def downgrade() -> None:
    if not _has_table("property_states"):
        return

    idxs = _indexes("property_states")
    cols = _cols("property_states")

    if "ix_property_states_current_stage" in idxs:
        op.drop_index("ix_property_states_current_stage", table_name="property_states")

    if "ix_property_states_org_stage" in idxs:
        op.drop_index("ix_property_states_org_stage", table_name="property_states")

    if "last_transitioned_at" in cols:
        op.drop_column("property_states", "last_transitioned_at")