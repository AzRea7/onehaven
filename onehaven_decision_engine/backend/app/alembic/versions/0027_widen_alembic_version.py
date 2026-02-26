"""widen alembic_version.version_num so long revision ids work

Revision ID: 0027_widen_alembic_version
Revises: 0026_add_limits_json_to_plans
Create Date: 2026-02-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0027_widen_alembic_version"
down_revision = "0026_add_limits_json_to_plans"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Postgres: widen column so revision ids like
    # "0006_compliance_and_rent_policy_fields" can be stored
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=128),
        existing_nullable=False,
    )


def downgrade() -> None:
    # Best-effort shrink back (may fail if current value > 32)
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=128),
        type_=sa.String(length=32),
        existing_nullable=False,
    )