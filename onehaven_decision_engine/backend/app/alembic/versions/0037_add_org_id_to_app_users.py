# onehaven_decision_engine/backend/app/alembic/versions/0037_add_org_id_to_app_users.py
"""
0037 add org_id to app_users

Revision ID: 0037_add_org_id_to_app_users
Revises: 0036_add_geo_risk_workflow_pipeline
Create Date: 2026-03-04
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0037_add_org_id_to_app_users"
down_revision = "0036_add_geo_risk_workflow_pipeline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add nullable org_id so we don't break existing users.
    op.add_column("app_users", sa.Column("org_id", sa.Integer(), nullable=True))

    # Index for tenant-scoped lookups
    op.create_index("ix_app_users_org_id", "app_users", ["org_id"], unique=False)

    # FK to organizations; SET NULL on org deletion since org_id is nullable in model.
    op.create_foreign_key(
        "fk_app_users_org_id_organizations",
        "app_users",
        "organizations",
        ["org_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    # Drop FK then index then column (reverse order).
    op.drop_constraint("fk_app_users_org_id_organizations", "app_users", type_="foreignkey")
    op.drop_index("ix_app_users_org_id", table_name="app_users")
    op.drop_column("app_users", "org_id")