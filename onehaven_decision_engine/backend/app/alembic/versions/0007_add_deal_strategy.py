"""add deal strategy

Revision ID: 0007_add_deal_strategy
Revises: 0006_compliance_and_rent_policy_fields
Create Date: 2026-02-10
"""

from alembic import op
import sqlalchemy as sa


revision = "0007_add_deal_strategy"
down_revision = "0006_compliance_and_rent_policy_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("deals", sa.Column("strategy", sa.String(length=20), nullable=False, server_default="section8"))
    op.execute("UPDATE deals SET strategy = 'section8' WHERE strategy IS NULL")
    op.alter_column("deals", "strategy", server_default=None)


def downgrade() -> None:
    op.drop_column("deals", "strategy")
