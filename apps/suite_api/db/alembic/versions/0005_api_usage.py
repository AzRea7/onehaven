"""api_usage table for external API call budgeting

Revision ID: XXXX_api_usage
Revises: <PUT_PREVIOUS_REVISION_ID_HERE>
Create Date: 2026-02-10
"""

from alembic import op
import sqlalchemy as sa

revision = "0005_api_usage"
down_revision = "0004_rent_learning"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api_usage",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(length=50), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("calls", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("provider", "day", name="uq_api_usage_provider_day"),
    )


def downgrade() -> None:
    op.drop_table("api_usage")
