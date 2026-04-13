"""add policy assertion validation gate

Revision ID: 0081_add_policy_assertion_validation_gate
Revises: 0080_add_policy_refresh_state_machine
Create Date: 2026-04-13
"""
from alembic import op
import sqlalchemy as sa

revision = "0081_add_policy_assertion_validation_gate"
down_revision = "0080_add_policy_refresh_state_machine"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("policy_assertions", sa.Column("validation_state", sa.String(length=40), nullable=False, server_default="pending"))
    op.add_column("policy_assertions", sa.Column("validation_score", sa.Float(), nullable=False, server_default="0"))
    op.add_column("policy_assertions", sa.Column("validation_reason", sa.String(length=255), nullable=True))
    op.add_column("policy_assertions", sa.Column("trust_state", sa.String(length=40), nullable=False, server_default="extracted"))
    op.add_column("policy_assertions", sa.Column("validated_at", sa.DateTime(), nullable=True))
    op.create_index("ix_policy_assertions_validation_state", "policy_assertions", ["validation_state"])
    op.create_index("ix_policy_assertions_trust_state", "policy_assertions", ["trust_state"])


def downgrade() -> None:
    op.drop_index("ix_policy_assertions_trust_state", table_name="policy_assertions")
    op.drop_index("ix_policy_assertions_validation_state", table_name="policy_assertions")
    op.drop_column("policy_assertions", "validated_at")
    op.drop_column("policy_assertions", "trust_state")
    op.drop_column("policy_assertions", "validation_reason")
    op.drop_column("policy_assertions", "validation_score")
    op.drop_column("policy_assertions", "validation_state")
