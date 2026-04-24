"""add policy override ledger

Revision ID: 0082_add_policy_override_ledger
Revises: 0081_add_policy_assertion_validation_gate
Create Date: 2026-04-15
"""
from alembic import op
import sqlalchemy as sa


revision = "0082_add_policy_override_ledger"
down_revision = "0081_add_policy_assertion_validation_gate"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _index_exists(table_name: str, index_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(str(idx.get("name")) == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    if not _table_exists("policy_override_ledger"):
        op.create_table(
            "policy_override_ledger",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=True),
            sa.Column("jurisdiction_profile_id", sa.Integer(), nullable=True),
            sa.Column("assertion_id", sa.Integer(), nullable=True),
            sa.Column("state", sa.String(length=2), nullable=True),
            sa.Column("county", sa.String(length=80), nullable=True),
            sa.Column("city", sa.String(length=120), nullable=True),
            sa.Column("pha_name", sa.String(length=180), nullable=True),
            sa.Column("program_type", sa.String(length=40), nullable=True),
            sa.Column("override_scope", sa.String(length=40), nullable=False, server_default="jurisdiction"),
            sa.Column("override_type", sa.String(length=40), nullable=False, server_default="interim_operational_override"),
            sa.Column("rule_key", sa.String(length=120), nullable=True),
            sa.Column("rule_category", sa.String(length=80), nullable=True),
            sa.Column("severity", sa.String(length=40), nullable=False, server_default="medium"),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("carrying_critical_rule", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("trust_impact", sa.String(length=40), nullable=False, server_default="review_required"),
            sa.Column("reason", sa.Text(), nullable=False),
            sa.Column("linked_evidence_json", sa.Text(), nullable=False, server_default="[]"),
            sa.Column("metadata_json", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("approved_by_user_id", sa.Integer(), nullable=True),
            sa.Column("expires_at", sa.DateTime(), nullable=True),
            sa.Column("revoked_at", sa.DateTime(), nullable=True),
            sa.Column("revoked_reason", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(["jurisdiction_profile_id"], ["jurisdiction_profiles.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["assertion_id"], ["policy_assertions.id"], ondelete="SET NULL"),
        )
    if not _index_exists("policy_override_ledger", "ix_policy_override_ledger_scope"):
        op.create_index("ix_policy_override_ledger_scope", "policy_override_ledger", ["state", "county", "city"], unique=False)
    if not _index_exists("policy_override_ledger", "ix_policy_override_ledger_profile"):
        op.create_index("ix_policy_override_ledger_profile", "policy_override_ledger", ["jurisdiction_profile_id"], unique=False)
    if not _index_exists("policy_override_ledger", "ix_policy_override_ledger_active"):
        op.create_index("ix_policy_override_ledger_active", "policy_override_ledger", ["is_active", "expires_at"], unique=False)
    if not _index_exists("policy_override_ledger", "ix_policy_override_ledger_rule_key"):
        op.create_index("ix_policy_override_ledger_rule_key", "policy_override_ledger", ["rule_key"], unique=False)
    if not _index_exists("policy_override_ledger", "ix_policy_override_ledger_rule_category"):
        op.create_index("ix_policy_override_ledger_rule_category", "policy_override_ledger", ["rule_category"], unique=False)
    if not _index_exists("policy_override_ledger", "ix_policy_override_ledger_severity"):
        op.create_index("ix_policy_override_ledger_severity", "policy_override_ledger", ["severity"], unique=False)


def downgrade() -> None:
    if _table_exists("policy_override_ledger"):
        for idx in [
            "ix_policy_override_ledger_severity",
            "ix_policy_override_ledger_rule_category",
            "ix_policy_override_ledger_rule_key",
            "ix_policy_override_ledger_active",
            "ix_policy_override_ledger_profile",
            "ix_policy_override_ledger_scope",
        ]:
            if _index_exists("policy_override_ledger", idx):
                op.drop_index(idx, table_name="policy_override_ledger")
        op.drop_table("policy_override_ledger")
