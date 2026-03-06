"""policy governance + coverage status + source versions

Revision ID: 0046_policy_governance_and_coverage
Revises: 0045_add_policy_sources_and_assertions
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0046_policy_governance_and_coverage"
down_revision = "0045_add_policy_sources_and_assertions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "policy_assertions",
        sa.Column("assertion_type", sa.String(length=40), nullable=False, server_default="document_reference"),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("rule_family", sa.String(length=80), nullable=True),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("effective_date", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("expires_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("source_rank", sa.Integer(), nullable=False, server_default="100"),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("reviewed_by_user_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("verification_reason", sa.String(length=80), nullable=True),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("stale_after", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "policy_assertions",
        sa.Column("superseded_by_assertion_id", sa.Integer(), nullable=True),
    )

    op.create_foreign_key(
        "fk_policy_assertions_superseded_by",
        "policy_assertions",
        "policy_assertions",
        ["superseded_by_assertion_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_index(
        "ix_policy_assertions_rule_family",
        "policy_assertions",
        ["rule_family"],
    )
    op.create_index(
        "ix_policy_assertions_assertion_type",
        "policy_assertions",
        ["assertion_type"],
    )
    op.create_index(
        "ix_policy_assertions_stale_after",
        "policy_assertions",
        ["stale_after"],
    )

    op.create_table(
        "policy_source_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("policy_sources.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("retrieved_at", sa.DateTime(), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("content_sha256", sa.String(length=64), nullable=True),
        sa.Column("raw_path", sa.Text(), nullable=True),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("fetch_error", sa.Text(), nullable=True),
        sa.Column("extracted_text", sa.Text(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index(
        "ix_policy_source_versions_source_retrieved",
        "policy_source_versions",
        ["source_id", "retrieved_at"],
    )

    op.create_table(
        "jurisdiction_coverage_status",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=True, index=True),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("pha_name", sa.String(length=180), nullable=True),
        sa.Column("coverage_status", sa.String(length=40), nullable=False, server_default="not_started"),
        sa.Column("production_readiness", sa.String(length=40), nullable=False, server_default="partial"),
        sa.Column("last_reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("last_source_refresh_at", sa.DateTime(), nullable=True),
        sa.Column("verified_rule_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("fetch_failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stale_warning_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_jurisdiction_coverage_scope",
        "jurisdiction_coverage_status",
        ["state", "county", "city"],
    )
    op.create_index(
        "ix_jurisdiction_coverage_status",
        "jurisdiction_coverage_status",
        ["coverage_status", "production_readiness"],
    )


def downgrade() -> None:
    op.drop_index("ix_jurisdiction_coverage_status", table_name="jurisdiction_coverage_status")
    op.drop_index("ix_jurisdiction_coverage_scope", table_name="jurisdiction_coverage_status")
    op.drop_table("jurisdiction_coverage_status")

    op.drop_index("ix_policy_source_versions_source_retrieved", table_name="policy_source_versions")
    op.drop_table("policy_source_versions")

    op.drop_index("ix_policy_assertions_stale_after", table_name="policy_assertions")
    op.drop_index("ix_policy_assertions_assertion_type", table_name="policy_assertions")
    op.drop_index("ix_policy_assertions_rule_family", table_name="policy_assertions")
    op.drop_constraint("fk_policy_assertions_superseded_by", "policy_assertions", type_="foreignkey")

    op.drop_column("policy_assertions", "superseded_by_assertion_id")
    op.drop_column("policy_assertions", "stale_after")
    op.drop_column("policy_assertions", "verification_reason")
    op.drop_column("policy_assertions", "reviewed_by_user_id")
    op.drop_column("policy_assertions", "source_rank")
    op.drop_column("policy_assertions", "priority")
    op.drop_column("policy_assertions", "expires_at")
    op.drop_column("policy_assertions", "effective_date")
    op.drop_column("policy_assertions", "rule_family")
    op.drop_column("policy_assertions", "assertion_type")
    