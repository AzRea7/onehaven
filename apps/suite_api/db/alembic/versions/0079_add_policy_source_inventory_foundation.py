from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0079_add_policy_source_inventory_foundation"
down_revision = "0078_add_policy_source_authority_tiering_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "policy_source_inventory",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("pha_name", sa.String(length=180), nullable=True),
        sa.Column("program_type", sa.String(length=40), nullable=True),
        sa.Column("scope_key", sa.String(length=255), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("domain_name", sa.String(length=255), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=True),
        sa.Column("publisher", sa.String(length=255), nullable=True),
        sa.Column("source_type", sa.String(length=80), nullable=True),
        sa.Column("publication_type", sa.String(length=80), nullable=True),
        sa.Column("policy_source_id", sa.Integer(), nullable=True),
        sa.Column("current_source_version_id", sa.Integer(), nullable=True),
        sa.Column("lifecycle_state", sa.String(length=40), nullable=False, server_default=sa.text("'discovered'")),
        sa.Column("crawl_status", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("inventory_origin", sa.String(length=40), nullable=False, server_default=sa.text("'discovered'")),
        sa.Column("candidate_origin_type", sa.String(length=80), nullable=True),
        sa.Column("candidate_status_reason", sa.String(length=255), nullable=True),
        sa.Column("is_curated", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_official_candidate", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("dedupe_key", sa.String(length=128), nullable=True),
        sa.Column("canonical_fingerprint", sa.String(length=128), nullable=True),
        sa.Column("fingerprint_algo", sa.String(length=40), nullable=False, server_default=sa.text("'sha256'")),
        sa.Column("authority_tier", sa.String(length=40), nullable=True),
        sa.Column("authority_rank", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("authority_score", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.Column("authority_use_type", sa.String(length=40), nullable=False, server_default=sa.text("'weak'")),
        sa.Column("authority_policy_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("expected_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("expected_tiers_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("category_hints_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("search_terms_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("discovered_at", sa.DateTime(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(), nullable=True),
        sa.Column("last_crawled_at", sa.DateTime(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(), nullable=True),
        sa.Column("next_crawl_due_at", sa.DateTime(), nullable=True),
        sa.Column("last_http_status", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("searched_not_found_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("rejected_at", sa.DateTime(), nullable=True),
        sa.Column("superseded_at", sa.DateTime(), nullable=True),
        sa.Column("inventory_metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("refresh_status_reason", sa.String(length=255), nullable=True),
        sa.Column("next_refresh_step", sa.String(length=80), nullable=True),
        sa.Column("revalidation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("validation_due_at", sa.DateTime(), nullable=True),
        sa.Column("last_change_detected_at", sa.DateTime(), nullable=True),
        sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("last_change_summary_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("last_search_retry_at", sa.DateTime(), nullable=True),
        sa.Column("next_search_retry_due_at", sa.DateTime(), nullable=True),
        sa.Column("last_state_transition_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["policy_source_id"], ["policy_sources.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["current_source_version_id"], ["policy_source_versions.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("org_id", "scope_key", "canonical_url", name="uq_policy_source_inventory_scope_url"),
    )
    op.create_index("ix_policy_source_inventory_scope", "policy_source_inventory", ["org_id", "state", "county", "city"])
    op.create_index("ix_policy_source_inventory_scope_key", "policy_source_inventory", ["scope_key"])
    op.create_index("ix_policy_source_inventory_lifecycle", "policy_source_inventory", ["lifecycle_state"])
    op.create_index("ix_policy_source_inventory_status", "policy_source_inventory", ["crawl_status"])
    op.create_index("ix_policy_source_inventory_next_crawl_due", "policy_source_inventory", ["next_crawl_due_at"])
    op.create_index("ix_policy_source_inventory_source_id", "policy_source_inventory", ["policy_source_id"])
    op.create_index("ix_policy_source_inventory_domain", "policy_source_inventory", ["domain_name"])
    op.create_index("ix_policy_source_inventory_refresh_state", "policy_source_inventory", ["refresh_state"])
    op.create_index("ix_policy_source_inventory_validation_due_at", "policy_source_inventory", ["validation_due_at"])
    op.create_index("ix_policy_source_inventory_next_search_retry_due_at", "policy_source_inventory", ["next_search_retry_due_at"])

    op.create_table(
        "policy_discovery_attempts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=True),
        sa.Column("inventory_id", sa.Integer(), nullable=True),
        sa.Column("policy_source_id", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("pha_name", sa.String(length=180), nullable=True),
        sa.Column("program_type", sa.String(length=40), nullable=True),
        sa.Column("scope_key", sa.String(length=255), nullable=True),
        sa.Column("attempt_type", sa.String(length=40), nullable=False, server_default=sa.text("'discovery'")),
        sa.Column("status", sa.String(length=40), nullable=False, server_default=sa.text("'started'")),
        sa.Column("query_text", sa.Text(), nullable=True),
        sa.Column("searched_categories_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("searched_tiers_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("result_urls_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("not_found", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("next_retry_due_at", sa.DateTime(), nullable=True),
        sa.Column("attempt_metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["inventory_id"], ["policy_source_inventory.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["policy_source_id"], ["policy_sources.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_policy_discovery_attempts_scope", "policy_discovery_attempts", ["org_id", "state", "county", "city"])
    op.create_index("ix_policy_discovery_attempts_status", "policy_discovery_attempts", ["status"])
    op.create_index("ix_policy_discovery_attempts_started", "policy_discovery_attempts", ["started_at"])
    op.create_index("ix_policy_discovery_attempts_inventory", "policy_discovery_attempts", ["inventory_id"])
    op.create_index("ix_policy_discovery_attempts_next_retry_due_at", "policy_discovery_attempts", ["next_retry_due_at"])


def downgrade() -> None:
    op.drop_index("ix_policy_discovery_attempts_next_retry_due_at", table_name="policy_discovery_attempts")
    op.drop_index("ix_policy_discovery_attempts_inventory", table_name="policy_discovery_attempts")
    op.drop_index("ix_policy_discovery_attempts_started", table_name="policy_discovery_attempts")
    op.drop_index("ix_policy_discovery_attempts_status", table_name="policy_discovery_attempts")
    op.drop_index("ix_policy_discovery_attempts_scope", table_name="policy_discovery_attempts")
    op.drop_table("policy_discovery_attempts")

    op.drop_index("ix_policy_source_inventory_next_search_retry_due_at", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_validation_due_at", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_refresh_state", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_domain", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_source_id", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_next_crawl_due", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_status", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_lifecycle", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_scope_key", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_scope", table_name="policy_source_inventory")
    op.drop_table("policy_source_inventory")
