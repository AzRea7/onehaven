from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0080_add_policy_refresh_state_machine"
down_revision = "0079_add_policy_source_inventory_foundation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("jurisdiction_profiles") as batch_op:
        batch_op.add_column(sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default="pending"))
        batch_op.add_column(sa.Column("refresh_status_reason", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("refresh_blocked_reason", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_state_transition_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_completed_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default="{}"))
        batch_op.add_column(sa.Column("refresh_requirements_json", sa.Text(), nullable=False, server_default="{}"))
        batch_op.add_column(sa.Column("refresh_retry_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("current_refresh_run_id", sa.String(length=120), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_changed_source_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("last_refresh_changed_rule_count", sa.Integer(), nullable=False, server_default="0"))
    op.create_index("ix_jp_refresh_state", "jurisdiction_profiles", ["refresh_state"])
    op.create_index("ix_jp_last_refresh_completed_at", "jurisdiction_profiles", ["last_refresh_completed_at"])

    with op.batch_alter_table("policy_sources") as batch_op:
        batch_op.add_column(sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default="pending"))
        batch_op.add_column(sa.Column("refresh_status_reason", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("refresh_blocked_reason", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_attempt_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_completed_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_state_transition_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default="{}"))
        batch_op.add_column(sa.Column("last_change_summary_json", sa.Text(), nullable=False, server_default="{}"))
        batch_op.add_column(sa.Column("revalidation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")))
        batch_op.add_column(sa.Column("validation_due_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("refresh_retry_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("current_refresh_run_id", sa.String(length=120), nullable=True))
    op.create_index("ix_policy_sources_refresh_state", "policy_sources", ["refresh_state"])
    op.create_index("ix_policy_sources_validation_due_at", "policy_sources", ["validation_due_at"])

    with op.batch_alter_table("policy_source_inventory") as batch_op:
        batch_op.add_column(sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default="pending"))
        batch_op.add_column(sa.Column("refresh_status_reason", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("next_refresh_step", sa.String(length=80), nullable=True))
        batch_op.add_column(sa.Column("revalidation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")))
        batch_op.add_column(sa.Column("validation_due_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_change_detected_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default="{}"))
        batch_op.add_column(sa.Column("last_change_summary_json", sa.Text(), nullable=False, server_default="{}"))
        batch_op.add_column(sa.Column("last_search_retry_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("next_search_retry_due_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_state_transition_at", sa.DateTime(), nullable=True))
    op.create_index("ix_policy_source_inventory_refresh_state", "policy_source_inventory", ["refresh_state"])
    op.create_index("ix_policy_source_inventory_validation_due_at", "policy_source_inventory", ["validation_due_at"])
    op.create_index("ix_policy_source_inventory_next_search_retry_due_at", "policy_source_inventory", ["next_search_retry_due_at"])

    with op.batch_alter_table("policy_discovery_attempts") as batch_op:
        batch_op.add_column(sa.Column("next_retry_due_at", sa.DateTime(), nullable=True))
    op.create_index("ix_policy_discovery_attempts_next_retry_due_at", "policy_discovery_attempts", ["next_retry_due_at"])


def downgrade() -> None:
    op.drop_index("ix_policy_discovery_attempts_next_retry_due_at", table_name="policy_discovery_attempts")
    with op.batch_alter_table("policy_discovery_attempts") as batch_op:
        batch_op.drop_column("next_retry_due_at")

    op.drop_index("ix_policy_source_inventory_next_search_retry_due_at", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_validation_due_at", table_name="policy_source_inventory")
    op.drop_index("ix_policy_source_inventory_refresh_state", table_name="policy_source_inventory")
    with op.batch_alter_table("policy_source_inventory") as batch_op:
        batch_op.drop_column("last_state_transition_at")
        batch_op.drop_column("next_search_retry_due_at")
        batch_op.drop_column("last_search_retry_at")
        batch_op.drop_column("last_change_summary_json")
        batch_op.drop_column("last_refresh_outcome_json")
        batch_op.drop_column("last_change_detected_at")
        batch_op.drop_column("validation_due_at")
        batch_op.drop_column("revalidation_required")
        batch_op.drop_column("next_refresh_step")
        batch_op.drop_column("refresh_status_reason")
        batch_op.drop_column("refresh_state")

    op.drop_index("ix_policy_sources_validation_due_at", table_name="policy_sources")
    op.drop_index("ix_policy_sources_refresh_state", table_name="policy_sources")
    with op.batch_alter_table("policy_sources") as batch_op:
        batch_op.drop_column("current_refresh_run_id")
        batch_op.drop_column("refresh_retry_count")
        batch_op.drop_column("validation_due_at")
        batch_op.drop_column("revalidation_required")
        batch_op.drop_column("last_change_summary_json")
        batch_op.drop_column("last_refresh_outcome_json")
        batch_op.drop_column("last_state_transition_at")
        batch_op.drop_column("last_refresh_completed_at")
        batch_op.drop_column("last_refresh_attempt_at")
        batch_op.drop_column("refresh_blocked_reason")
        batch_op.drop_column("refresh_status_reason")
        batch_op.drop_column("refresh_state")

    op.drop_index("ix_jp_last_refresh_completed_at", table_name="jurisdiction_profiles")
    op.drop_index("ix_jp_refresh_state", table_name="jurisdiction_profiles")
    with op.batch_alter_table("jurisdiction_profiles") as batch_op:
        batch_op.drop_column("last_refresh_changed_rule_count")
        batch_op.drop_column("last_refresh_changed_source_count")
        batch_op.drop_column("current_refresh_run_id")
        batch_op.drop_column("refresh_retry_count")
        batch_op.drop_column("refresh_requirements_json")
        batch_op.drop_column("last_refresh_outcome_json")
        batch_op.drop_column("last_refresh_completed_at")
        batch_op.drop_column("last_refresh_state_transition_at")
        batch_op.drop_column("refresh_blocked_reason")
        batch_op.drop_column("refresh_status_reason")
        batch_op.drop_column("refresh_state")
