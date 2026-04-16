from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0080_add_policy_refresh_state_machine"
down_revision = "0079_add_policy_source_inventory_foundation"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        return table_name in inspector.get_table_names()
    except Exception:
        return False


def _column_exists(table_name: str, column_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        cols = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _index_exists(index_name: str, table_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        indexes = inspector.get_indexes(table_name)
    except Exception:
        return False
    return any(str(idx.get("name")) == index_name for idx in indexes)


def _add_column(table_name: str, column: sa.Column) -> None:
    if _table_exists(table_name) and not _column_exists(table_name, str(column.name)):
        op.add_column(table_name, column)


def _drop_column(table_name: str, column_name: str) -> None:
    if _table_exists(table_name) and _column_exists(table_name, column_name):
        op.drop_column(table_name, column_name)


def _create_index(index_name: str, table_name: str, columns: list[str], *, unique: bool = False) -> None:
    if _table_exists(table_name) and not _index_exists(index_name, table_name):
        op.create_index(index_name, table_name, columns, unique=unique)


def _drop_index(index_name: str, table_name: str) -> None:
    if _table_exists(table_name) and _index_exists(index_name, table_name):
        op.drop_index(index_name, table_name=table_name)


def upgrade() -> None:
    if _table_exists("jurisdiction_profiles"):
        _add_column(
            "jurisdiction_profiles",
            sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("refresh_status_reason", sa.String(length=255), nullable=True),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("refresh_blocked_reason", sa.Text(), nullable=True),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("last_refresh_state_transition_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("last_refresh_completed_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("refresh_requirements_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("refresh_retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("current_refresh_run_id", sa.String(length=120), nullable=True),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("last_refresh_changed_source_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "jurisdiction_profiles",
            sa.Column("last_refresh_changed_rule_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )

        _create_index("ix_jp_refresh_state", "jurisdiction_profiles", ["refresh_state"])
        _create_index("ix_jp_last_refresh_completed_at", "jurisdiction_profiles", ["last_refresh_completed_at"])

    if _table_exists("policy_sources"):
        _add_column(
            "policy_sources",
            sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("refresh_status_reason", sa.String(length=255), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("refresh_blocked_reason", sa.Text(), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("last_refresh_attempt_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("last_refresh_completed_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("last_state_transition_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("last_change_summary_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("revalidation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )
        _add_column(
            "policy_sources",
            sa.Column("validation_due_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_sources",
            sa.Column("refresh_retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "policy_sources",
            sa.Column("current_refresh_run_id", sa.String(length=120), nullable=True),
        )

        _create_index("ix_policy_sources_refresh_state", "policy_sources", ["refresh_state"])
        _create_index("ix_policy_sources_validation_due_at", "policy_sources", ["validation_due_at"])

    if _table_exists("policy_source_inventory"):
        _add_column(
            "policy_source_inventory",
            sa.Column("refresh_state", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("refresh_status_reason", sa.String(length=255), nullable=True),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("next_refresh_step", sa.String(length=80), nullable=True),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("revalidation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("validation_due_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("last_change_detected_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("last_refresh_outcome_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("last_change_summary_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("last_search_retry_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("next_search_retry_due_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "policy_source_inventory",
            sa.Column("last_state_transition_at", sa.DateTime(), nullable=True),
        )

        _create_index("ix_policy_source_inventory_refresh_state", "policy_source_inventory", ["refresh_state"])
        _create_index("ix_policy_source_inventory_validation_due_at", "policy_source_inventory", ["validation_due_at"])
        _create_index(
            "ix_policy_source_inventory_next_search_retry_due_at",
            "policy_source_inventory",
            ["next_search_retry_due_at"],
        )

    if _table_exists("policy_discovery_attempts"):
        _add_column(
            "policy_discovery_attempts",
            sa.Column("next_retry_due_at", sa.DateTime(), nullable=True),
        )
        _create_index(
            "ix_policy_discovery_attempts_next_retry_due_at",
            "policy_discovery_attempts",
            ["next_retry_due_at"],
        )


def downgrade() -> None:
    _drop_index("ix_policy_discovery_attempts_next_retry_due_at", "policy_discovery_attempts")
    _drop_column("policy_discovery_attempts", "next_retry_due_at")

    _drop_index("ix_policy_source_inventory_next_search_retry_due_at", "policy_source_inventory")
    _drop_index("ix_policy_source_inventory_validation_due_at", "policy_source_inventory")
    _drop_index("ix_policy_source_inventory_refresh_state", "policy_source_inventory")
    _drop_column("policy_source_inventory", "last_state_transition_at")
    _drop_column("policy_source_inventory", "next_search_retry_due_at")
    _drop_column("policy_source_inventory", "last_search_retry_at")
    _drop_column("policy_source_inventory", "last_change_summary_json")
    _drop_column("policy_source_inventory", "last_refresh_outcome_json")
    _drop_column("policy_source_inventory", "last_change_detected_at")
    _drop_column("policy_source_inventory", "validation_due_at")
    _drop_column("policy_source_inventory", "revalidation_required")
    _drop_column("policy_source_inventory", "next_refresh_step")
    _drop_column("policy_source_inventory", "refresh_status_reason")
    _drop_column("policy_source_inventory", "refresh_state")

    _drop_index("ix_policy_sources_validation_due_at", "policy_sources")
    _drop_index("ix_policy_sources_refresh_state", "policy_sources")
    _drop_column("policy_sources", "current_refresh_run_id")
    _drop_column("policy_sources", "refresh_retry_count")
    _drop_column("policy_sources", "validation_due_at")
    _drop_column("policy_sources", "revalidation_required")
    _drop_column("policy_sources", "last_change_summary_json")
    _drop_column("policy_sources", "last_refresh_outcome_json")
    _drop_column("policy_sources", "last_state_transition_at")
    _drop_column("policy_sources", "last_refresh_completed_at")
    _drop_column("policy_sources", "last_refresh_attempt_at")
    _drop_column("policy_sources", "refresh_blocked_reason")
    _drop_column("policy_sources", "refresh_status_reason")
    _drop_column("policy_sources", "refresh_state")

    _drop_index("ix_jp_last_refresh_completed_at", "jurisdiction_profiles")
    _drop_index("ix_jp_refresh_state", "jurisdiction_profiles")
    _drop_column("jurisdiction_profiles", "last_refresh_changed_rule_count")
    _drop_column("jurisdiction_profiles", "last_refresh_changed_source_count")
    _drop_column("jurisdiction_profiles", "current_refresh_run_id")
    _drop_column("jurisdiction_profiles", "refresh_retry_count")
    _drop_column("jurisdiction_profiles", "refresh_requirements_json")
    _drop_column("jurisdiction_profiles", "last_refresh_outcome_json")
    _drop_column("jurisdiction_profiles", "last_refresh_completed_at")
    _drop_column("jurisdiction_profiles", "last_refresh_state_transition_at")
    _drop_column("jurisdiction_profiles", "refresh_blocked_reason")
    _drop_column("jurisdiction_profiles", "refresh_status_reason")
    _drop_column("jurisdiction_profiles", "refresh_state")