# backend/app/alembic/versions/0075_expand_policy_foundation_for_trustworthy_projection.py
"""expand policy foundation for trustworthy projection

Revision ID: 0075_expand_policy_foundation_for_trustworthy_projection
Revises: 0074_add_compliance_rule_projection_foundation
Create Date: 2026-04-08
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0075_expand_policy_foundation_for_trustworthy_projection"
down_revision = "0074_add_compliance_rule_projection_foundation"
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
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        cols = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _index_exists(index_name: str, table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        indexes = inspector.get_indexes(table_name)
    except Exception:
        return False
    return any(str(idx.get("name")) == index_name for idx in indexes)


def _fk_exists(table_name: str, fk_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        fks = inspector.get_foreign_keys(table_name)
    except Exception:
        return False
    return any(str(fk.get("name")) == fk_name for fk in fks)


def _add_column(table_name: str, column: sa.Column) -> None:
    if _table_exists(table_name) and not _column_exists(table_name, str(column.name)):
        op.add_column(table_name, column)


def _create_index(index_name: str, table_name: str, columns: list[str], *, unique: bool = False) -> None:
    if _table_exists(table_name) and not _index_exists(index_name, table_name):
        op.create_index(index_name, table_name, columns, unique=unique)


def _create_fk(
    fk_name: str,
    source_table: str,
    referent_table: str,
    local_cols: list[str],
    remote_cols: list[str],
    *,
    ondelete: str | None = None,
) -> None:
    if _table_exists(source_table) and _table_exists(referent_table) and not _fk_exists(source_table, fk_name):
        op.create_foreign_key(
            fk_name,
            source_table,
            referent_table,
            local_cols,
            remote_cols,
            ondelete=ondelete,
        )


def upgrade() -> None:
    if _table_exists("policy_sources"):
        _add_column("policy_sources", sa.Column("next_refresh_due_at", sa.DateTime(), nullable=True))
        _add_column("policy_sources", sa.Column("last_fetch_error", sa.Text(), nullable=True))
        _add_column("policy_sources", sa.Column("last_http_status", sa.Integer(), nullable=True))
        _add_column("policy_sources", sa.Column("last_seen_same_fingerprint_at", sa.DateTime(), nullable=True))
        _add_column(
            "policy_sources",
            sa.Column("source_metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column("policy_sources", sa.Column("last_verified_by_user_id", sa.Integer(), nullable=True))

        _create_index(
            "ix_policy_sources_status_type",
            "policy_sources",
            ["registry_status", "source_type"],
        )
        _create_index(
            "ix_policy_sources_next_refresh_due_at",
            "policy_sources",
            ["next_refresh_due_at"],
        )

    if _table_exists("policy_assertions"):
        _add_column("policy_assertions", sa.Column("source_version_id", sa.Integer(), nullable=True))
        _create_fk(
            "fk_policy_assertions_source_version_id",
            "policy_assertions",
            "policy_source_versions",
            ["source_version_id"],
            ["id"],
            ondelete="SET NULL",
        )

        _add_column("policy_assertions", sa.Column("replaced_by_assertion_id", sa.Integer(), nullable=True))
        _create_fk(
            "fk_policy_assertions_replaced_by_assertion_id",
            "policy_assertions",
            "policy_assertions",
            ["replaced_by_assertion_id"],
            ["id"],
            ondelete="SET NULL",
        )

        _add_column(
            "policy_assertions",
            sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("citation_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("rule_provenance_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column("policy_assertions", sa.Column("value_hash", sa.String(length=64), nullable=True))
        _add_column("policy_assertions", sa.Column("confidence_basis", sa.String(length=80), nullable=True))
        _add_column("policy_assertions", sa.Column("change_summary", sa.Text(), nullable=True))
        _add_column("policy_assertions", sa.Column("approved_at", sa.DateTime(), nullable=True))
        _add_column("policy_assertions", sa.Column("approved_by_user_id", sa.Integer(), nullable=True))
        _add_column("policy_assertions", sa.Column("activated_at", sa.DateTime(), nullable=True))
        _add_column("policy_assertions", sa.Column("activated_by_user_id", sa.Integer(), nullable=True))
        _add_column("policy_assertions", sa.Column("replaced_at", sa.DateTime(), nullable=True))

        _create_index(
            "ix_policy_assertions_version_group_number",
            "policy_assertions",
            ["version_group", "version_number"],
        )
        _create_index("ix_policy_assertions_source_version_id", "policy_assertions", ["source_version_id"])
        _create_index("ix_policy_assertions_is_current", "policy_assertions", ["is_current"])

    if _table_exists("property_compliance_projections"):
        _add_column(
            "property_compliance_projections",
            sa.Column("evidence_gap_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "property_compliance_projections",
            sa.Column("confirmed_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "property_compliance_projections",
            sa.Column("inferred_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "property_compliance_projections",
            sa.Column("failing_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "property_compliance_projections",
            sa.Column("source_confidence_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "property_compliance_projections",
            sa.Column("projection_reason_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column("property_compliance_projections", sa.Column("rules_effective_at", sa.DateTime(), nullable=True))
        _add_column("property_compliance_projections", sa.Column("last_rule_change_at", sa.DateTime(), nullable=True))

        _create_index(
            "ix_property_compliance_projections_last_rule_change_at",
            "property_compliance_projections",
            ["last_rule_change_at"],
        )

    if _table_exists("property_compliance_projection_items"):
        _add_column(
            "property_compliance_projection_items",
            sa.Column("proof_state", sa.String(length=40), nullable=False, server_default=sa.text("'unknown'")),
        )
        _add_column("property_compliance_projection_items", sa.Column("status_reason", sa.Text(), nullable=True))
        _add_column("property_compliance_projection_items", sa.Column("source_citation", sa.Text(), nullable=True))
        _add_column("property_compliance_projection_items", sa.Column("raw_excerpt", sa.Text(), nullable=True))
        _add_column("property_compliance_projection_items", sa.Column("rule_value_json", sa.Text(), nullable=True))
        _add_column(
            "property_compliance_projection_items",
            sa.Column("conflicting_evidence_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        _add_column(
            "property_compliance_projection_items",
            sa.Column("required_document_kind", sa.String(length=80), nullable=True),
        )
        _add_column("property_compliance_projection_items", sa.Column("last_evaluated_at", sa.DateTime(), nullable=True))
        _add_column("property_compliance_projection_items", sa.Column("evidence_updated_at", sa.DateTime(), nullable=True))

        _create_index(
            "ix_property_compliance_projection_items_proof_state",
            "property_compliance_projection_items",
            ["proof_state"],
        )

    if _table_exists("property_compliance_evidence"):
        _add_column(
            "property_compliance_evidence",
            sa.Column("evidence_category", sa.String(length=80), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("confidence", sa.Float(), nullable=False, server_default=sa.text("0")),
        )
        _add_column("property_compliance_evidence", sa.Column("verified_at", sa.DateTime(), nullable=True))
        _add_column("property_compliance_evidence", sa.Column("verified_by_user_id", sa.Integer(), nullable=True))
        _add_column("property_compliance_evidence", sa.Column("invalidated_at", sa.DateTime(), nullable=True))
        _add_column("property_compliance_evidence", sa.Column("invalidated_reason", sa.Text(), nullable=True))
        _add_column(
            "property_compliance_evidence",
            sa.Column("metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )

        _create_index(
            "ix_property_compliance_evidence_verified_at",
            "property_compliance_evidence",
            ["verified_at"],
        )


def downgrade() -> None:
    # Conservative downgrade on purpose.
    # This migration is additive and supports live data. Dropping these columns could
    # destroy operator-entered governance/evidence history in real environments.
    pass