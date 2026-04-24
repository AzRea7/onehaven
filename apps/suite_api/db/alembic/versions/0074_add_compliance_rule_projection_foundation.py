# backend/app/alembic/versions/0074_add_compliance_rule_projection_foundation.py
"""add compliance rule projection foundation

Revision ID: 0074_add_compliance_rule_projection_foundation
Revises: 0073_add_compliance_documents
Create Date: 2026-04-07
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0074_add_compliance_rule_projection_foundation"
down_revision = "0073_add_compliance_documents"
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
    if not _column_exists(table_name, str(column.name)):
        op.add_column(table_name, column)


def _drop_column(table_name: str, column_name: str) -> None:
    if _column_exists(table_name, column_name):
        op.drop_column(table_name, column_name)


def _create_index(index_name: str, table_name: str, columns: list[str], *, unique: bool = False) -> None:
    if not _index_exists(index_name, table_name):
        op.create_index(index_name, table_name, columns, unique=unique)


def _drop_index(index_name: str, table_name: str) -> None:
    if _index_exists(index_name, table_name):
        op.drop_index(index_name, table_name=table_name)


def _create_fk(
    fk_name: str,
    source_table: str,
    referent_table: str,
    local_cols: list[str],
    remote_cols: list[str],
    *,
    ondelete: str | None = None,
) -> None:
    if not _fk_exists(source_table, fk_name):
        op.create_foreign_key(
            fk_name,
            source_table,
            referent_table,
            local_cols,
            remote_cols,
            ondelete=ondelete,
        )


def _drop_fk(table_name: str, fk_name: str) -> None:
    if _fk_exists(table_name, fk_name):
        op.drop_constraint(fk_name, table_name, type_="foreignkey")


def upgrade() -> None:
    if _table_exists("policy_sources"):
        _add_column("policy_sources", sa.Column("source_name", sa.String(length=255), nullable=True))
        _add_column(
            "policy_sources",
            sa.Column("source_type", sa.String(length=40), nullable=False, server_default=sa.text("'local'")),
        )
        _add_column("policy_sources", sa.Column("jurisdiction_slug", sa.String(length=160), nullable=True))
        _add_column(
            "policy_sources",
            sa.Column("fetch_method", sa.String(length=40), nullable=False, server_default=sa.text("'manual'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("trust_level", sa.Float(), nullable=False, server_default=sa.text("0.5")),
        )
        _add_column(
            "policy_sources",
            sa.Column("refresh_interval_days", sa.Integer(), nullable=False, server_default=sa.text("30")),
        )
        _add_column("policy_sources", sa.Column("last_fetched_at", sa.DateTime(), nullable=True))
        _add_column(
            "policy_sources",
            sa.Column("registry_status", sa.String(length=40), nullable=False, server_default=sa.text("'active'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("fetch_config_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("registry_meta_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        )
        _add_column(
            "policy_sources",
            sa.Column("fingerprint_algo", sa.String(length=40), nullable=False, server_default=sa.text("'sha256'")),
        )
        _add_column("policy_sources", sa.Column("current_fingerprint", sa.String(length=128), nullable=True))
        _add_column("policy_sources", sa.Column("last_changed_at", sa.DateTime(), nullable=True))

        _create_index("ix_policy_sources_jurisdiction_slug", "policy_sources", ["jurisdiction_slug"])
        _create_index("ix_policy_sources_registry_status", "policy_sources", ["registry_status"])

    if _table_exists("policy_assertions"):
        _add_column("policy_assertions", sa.Column("jurisdiction_slug", sa.String(length=160), nullable=True))
        _add_column(
            "policy_assertions",
            sa.Column("source_level", sa.String(length=40), nullable=False, server_default=sa.text("'local'")),
        )
        _add_column("policy_assertions", sa.Column("property_type", sa.String(length=64), nullable=True))
        _add_column("policy_assertions", sa.Column("rule_category", sa.String(length=80), nullable=True))
        _add_column(
            "policy_assertions",
            sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("blocking", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )
        _add_column("policy_assertions", sa.Column("source_citation", sa.Text(), nullable=True))
        _add_column("policy_assertions", sa.Column("raw_excerpt", sa.Text(), nullable=True))
        _add_column(
            "policy_assertions",
            sa.Column("normalized_version", sa.String(length=40), nullable=False, server_default=sa.text("'v1'")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("rule_status", sa.String(length=40), nullable=False, server_default=sa.text("'candidate'")),
        )
        _add_column(
            "policy_assertions",
            sa.Column("governance_state", sa.String(length=40), nullable=False, server_default=sa.text("'draft'")),
        )
        _add_column("policy_assertions", sa.Column("version_group", sa.String(length=120), nullable=True))
        _add_column(
            "policy_assertions",
            sa.Column("version_number", sa.Integer(), nullable=False, server_default=sa.text("1")),
        )

        _create_index("ix_policy_assertions_jurisdiction_slug", "policy_assertions", ["jurisdiction_slug"])
        _create_index("ix_policy_assertions_governance_state", "policy_assertions", ["governance_state"])
        _create_index("ix_policy_assertions_rule_status", "policy_assertions", ["rule_status"])
        _create_index("ix_policy_assertions_rule_category", "policy_assertions", ["rule_category"])

    if not _table_exists("property_compliance_projections"):
        op.create_table(
            "property_compliance_projections",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
            sa.Column("jurisdiction_slug", sa.String(length=160), nullable=True),
            sa.Column("program_type", sa.String(length=40), nullable=True),
            sa.Column("rules_version", sa.String(length=64), nullable=False, server_default=sa.text("'v1'")),
            sa.Column("projection_status", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
            sa.Column("projection_basis_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("blocking_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("unknown_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("stale_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("conflicting_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("readiness_score", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("projected_compliance_cost", sa.Float(), nullable=True),
            sa.Column("projected_days_to_rent", sa.Integer(), nullable=True),
            sa.Column("confidence_score", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("impacted_rules_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("unresolved_evidence_gaps_json", sa.Text(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("last_projected_at", sa.DateTime(), nullable=True),
            sa.Column("superseded_at", sa.DateTime(), nullable=True),
            sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )

    _create_index(
        "ix_property_compliance_projections_org_property",
        "property_compliance_projections",
        ["org_id", "property_id"],
    )
    _create_index(
        "ix_property_compliance_projections_rules_version",
        "property_compliance_projections",
        ["rules_version"],
    )
    _create_index(
        "ix_property_compliance_projections_is_current",
        "property_compliance_projections",
        ["is_current"],
    )
    _create_index(
        "ix_property_compliance_projections_jurisdiction_slug",
        "property_compliance_projections",
        ["jurisdiction_slug"],
    )

    if not _table_exists("property_compliance_projection_items"):
        op.create_table(
            "property_compliance_projection_items",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
            sa.Column(
                "projection_id",
                sa.Integer(),
                sa.ForeignKey("property_compliance_projections.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
            sa.Column(
                "policy_assertion_id",
                sa.Integer(),
                sa.ForeignKey("policy_assertions.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("jurisdiction_slug", sa.String(length=160), nullable=True),
            sa.Column("program_type", sa.String(length=40), nullable=True),
            sa.Column("property_type", sa.String(length=64), nullable=True),
            sa.Column("source_level", sa.String(length=40), nullable=True),
            sa.Column("rule_key", sa.String(length=120), nullable=False),
            sa.Column("rule_category", sa.String(length=80), nullable=True),
            sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("blocking", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("evaluation_status", sa.String(length=40), nullable=False, server_default=sa.text("'unknown'")),
            sa.Column("evidence_status", sa.String(length=40), nullable=False, server_default=sa.text("'unknown'")),
            sa.Column("confidence", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("estimated_cost", sa.Float(), nullable=True),
            sa.Column("estimated_days", sa.Integer(), nullable=True),
            sa.Column("evidence_summary", sa.Text(), nullable=True),
            sa.Column("evidence_gap", sa.Text(), nullable=True),
            sa.Column("resolution_detail_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )

    _create_index(
        "ix_property_compliance_projection_items_projection",
        "property_compliance_projection_items",
        ["projection_id"],
    )
    _create_index(
        "ix_property_compliance_projection_items_org_property",
        "property_compliance_projection_items",
        ["org_id", "property_id"],
    )
    _create_index(
        "ix_property_compliance_projection_items_rule_key",
        "property_compliance_projection_items",
        ["rule_key"],
    )
    _create_index(
        "ix_property_compliance_projection_items_evaluation_status",
        "property_compliance_projection_items",
        ["evaluation_status"],
    )

    if not _table_exists("property_compliance_evidence"):
        op.create_table(
            "property_compliance_evidence",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
            sa.Column(
                "projection_item_id",
                sa.Integer(),
                sa.ForeignKey("property_compliance_projection_items.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column(
                "policy_assertion_id",
                sa.Integer(),
                sa.ForeignKey("policy_assertions.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column(
                "compliance_document_id",
                sa.Integer(),
                sa.ForeignKey("compliance_documents.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("inspection_id", sa.Integer(), sa.ForeignKey("inspections.id", ondelete="SET NULL"), nullable=True),
            sa.Column(
                "checklist_item_id",
                sa.Integer(),
                sa.ForeignKey("property_checklist_items.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("evidence_source_type", sa.String(length=40), nullable=False, server_default=sa.text("'document'")),
            sa.Column("evidence_key", sa.String(length=160), nullable=True),
            sa.Column("evidence_name", sa.String(length=255), nullable=True),
            sa.Column("evidence_status", sa.String(length=40), nullable=False, server_default=sa.text("'unknown'")),
            sa.Column("proof_state", sa.String(length=40), nullable=False, server_default=sa.text("'inferred'")),
            sa.Column("satisfies_rule", sa.Boolean(), nullable=True),
            sa.Column("observed_at", sa.DateTime(), nullable=True),
            sa.Column("expires_at", sa.DateTime(), nullable=True),
            sa.Column("resolved_at", sa.DateTime(), nullable=True),
            sa.Column("source_details_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )

    _create_index(
        "ix_property_compliance_evidence_org_property",
        "property_compliance_evidence",
        ["org_id", "property_id"],
    )
    _create_index(
        "ix_property_compliance_evidence_projection_item",
        "property_compliance_evidence",
        ["projection_item_id"],
    )
    _create_index(
        "ix_property_compliance_evidence_policy_assertion",
        "property_compliance_evidence",
        ["policy_assertion_id"],
    )
    _create_index(
        "ix_property_compliance_evidence_status",
        "property_compliance_evidence",
        ["evidence_status", "proof_state"],
    )
    _create_index(
        "ix_property_compliance_evidence_expires_at",
        "property_compliance_evidence",
        ["expires_at"],
    )


def downgrade() -> None:
    _drop_index("ix_property_compliance_evidence_expires_at", "property_compliance_evidence")
    _drop_index("ix_property_compliance_evidence_status", "property_compliance_evidence")
    _drop_index("ix_property_compliance_evidence_policy_assertion", "property_compliance_evidence")
    _drop_index("ix_property_compliance_evidence_projection_item", "property_compliance_evidence")
    _drop_index("ix_property_compliance_evidence_org_property", "property_compliance_evidence")
    if _table_exists("property_compliance_evidence"):
        op.drop_table("property_compliance_evidence")

    _drop_index("ix_property_compliance_projection_items_evaluation_status", "property_compliance_projection_items")
    _drop_index("ix_property_compliance_projection_items_rule_key", "property_compliance_projection_items")
    _drop_index("ix_property_compliance_projection_items_org_property", "property_compliance_projection_items")
    _drop_index("ix_property_compliance_projection_items_projection", "property_compliance_projection_items")
    if _table_exists("property_compliance_projection_items"):
        op.drop_table("property_compliance_projection_items")

    _drop_index("ix_property_compliance_projections_jurisdiction_slug", "property_compliance_projections")
    _drop_index("ix_property_compliance_projections_is_current", "property_compliance_projections")
    _drop_index("ix_property_compliance_projections_rules_version", "property_compliance_projections")
    _drop_index("ix_property_compliance_projections_org_property", "property_compliance_projections")
    if _table_exists("property_compliance_projections"):
        op.drop_table("property_compliance_projections")

    _drop_index("ix_policy_assertions_rule_category", "policy_assertions")
    _drop_index("ix_policy_assertions_rule_status", "policy_assertions")
    _drop_index("ix_policy_assertions_governance_state", "policy_assertions")
    _drop_index("ix_policy_assertions_jurisdiction_slug", "policy_assertions")
    for col in [
        "version_number",
        "version_group",
        "governance_state",
        "rule_status",
        "normalized_version",
        "raw_excerpt",
        "source_citation",
        "blocking",
        "required",
        "rule_category",
        "property_type",
        "source_level",
        "jurisdiction_slug",
    ]:
        _drop_column("policy_assertions", col)

    _drop_index("ix_policy_sources_registry_status", "policy_sources")
    _drop_index("ix_policy_sources_jurisdiction_slug", "policy_sources")
    for col in [
        "last_changed_at",
        "current_fingerprint",
        "fingerprint_algo",
        "registry_meta_json",
        "fetch_config_json",
        "registry_status",
        "last_fetched_at",
        "refresh_interval_days",
        "trust_level",
        "fetch_method",
        "jurisdiction_slug",
        "source_type",
        "source_name",
    ]:
        _drop_column("policy_sources", col)