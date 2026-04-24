# backend/app/alembic/versions/0076_add_property_evidence_proof_fields.py
"""add property evidence proof fields

Revision ID: 0076_add_property_evidence_proof_fields
Revises: 0075_expand_policy_foundation_for_trustworthy_projection
Create Date: 2026-04-09
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0076_add_property_evidence_proof_fields"
down_revision = "0075_expand_policy_foundation_for_trustworthy_projection"
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


def _drop_index(index_name: str, table_name: str) -> None:
    if _table_exists(table_name) and _index_exists(index_name, table_name):
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
    if _table_exists(source_table) and _table_exists(referent_table) and not _fk_exists(source_table, fk_name):
        op.create_foreign_key(
            fk_name,
            source_table,
            referent_table,
            local_cols,
            remote_cols,
            ondelete=ondelete,
        )


def _drop_fk(table_name: str, fk_name: str) -> None:
    if _table_exists(table_name) and _fk_exists(table_name, fk_name):
        op.drop_constraint(fk_name, table_name, type_="foreignkey")


def upgrade() -> None:
    if _table_exists("property_compliance_projection_items"):
        _add_column(
            "property_compliance_projection_items",
            sa.Column("required_evidence_type", sa.String(length=80), nullable=True),
        )
        _add_column(
            "property_compliance_projection_items",
            sa.Column("required_evidence_key", sa.String(length=160), nullable=True),
        )
        _add_column(
            "property_compliance_projection_items",
            sa.Column("required_evidence_group", sa.String(length=120), nullable=True),
        )
        _add_column(
            "property_compliance_projection_items",
            sa.Column(
                "proof_requirement_level",
                sa.String(length=40),
                nullable=False,
                server_default=sa.text("'standard'"),
            ),
        )
        _add_column(
            "property_compliance_projection_items",
            sa.Column("proof_validity_days", sa.Integer(), nullable=True),
        )
        _create_index(
            "ix_property_compliance_projection_items_required_evidence_key",
            "property_compliance_projection_items",
            ["required_evidence_key"],
        )

    if _table_exists("property_compliance_evidence"):
        _add_column(
            "property_compliance_evidence",
            sa.Column("jurisdiction_slug", sa.String(length=160), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("program_type", sa.String(length=40), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("rule_key", sa.String(length=120), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("rule_category", sa.String(length=80), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("document_kind", sa.String(length=80), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("issuing_authority", sa.String(length=160), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("reference_number", sa.String(length=160), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("line_item_key", sa.String(length=160), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("line_item_label", sa.String(length=255), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("line_item_status", sa.String(length=40), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("severity", sa.String(length=40), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("remediation_status", sa.String(length=40), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("remediation_due_at", sa.DateTime(), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column("superseded_by_evidence_id", sa.Integer(), nullable=True),
        )
        _add_column(
            "property_compliance_evidence",
            sa.Column(
                "is_current",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("true"),
            ),
        )

        _create_fk(
            "fk_property_compliance_evidence_superseded_by_evidence_id",
            "property_compliance_evidence",
            "property_compliance_evidence",
            ["superseded_by_evidence_id"],
            ["id"],
            ondelete="SET NULL",
        )
        _create_index(
            "ix_property_compliance_evidence_rule_key",
            "property_compliance_evidence",
            ["rule_key"],
        )
        _create_index(
            "ix_property_compliance_evidence_reference_number",
            "property_compliance_evidence",
            ["reference_number"],
        )
        _create_index(
            "ix_property_compliance_evidence_current",
            "property_compliance_evidence",
            ["is_current", "invalidated_at"],
        )

    if not _table_exists("property_compliance_evidence_facts"):
        op.create_table(
            "property_compliance_evidence_facts",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False),
            sa.Column("property_id", sa.Integer(), nullable=False),
            sa.Column("evidence_id", sa.Integer(), nullable=False),
            sa.Column("projection_item_id", sa.Integer(), nullable=True),
            sa.Column("inspection_id", sa.Integer(), nullable=True),
            sa.Column("checklist_item_id", sa.Integer(), nullable=True),
            sa.Column("rule_key", sa.String(length=120), nullable=True),
            sa.Column("fact_key", sa.String(length=160), nullable=False),
            sa.Column("fact_label", sa.String(length=255), nullable=True),
            sa.Column(
                "fact_type",
                sa.String(length=40),
                nullable=False,
                server_default=sa.text("'status'"),
            ),
            sa.Column("fact_value", sa.Text(), nullable=True),
            sa.Column(
                "fact_status",
                sa.String(length=40),
                nullable=False,
                server_default=sa.text("'observed'"),
            ),
            sa.Column(
                "proof_state",
                sa.String(length=40),
                nullable=False,
                server_default=sa.text("'inferred'"),
            ),
            sa.Column("severity", sa.String(length=40), nullable=True),
            sa.Column("satisfies_rule", sa.Boolean(), nullable=True),
            sa.Column("observed_at", sa.DateTime(), nullable=True),
            sa.Column("expires_at", sa.DateTime(), nullable=True),
            sa.Column("resolved_at", sa.DateTime(), nullable=True),
            sa.Column(
                "source_details_json",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'{}'"),
            ),
            sa.Column(
                "metadata_json",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'{}'"),
            ),
            sa.Column(
                "created_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.text("now()"),
            ),
        )
        _create_fk(
            "fk_property_compliance_evidence_facts_org_id",
            "property_compliance_evidence_facts",
            "organizations",
            ["org_id"],
            ["id"],
        )
        _create_fk(
            "fk_property_compliance_evidence_facts_property_id",
            "property_compliance_evidence_facts",
            "properties",
            ["property_id"],
            ["id"],
            ondelete="CASCADE",
        )
        _create_fk(
            "fk_property_compliance_evidence_facts_evidence_id",
            "property_compliance_evidence_facts",
            "property_compliance_evidence",
            ["evidence_id"],
            ["id"],
            ondelete="CASCADE",
        )
        _create_fk(
            "fk_property_compliance_evidence_facts_projection_item_id",
            "property_compliance_evidence_facts",
            "property_compliance_projection_items",
            ["projection_item_id"],
            ["id"],
            ondelete="SET NULL",
        )
        _create_fk(
            "fk_property_compliance_evidence_facts_inspection_id",
            "property_compliance_evidence_facts",
            "inspections",
            ["inspection_id"],
            ["id"],
            ondelete="SET NULL",
        )
        _create_fk(
            "fk_property_compliance_evidence_facts_checklist_item_id",
            "property_compliance_evidence_facts",
            "property_checklist_items",
            ["checklist_item_id"],
            ["id"],
            ondelete="SET NULL",
        )
        _create_index(
            "ix_property_compliance_evidence_facts_evidence",
            "property_compliance_evidence_facts",
            ["evidence_id"],
        )
        _create_index(
            "ix_property_compliance_evidence_facts_projection_item",
            "property_compliance_evidence_facts",
            ["projection_item_id"],
        )
        _create_index(
            "ix_property_compliance_evidence_facts_org_property",
            "property_compliance_evidence_facts",
            ["org_id", "property_id"],
        )
        _create_index(
            "ix_property_compliance_evidence_facts_rule_key",
            "property_compliance_evidence_facts",
            ["rule_key"],
        )
        _create_index(
            "ix_property_compliance_evidence_facts_status",
            "property_compliance_evidence_facts",
            ["fact_status", "proof_state"],
        )


def downgrade() -> None:
    _drop_index(
        "ix_property_compliance_projection_items_required_evidence_key",
        "property_compliance_projection_items",
    )

    if _table_exists("property_compliance_evidence_facts"):
        _drop_index("ix_property_compliance_evidence_facts_status", "property_compliance_evidence_facts")
        _drop_index("ix_property_compliance_evidence_facts_rule_key", "property_compliance_evidence_facts")
        _drop_index("ix_property_compliance_evidence_facts_org_property", "property_compliance_evidence_facts")
        _drop_index("ix_property_compliance_evidence_facts_projection_item", "property_compliance_evidence_facts")
        _drop_index("ix_property_compliance_evidence_facts_evidence", "property_compliance_evidence_facts")
        _drop_fk("property_compliance_evidence_facts", "fk_property_compliance_evidence_facts_checklist_item_id")
        _drop_fk("property_compliance_evidence_facts", "fk_property_compliance_evidence_facts_inspection_id")
        _drop_fk("property_compliance_evidence_facts", "fk_property_compliance_evidence_facts_projection_item_id")
        _drop_fk("property_compliance_evidence_facts", "fk_property_compliance_evidence_facts_evidence_id")
        _drop_fk("property_compliance_evidence_facts", "fk_property_compliance_evidence_facts_property_id")
        _drop_fk("property_compliance_evidence_facts", "fk_property_compliance_evidence_facts_org_id")
        op.drop_table("property_compliance_evidence_facts")

    _drop_index("ix_property_compliance_evidence_current", "property_compliance_evidence")
    _drop_index("ix_property_compliance_evidence_reference_number", "property_compliance_evidence")
    _drop_index("ix_property_compliance_evidence_rule_key", "property_compliance_evidence")
    _drop_fk(
        "property_compliance_evidence",
        "fk_property_compliance_evidence_superseded_by_evidence_id",
    )

    for table_name, column_name in [
        ("property_compliance_evidence", "is_current"),
        ("property_compliance_evidence", "superseded_by_evidence_id"),
        ("property_compliance_evidence", "remediation_due_at"),
        ("property_compliance_evidence", "remediation_status"),
        ("property_compliance_evidence", "severity"),
        ("property_compliance_evidence", "line_item_status"),
        ("property_compliance_evidence", "line_item_label"),
        ("property_compliance_evidence", "line_item_key"),
        ("property_compliance_evidence", "reference_number"),
        ("property_compliance_evidence", "issuing_authority"),
        ("property_compliance_evidence", "document_kind"),
        ("property_compliance_evidence", "rule_category"),
        ("property_compliance_evidence", "rule_key"),
        ("property_compliance_evidence", "program_type"),
        ("property_compliance_evidence", "jurisdiction_slug"),
        ("property_compliance_projection_items", "proof_validity_days"),
        ("property_compliance_projection_items", "proof_requirement_level"),
        ("property_compliance_projection_items", "required_evidence_group"),
        ("property_compliance_projection_items", "required_evidence_key"),
        ("property_compliance_projection_items", "required_evidence_type"),
    ]:
        if _table_exists(table_name) and _column_exists(table_name, column_name):
            op.drop_column(table_name, column_name)