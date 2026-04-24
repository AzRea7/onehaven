from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0090_add_property_compliance_resolution_snapshots"
down_revision = "0089_add_nspire_rule_catalog"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_index(table: str, idx_name: str) -> bool:
    if not _has_table(table):
        return False
    return idx_name in {idx["name"] for idx in _insp().get_indexes(table)}


def _has_uc(table: str, uc_name: str) -> bool:
    if not _has_table(table):
        return False
    return uc_name in {uc["name"] for uc in _insp().get_unique_constraints(table)}


def upgrade() -> None:
    if not _has_table("property_compliance_resolution_snapshots"):
        op.create_table(
            "property_compliance_resolution_snapshots",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=True),
            sa.Column("property_id", sa.Integer(), nullable=False),
            sa.Column("jurisdiction_id", sa.Integer(), nullable=True),
            sa.Column("jurisdiction_profile_id", sa.Integer(), nullable=True),
            sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("trust_status", sa.String(length=40), nullable=False, server_default="unknown"),
            sa.Column("resolution_status", sa.String(length=40), nullable=False, server_default="pending"),
            sa.Column("safe_to_rely_on", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("missing_categories_json", sa.Text(), nullable=True),
            sa.Column("stale_categories_json", sa.Text(), nullable=True),
            sa.Column("blocked_categories_json", sa.Text(), nullable=True),
            sa.Column("source_family_summary_json", sa.Text(), nullable=True),
            sa.Column("applied_rule_refs_json", sa.Text(), nullable=True),
            sa.Column("unresolved_items_json", sa.Text(), nullable=True),
            sa.Column("evidence_summary_json", sa.Text(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("recompute_reason", sa.String(length=200), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(
                ["property_id"],
                ["properties.id"],
                name="fk_pcrs_property_id",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["jurisdiction_id"],
                ["jurisdiction_registry.id"],
                name="fk_pcrs_jurisdiction_id",
            ),
            sa.ForeignKeyConstraint(
                ["jurisdiction_profile_id"],
                ["jurisdiction_profiles.id"],
                name="fk_pcrs_jurisdiction_profile_id",
            ),
        )

    if not _has_uc("property_compliance_resolution_snapshots", "uq_pcrs_property_version"):
        with op.batch_alter_table("property_compliance_resolution_snapshots") as batch:
            batch.create_unique_constraint(
                "uq_pcrs_property_version",
                ["property_id", "version"],
            )

    if not _has_index("property_compliance_resolution_snapshots", "ix_pcrs_property_id"):
        op.create_index(
            "ix_pcrs_property_id",
            "property_compliance_resolution_snapshots",
            ["property_id"],
        )

    if not _has_index("property_compliance_resolution_snapshots", "ix_pcrs_jurisdiction_id"):
        op.create_index(
            "ix_pcrs_jurisdiction_id",
            "property_compliance_resolution_snapshots",
            ["jurisdiction_id"],
        )

    if not _has_index("property_compliance_resolution_snapshots", "ix_pcrs_resolution_status"):
        op.create_index(
            "ix_pcrs_resolution_status",
            "property_compliance_resolution_snapshots",
            ["resolution_status"],
        )

    if not _has_index("property_compliance_resolution_snapshots", "ix_pcrs_trust_status"):
        op.create_index(
            "ix_pcrs_trust_status",
            "property_compliance_resolution_snapshots",
            ["trust_status"],
        )


def downgrade() -> None:
    # Conservative downgrade: leave data intact.
    pass