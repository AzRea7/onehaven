from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0089_add_nspire_rule_catalog"
down_revision = "0088_add_jurisdiction_registry_and_source_families"
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
    if not _has_table("nspire_rule_catalog"):
        op.create_table(
            "nspire_rule_catalog",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("rule_key", sa.String(length=240), nullable=False),
            sa.Column("template_key", sa.String(length=80), nullable=False, server_default="nspire_hcv"),
            sa.Column("template_version", sa.String(length=80), nullable=False, server_default="nspire_hcv_2026"),
            sa.Column("source_name", sa.String(length=220), nullable=False, server_default="HUD NSPIRE HCV Checklist"),
            sa.Column("standard_code", sa.String(length=120), nullable=False),
            sa.Column("standard_label", sa.String(length=220), nullable=False),
            sa.Column("deficiency_description", sa.Text(), nullable=False),
            sa.Column("severity_code", sa.String(length=8), nullable=False),
            sa.Column("severity_label", sa.String(length=40), nullable=False),
            sa.Column("correction_days", sa.Integer(), nullable=True),
            sa.Column("pass_fail", sa.String(length=16), nullable=False),
            sa.Column("inspectable_area", sa.String(length=120), nullable=True),
            sa.Column("location_scope", sa.String(length=120), nullable=True),
            sa.Column("citation", sa.String(length=240), nullable=True),
            sa.Column("source_url", sa.String(length=1000), nullable=True),
            sa.Column("effective_date", sa.Date(), nullable=False, server_default=sa.text("'2026-01-01'::date")),
            sa.Column("is_hcv_applicable", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )

    if not _has_uc("nspire_rule_catalog", "uq_nspire_rule_catalog_template_rule"):
        with op.batch_alter_table("nspire_rule_catalog") as batch:
            batch.create_unique_constraint(
                "uq_nspire_rule_catalog_template_rule",
                ["template_key", "template_version", "rule_key"],
            )

    if not _has_index("nspire_rule_catalog", "ix_nspire_rule_catalog_standard_code"):
        op.create_index(
            "ix_nspire_rule_catalog_standard_code",
            "nspire_rule_catalog",
            ["standard_code"],
        )

    if not _has_index("nspire_rule_catalog", "ix_nspire_rule_catalog_template_key"):
        op.create_index(
            "ix_nspire_rule_catalog_template_key",
            "nspire_rule_catalog",
            ["template_key"],
        )

    if not _has_index("nspire_rule_catalog", "ix_nspire_rule_catalog_severity_label"):
        op.create_index(
            "ix_nspire_rule_catalog_severity_label",
            "nspire_rule_catalog",
            ["severity_label"],
        )


def downgrade() -> None:
    # Conservative downgrade: leave data intact.
    pass