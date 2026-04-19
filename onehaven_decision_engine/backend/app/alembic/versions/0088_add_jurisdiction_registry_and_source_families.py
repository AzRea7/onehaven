from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0088_add_jurisdiction_registry_and_source_families"
down_revision = "0087_fix_hqs_rules_schema_drift"
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
    if not _has_table("jurisdiction_registry"):
        op.create_table(
            "jurisdiction_registry",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=True),
            sa.Column("jurisdiction_type", sa.String(length=32), nullable=False),
            sa.Column("state_code", sa.String(length=8), nullable=True),
            sa.Column("state_name", sa.String(length=120), nullable=True),
            sa.Column("county_name", sa.String(length=160), nullable=True),
            sa.Column("city_name", sa.String(length=160), nullable=True),
            sa.Column("display_name", sa.String(length=220), nullable=False),
            sa.Column("slug", sa.String(length=240), nullable=False),
            sa.Column("geoid", sa.String(length=32), nullable=True),
            sa.Column("lsad", sa.String(length=32), nullable=True),
            sa.Column("census_class", sa.String(length=64), nullable=True),
            sa.Column("parent_jurisdiction_id", sa.Integer(), nullable=True),
            sa.Column("official_website", sa.String(length=500), nullable=True),
            sa.Column("onboarding_status", sa.String(length=64), nullable=False, server_default="discovered"),
            sa.Column("source_confidence", sa.Float(), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("last_reviewed_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(
                ["parent_jurisdiction_id"],
                ["jurisdiction_registry.id"],
                name="fk_jurisdiction_registry_parent",
            ),
        )

    if not _has_uc("jurisdiction_registry", "uq_jurisdiction_registry_slug"):
        with op.batch_alter_table("jurisdiction_registry") as batch:
            batch.create_unique_constraint("uq_jurisdiction_registry_slug", ["slug"])

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_geoid"):
        op.create_index("ix_jurisdiction_registry_geoid", "jurisdiction_registry", ["geoid"])

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_state_code"):
        op.create_index("ix_jurisdiction_registry_state_code", "jurisdiction_registry", ["state_code"])

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_parent_jurisdiction_id"):
        op.create_index(
            "ix_jurisdiction_registry_parent_jurisdiction_id",
            "jurisdiction_registry",
            ["parent_jurisdiction_id"],
        )

    if not _has_table("jurisdiction_source_families"):
        op.create_table(
            "jurisdiction_source_families",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("jurisdiction_id", sa.Integer(), nullable=False),
            sa.Column("category", sa.String(length=80), nullable=False),
            sa.Column("source_label", sa.String(length=220), nullable=True),
            sa.Column("source_url", sa.String(length=1000), nullable=True),
            sa.Column("source_kind", sa.String(length=40), nullable=True),
            sa.Column("publisher_name", sa.String(length=220), nullable=True),
            sa.Column("publisher_type", sa.String(length=80), nullable=True),
            sa.Column("authority_level", sa.String(length=64), nullable=True),
            sa.Column("fetch_mode", sa.String(length=40), nullable=True),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="active"),
            sa.Column("is_official", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("coverage_hint", sa.String(length=220), nullable=True),
            sa.Column("review_state", sa.String(length=40), nullable=True),
            sa.Column("last_checked_at", sa.DateTime(), nullable=True),
            sa.Column("last_reviewed_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(
                ["jurisdiction_id"],
                ["jurisdiction_registry.id"],
                name="fk_jurisdiction_source_families_jurisdiction_id",
                ondelete="CASCADE",
            ),
        )

    if not _has_index("jurisdiction_source_families", "ix_jsf_jurisdiction_id"):
        op.create_index("ix_jsf_jurisdiction_id", "jurisdiction_source_families", ["jurisdiction_id"])

    if not _has_index("jurisdiction_source_families", "ix_jsf_category"):
        op.create_index("ix_jsf_category", "jurisdiction_source_families", ["category"])

    if not _has_index("jurisdiction_source_families", "ix_jsf_status"):
        op.create_index("ix_jsf_status", "jurisdiction_source_families", ["status"])

    if not _has_uc("jurisdiction_source_families", "uq_jsf_jurisdiction_category_source_url"):
        with op.batch_alter_table("jurisdiction_source_families") as batch:
            batch.create_unique_constraint(
                "uq_jsf_jurisdiction_category_source_url",
                ["jurisdiction_id", "category", "source_url"],
            )


def downgrade() -> None:
    # Conservative downgrade: leave data intact.
    pass