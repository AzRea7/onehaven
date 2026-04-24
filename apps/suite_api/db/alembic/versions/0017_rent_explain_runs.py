# backend/app/alembic/versions/0017_rent_explain_runs.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0017_rent_explain_runs"
down_revision = "0016_import_snapshots_org_scope"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rent_explain_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        # index=True already creates ix_rent_explain_runs_org_id
        sa.Column(
            "org_id",
            sa.Integer(),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        # index=True already creates ix_rent_explain_runs_property_id
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("strategy", sa.String(length=20), nullable=False, server_default="section8"),
        sa.Column("cap_reason", sa.String(length=32), nullable=True),
        sa.Column("explain_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("decision_version", sa.String(length=64), nullable=False, server_default="unknown"),
        sa.Column("payment_standard_pct_used", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    # ✅ DO NOT create indexes again here.
    # The two indexes are already created by index=True on the columns.


def downgrade() -> None:
    # Dropping the table drops its indexes too.
    op.drop_table("rent_explain_runs")
