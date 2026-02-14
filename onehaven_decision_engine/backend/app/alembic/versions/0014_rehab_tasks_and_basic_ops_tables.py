from alembic import op
import sqlalchemy as sa

revision = "0014_rehab_tasks_and_basic_ops_tables"
down_revision = "0013_property_checklist_items_and_org_scoping"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade():
    conn = op.get_bind()

    if not _has_table(conn, "rehab_tasks"):
        op.create_table(
            "rehab_tasks",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False, index=True),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("title", sa.String(255), nullable=False),
            sa.Column("category", sa.String(60), nullable=False, server_default="rehab"),
            sa.Column("inspection_relevant", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("status", sa.String(20), nullable=False, server_default="todo"),
            sa.Column("cost_estimate", sa.Float(), nullable=True),
            sa.Column("vendor", sa.String(160), nullable=True),
            sa.Column("deadline", sa.DateTime(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("org_id", "property_id", "title", name="uq_rehab_tasks_org_property_title"),
        )

    if not _has_table(conn, "tenants"):
        op.create_table(
            "tenants",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False, index=True),
            sa.Column("full_name", sa.String(200), nullable=False),
            sa.Column("phone", sa.String(40), nullable=True),
            sa.Column("email", sa.String(200), nullable=True),
            sa.Column("voucher_status", sa.String(80), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("org_id", "full_name", name="uq_tenants_org_name"),
        )

    if not _has_table(conn, "leases"):
        op.create_table(
            "leases",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False, index=True),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("start_date", sa.DateTime(), nullable=False),
            sa.Column("end_date", sa.DateTime(), nullable=True),
            sa.Column("total_rent", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("tenant_portion", sa.Float(), nullable=True),
            sa.Column("housing_authority_portion", sa.Float(), nullable=True),
            sa.Column("hap_contract_status", sa.String(80), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )

    if not _has_table(conn, "transactions"):
        op.create_table(
            "transactions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False, index=True),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("txn_date", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("txn_type", sa.String(80), nullable=False, server_default="other"),
            sa.Column("amount", sa.Float(), nullable=False),
            sa.Column("memo", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )

    if not _has_table(conn, "valuations"):
        op.create_table(
            "valuations",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False, index=True),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("as_of", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("estimated_value", sa.Float(), nullable=False),
            sa.Column("loan_balance", sa.Float(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        )


def downgrade():
    op.drop_table("valuations")
    op.drop_table("transactions")
    op.drop_table("leases")
    op.drop_table("tenants")
    op.drop_table("rehab_tasks")
