from alembic import op
import sqlalchemy as sa

revision = "0012_fix_org_scoping_not_null"
down_revision = "0011_multitenant_rbac_audit_eventlog"
branch_labels = None
depends_on = None

def upgrade():
    # ---- Safety: if multiple orgs exist, and you still have NULL org rows, don't guess.
    conn = op.get_bind()

    org_count = conn.execute(sa.text("select count(*) from organizations")).scalar() or 0
    null_props = conn.execute(sa.text("select count(*) from properties where org_id is null")).scalar() or 0
    null_deals = conn.execute(sa.text("select count(*) from deals where org_id is null")).scalar() or 0
    null_ra = conn.execute(sa.text("select count(*) from rent_assumptions where org_id is null")).scalar() or 0

    if org_count > 1 and (null_props or null_deals or null_ra):
        raise RuntimeError(
            f"Refusing to backfill org_id with multiple orgs present. "
            f"NULLS: properties={null_props}, deals={null_deals}, rent_assumptions={null_ra}"
        )

    # ---- Dev-friendly backfill: if only 1 org, set NULLs to that org id
    if org_count == 1:
        only_org_id = conn.execute(sa.text("select id from organizations order by id limit 1")).scalar()
        conn.execute(sa.text("update properties set org_id=:oid where org_id is null"), {"oid": only_org_id})
        conn.execute(sa.text("update deals set org_id=:oid where org_id is null"), {"oid": only_org_id})
        conn.execute(sa.text("update rent_assumptions set org_id=:oid where org_id is null"), {"oid": only_org_id})

    # ---- Enforce NOT NULL at DB level
    op.alter_column("properties", "org_id", existing_type=sa.Integer(), nullable=False)
    op.alter_column("deals", "org_id", existing_type=sa.Integer(), nullable=False)
    op.alter_column("rent_assumptions", "org_id", existing_type=sa.Integer(), nullable=False)

    # ---- Unique per org/property for rent assumptions
    op.create_unique_constraint(
        "uq_rent_assumptions_org_property",
        "rent_assumptions",
        ["org_id", "property_id"],
    )

def downgrade():
    op.drop_constraint("uq_rent_assumptions_org_property", "rent_assumptions", type_="unique")
    op.alter_column("rent_assumptions", "org_id", existing_type=sa.Integer(), nullable=True)
    op.alter_column("deals", "org_id", existing_type=sa.Integer(), nullable=True)
    op.alter_column("properties", "org_id", existing_type=sa.Integer(), nullable=True)
