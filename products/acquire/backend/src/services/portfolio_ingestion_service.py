
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import (
    AcquisitionDeal,
    DataSource,
    Lease,
    Portfolio,
    PortfolioPropertyLink,
    Property,
    SyncJob,
    Task,
    Tenant,
    Unit,
)
from onehaven_platform.backend.src.services.property_normalization_service import (
    build_normalized_property_identity,
    match_existing_property,
    normalize_county,
)


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _parse_dt(value: Any) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def upsert_portfolio(
    db: Session,
    *,
    org_id: int,
    slug: str,
    name: str,
    description: str | None = None,
    portfolio_type: str = "general",
    is_default: bool = False,
    metadata_json: dict[str, Any] | None = None,
) -> Portfolio:
    stmt = select(Portfolio).where(Portfolio.org_id == int(org_id), Portfolio.slug == str(slug).strip())
    row = db.scalars(stmt.limit(1)).first()
    if row is None:
        row = Portfolio(
            org_id=int(org_id),
            slug=str(slug).strip(),
            name=str(name).strip(),
            description=description,
            portfolio_type=portfolio_type,
            is_default=bool(is_default),
            metadata_json=metadata_json,
        )
        db.add(row)
        db.flush()
        return row

    row.name = str(name).strip()
    row.description = description
    row.portfolio_type = portfolio_type
    row.is_default = bool(is_default)
    row.metadata_json = metadata_json
    db.add(row)
    db.flush()
    return row


def upsert_property(
    db: Session,
    *,
    org_id: int,
    address: str,
    city: str,
    state: str,
    zip_code: str | None = None,
    county: str | None = None,
    parcel_id: str | None = None,
    bedrooms: int | None = None,
    bathrooms: float | None = None,
    square_feet: int | None = None,
    year_built: int | None = None,
    property_type: str = "single_family",
    source_metadata: dict[str, Any] | None = None,
) -> Property:
    identity = build_normalized_property_identity(
        address=address,
        city=city,
        state=state,
        zip_code=zip_code,
        county=county,
        parcel_id=parcel_id,
    )
    row = match_existing_property(
        db,
        org_id=int(org_id),
        normalized_address=identity.normalized_address,
        parcel_id=identity.parcel_id,
    )
    if row is None:
        row = Property(
            org_id=int(org_id),
            address=identity.address,
            city=identity.city,
            state=identity.state,
            zip=identity.zip_code or "",
            county=identity.county,
            normalized_address=identity.normalized_address,
            bedrooms=int(bedrooms or 0),
            bathrooms=float(bathrooms or 1.0),
            square_feet=square_feet,
            year_built=year_built,
            property_type=property_type,
        )
        if hasattr(row, "parcel_id"):
            row.parcel_id = identity.parcel_id
        if hasattr(row, "metadata_json"):
            row.metadata_json = source_metadata
        db.add(row)
        db.flush()
        return row

    row.address = identity.address
    row.city = identity.city
    row.state = identity.state
    row.zip = identity.zip_code or row.zip
    row.county = identity.county
    row.normalized_address = identity.normalized_address
    if hasattr(row, "parcel_id") and identity.parcel_id:
        row.parcel_id = identity.parcel_id
    if bedrooms is not None:
        row.bedrooms = int(bedrooms)
    if bathrooms is not None:
        row.bathrooms = float(bathrooms)
    if square_feet is not None:
        row.square_feet = square_feet
    if year_built is not None:
        row.year_built = year_built
    row.property_type = property_type or row.property_type
    db.add(row)
    db.flush()
    return row


def link_property_to_portfolio(
    db: Session,
    *,
    org_id: int,
    portfolio_id: int,
    property_id: int,
    role: str = "tracked",
    metadata_json: dict[str, Any] | None = None,
) -> PortfolioPropertyLink:
    stmt = select(PortfolioPropertyLink).where(
        PortfolioPropertyLink.portfolio_id == int(portfolio_id),
        PortfolioPropertyLink.property_id == int(property_id),
    )
    row = db.scalars(stmt.limit(1)).first()
    if row is None:
        row = PortfolioPropertyLink(
            org_id=int(org_id),
            portfolio_id=int(portfolio_id),
            property_id=int(property_id),
            role=role,
            metadata_json=metadata_json,
        )
        db.add(row)
        db.flush()
        return row

    row.role = role
    row.metadata_json = metadata_json
    db.add(row)
    db.flush()
    return row


def upsert_unit(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    unit_label: str,
    unit_type: str = "residential",
    bedrooms: int | None = None,
    bathrooms: float | None = None,
    square_feet: int | None = None,
    occupancy_status: str = "unknown",
    market_rent: float | None = None,
    voucher_eligible: bool = False,
    metadata_json: dict[str, Any] | None = None,
) -> Unit:
    stmt = select(Unit).where(
        Unit.org_id == int(org_id),
        Unit.property_id == int(property_id),
        Unit.unit_label == str(unit_label).strip(),
    )
    row = db.scalars(stmt.limit(1)).first()
    if row is None:
        row = Unit(
            org_id=int(org_id),
            property_id=int(property_id),
            unit_label=str(unit_label).strip(),
            unit_type=unit_type,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            square_feet=square_feet,
            occupancy_status=occupancy_status,
            market_rent=market_rent,
            voucher_eligible=bool(voucher_eligible),
            metadata_json=metadata_json,
        )
        db.add(row)
        db.flush()
        return row

    row.unit_type = unit_type
    row.bedrooms = bedrooms
    row.bathrooms = bathrooms
    row.square_feet = square_feet
    row.occupancy_status = occupancy_status
    row.market_rent = market_rent
    row.voucher_eligible = bool(voucher_eligible)
    row.metadata_json = metadata_json
    db.add(row)
    db.flush()
    return row


def upsert_tenant(
    db: Session,
    *,
    org_id: int,
    full_name: str,
    phone: str | None = None,
    email: str | None = None,
    voucher_status: str | None = None,
    notes: str | None = None,
) -> Tenant:
    stmt = select(Tenant).where(Tenant.org_id == int(org_id), Tenant.full_name == str(full_name).strip())
    row = db.scalars(stmt.limit(1)).first()
    if row is None:
        row = Tenant(
            org_id=int(org_id),
            full_name=str(full_name).strip(),
            phone=phone,
            email=email,
            voucher_status=voucher_status,
            notes=notes,
        )
        db.add(row)
        db.flush()
        return row

    row.phone = phone or row.phone
    row.email = email or row.email
    row.voucher_status = voucher_status or row.voucher_status
    row.notes = notes or row.notes
    db.add(row)
    db.flush()
    return row


def upsert_lease(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    tenant_id: int,
    start_date: datetime,
    unit_id: int | None = None,
    end_date: datetime | None = None,
    total_rent: float | None = None,
    tenant_portion: float | None = None,
    housing_authority_portion: float | None = None,
    hap_contract_status: str | None = None,
    notes: str | None = None,
) -> Lease:
    stmt = select(Lease).where(
        Lease.org_id == int(org_id),
        Lease.property_id == int(property_id),
        Lease.tenant_id == int(tenant_id),
        Lease.start_date == start_date,
    )
    row = db.scalars(stmt.limit(1)).first()
    if row is None:
        row = Lease(
            org_id=int(org_id),
            property_id=int(property_id),
            tenant_id=int(tenant_id),
            start_date=start_date,
            end_date=end_date,
            total_rent=float(total_rent or 0.0),
            tenant_portion=tenant_portion,
            housing_authority_portion=housing_authority_portion,
            hap_contract_status=hap_contract_status,
            notes=notes,
        )
        if hasattr(row, "unit_id"):
            row.unit_id = unit_id
        db.add(row)
        db.flush()
        return row

    row.end_date = end_date
    row.total_rent = float(total_rent or row.total_rent or 0.0)
    row.tenant_portion = tenant_portion
    row.housing_authority_portion = housing_authority_portion
    row.hap_contract_status = hap_contract_status
    row.notes = notes
    if hasattr(row, "unit_id"):
        row.unit_id = unit_id
    db.add(row)
    db.flush()
    return row


def ensure_data_source(
    db: Session,
    *,
    org_id: int,
    provider: str,
    slug: str,
    display_name: str,
    source_kind: str,
    product_surface: str | None = None,
    config_json: dict[str, Any] | None = None,
) -> DataSource:
    stmt = select(DataSource).where(
        DataSource.org_id == int(org_id),
        DataSource.provider == provider,
        DataSource.slug == slug,
    )
    row = db.scalars(stmt.limit(1)).first()
    if row is None:
        row = DataSource(
            org_id=int(org_id),
            provider=provider,
            slug=slug,
            display_name=display_name,
            source_kind=source_kind,
            product_surface=product_surface,
            is_enabled=True,
            config_json=config_json,
        )
        db.add(row)
        db.flush()
        return row

    row.display_name = display_name
    row.source_kind = source_kind
    row.product_surface = product_surface
    row.config_json = config_json
    db.add(row)
    db.flush()
    return row


def create_sync_job(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    trigger_type: str,
    data_source_id: int | None = None,
    summary_json: dict[str, Any] | None = None,
) -> SyncJob:
    row = SyncJob(
        org_id=int(org_id),
        data_source_id=data_source_id,
        product_surface=product_surface,
        trigger_type=trigger_type,
        status="running",
        summary_json=summary_json,
    )
    db.add(row)
    db.flush()
    return row


def finish_sync_job(
    db: Session,
    *,
    sync_job: SyncJob,
    status: str,
    summary_json: dict[str, Any] | None = None,
    error_json: dict[str, Any] | None = None,
) -> SyncJob:
    sync_job.status = status
    sync_job.summary_json = summary_json
    sync_job.error_json = error_json
    sync_job.finished_at = datetime.utcnow()
    db.add(sync_job)
    db.flush()
    return sync_job


def import_portfolio_rows(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    rows: list[dict[str, Any]],
    portfolio_slug: str = "default",
    portfolio_name: str = "Default Portfolio",
    source_provider: str = "manual",
    source_kind: str = "manual",
) -> dict[str, Any]:
    portfolio = upsert_portfolio(
        db,
        org_id=org_id,
        slug=portfolio_slug,
        name=portfolio_name,
        is_default=(portfolio_slug == "default"),
        metadata_json={"product_surface": product_surface},
    )
    data_source = ensure_data_source(
        db,
        org_id=org_id,
        provider=source_provider,
        slug=f"{product_surface}-{source_kind}",
        display_name=f"{product_surface.title()} {source_kind.title()} Import",
        source_kind=source_kind,
        product_surface=product_surface,
    )
    sync_job = create_sync_job(
        db,
        org_id=org_id,
        product_surface=product_surface,
        trigger_type=source_kind,
        data_source_id=int(data_source.id),
        summary_json={"row_count": len(rows)},
    )

    created_properties = 0
    created_units = 0
    created_tenants = 0
    created_leases = 0
    property_ids: list[int] = []

    try:
        for row in rows:
            prop = upsert_property(
                db,
                org_id=org_id,
                address=str(row.get("address") or ""),
                city=str(row.get("city") or ""),
                state=str(row.get("state") or "MI"),
                zip_code=_clean_text(row.get("zip")),
                county=normalize_county(row.get("county")),
                parcel_id=_clean_text(row.get("parcel_id")),
                bedrooms=_as_int(row.get("bedrooms") or row.get("beds")),
                bathrooms=_as_float(row.get("bathrooms") or row.get("baths")),
                square_feet=_as_int(row.get("square_feet")),
                year_built=_as_int(row.get("year_built")),
                property_type=_clean_text(row.get("property_type")) or "single_family",
                source_metadata={"import_row": row},
            )
            property_ids.append(int(prop.id))
            created_properties += 1
            link_property_to_portfolio(
                db,
                org_id=org_id,
                portfolio_id=int(portfolio.id),
                property_id=int(prop.id),
                role="tracked",
                metadata_json={"product_surface": product_surface},
            )

            unit_label = _clean_text(row.get("unit_label"))
            unit = None
            if unit_label:
                unit = upsert_unit(
                    db,
                    org_id=org_id,
                    property_id=int(prop.id),
                    unit_label=unit_label,
                    unit_type=_clean_text(row.get("unit_type")) or "residential",
                    bedrooms=_as_int(row.get("unit_bedrooms") or row.get("bedrooms")),
                    bathrooms=_as_float(row.get("unit_bathrooms") or row.get("bathrooms")),
                    square_feet=_as_int(row.get("unit_square_feet") or row.get("square_feet")),
                    occupancy_status=_clean_text(row.get("occupancy_status")) or "unknown",
                    market_rent=_as_float(row.get("market_rent")),
                    voucher_eligible=_as_bool(row.get("voucher_eligible")),
                    metadata_json={"import_row": row},
                )
                created_units += 1

            tenant_name = _clean_text(row.get("tenant_name") or row.get("full_name"))
            tenant = None
            if tenant_name:
                tenant = upsert_tenant(
                    db,
                    org_id=org_id,
                    full_name=tenant_name,
                    phone=_clean_text(row.get("phone")),
                    email=_clean_text(row.get("email")),
                    voucher_status=_clean_text(row.get("voucher_status")),
                    notes=_clean_text(row.get("notes")),
                )
                created_tenants += 1

            lease_start = _parse_dt(row.get("start_date"))
            if tenant is not None and lease_start is not None:
                upsert_lease(
                    db,
                    org_id=org_id,
                    property_id=int(prop.id),
                    tenant_id=int(tenant.id),
                    unit_id=int(unit.id) if unit is not None else None,
                    start_date=lease_start,
                    end_date=_parse_dt(row.get("end_date")),
                    total_rent=_as_float(row.get("total_rent")),
                    tenant_portion=_as_float(row.get("tenant_portion")),
                    housing_authority_portion=_as_float(row.get("housing_authority_portion")),
                    hap_contract_status=_clean_text(row.get("hap_contract_status")),
                    notes=_clean_text(row.get("lease_notes") or row.get("notes")),
                )
                created_leases += 1

        finish_sync_job(
            db,
            sync_job=sync_job,
            status="succeeded",
            summary_json={
                "portfolio_id": int(portfolio.id),
                "property_ids": sorted(set(property_ids)),
                "created_properties": created_properties,
                "created_units": created_units,
                "created_tenants": created_tenants,
                "created_leases": created_leases,
            },
        )
        db.commit()
        return {
            "ok": True,
            "portfolio_id": int(portfolio.id),
            "sync_job_id": int(sync_job.id),
            "property_ids": sorted(set(property_ids)),
            "created_properties": created_properties,
            "created_units": created_units,
            "created_tenants": created_tenants,
            "created_leases": created_leases,
        }
    except Exception as exc:
        db.rollback()
        try:
            finish_sync_job(
                db,
                sync_job=sync_job,
                status="failed",
                summary_json={"portfolio_id": int(portfolio.id)},
                error_json={"error": str(exc)},
            )
            db.commit()
        except Exception:
            db.rollback()
        return {"ok": False, "error": str(exc)}
