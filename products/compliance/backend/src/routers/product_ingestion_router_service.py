
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.product_surfaces import PRODUCT_SURFACES
from products.acquire.backend.src.services.csv_import_mapping_service import map_csv_payload
from products.acquire.backend.src.services.document_ingestion_router_service import route_document_upload
from products.acquire.backend.src.services.portfolio_ingestion_service import import_portfolio_rows


def ingest_manual_property_payload(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return import_portfolio_rows(
        db,
        org_id=org_id,
        product_surface=product_surface,
        rows=[payload],
        portfolio_slug=f"{product_surface}-manual",
        portfolio_name=f"{PRODUCT_SURFACES[product_surface].display_name} Manual Imports",
        source_provider="manual",
        source_kind="manual_property",
    )


def ingest_csv_payload(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    template_key: str,
    data: bytes | str,
) -> dict[str, Any]:
    mapped = map_csv_payload(template_key, data)
    if not mapped.get("ok"):
        return mapped
    return {
        **import_portfolio_rows(
            db,
            org_id=org_id,
            product_surface=product_surface,
            rows=list(mapped.get("rows") or []),
            portfolio_slug=f"{product_surface}-{template_key}",
            portfolio_name=f"{PRODUCT_SURFACES[product_surface].display_name} CSV Imports",
            source_provider="csv",
            source_kind="csv_upload",
        ),
        "mapping": mapped.get("validation"),
    }


def ingest_document_payload(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    uploads: list[dict[str, Any]],
) -> dict[str, Any]:
    return route_document_upload(
        db,
        org_id=org_id,
        product_surface=product_surface,
        uploads=uploads,
    )


def route_product_ingestion(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    ingestion_mode: str,
    payload: dict[str, Any] | None = None,
    csv_template_key: str | None = None,
    csv_data: bytes | str | None = None,
    uploads: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if product_surface not in PRODUCT_SURFACES:
        return {"ok": False, "error": "unknown_product_surface", "product_surface": product_surface}

    mode = str(ingestion_mode).strip().lower()
    if mode == "manual_property":
        return ingest_manual_property_payload(
            db,
            org_id=org_id,
            product_surface=product_surface,
            payload=dict(payload or {}),
        )
    if mode == "csv_upload":
        return ingest_csv_payload(
            db,
            org_id=org_id,
            product_surface=product_surface,
            template_key=str(csv_template_key or ""),
            data=csv_data or "",
        )
    if mode == "document_upload":
        return ingest_document_payload(
            db,
            org_id=org_id,
            product_surface=product_surface,
            uploads=list(uploads or []),
        )
    if mode == "api_sync":
        return {
            "ok": True,
            "product_surface": product_surface,
            "ingestion_mode": mode,
            "status": "not_implemented_yet",
            "message": "API sync is reserved for the shared ingestion core but not implemented in this step.",
        }
    return {"ok": False, "error": "unsupported_ingestion_mode", "ingestion_mode": mode}
