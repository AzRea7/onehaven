
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import Document


DOCUMENT_KIND_BY_PRODUCT: dict[str, str] = {
    "acquire": "acquisition_document",
    "compliance": "compliance_document",
    "inspection": "inspection_document",
    "tenants": "tenant_document",
    "ops": "operations_document",
}


def route_document_kind(*, product_surface: str, explicit_kind: str | None = None) -> str:
    if explicit_kind:
        return str(explicit_kind).strip()
    return DOCUMENT_KIND_BY_PRODUCT.get(str(product_surface).strip().lower(), "general")


def create_routed_document(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    title: str | None = None,
    property_id: int | None = None,
    acquisition_deal_id: int | None = None,
    inspection_id: int | None = None,
    lease_id: int | None = None,
    tenant_id: int | None = None,
    storage_key: str | None = None,
    external_url: str | None = None,
    content_type: str | None = None,
    parser_status: str | None = None,
    parsed_summary_json: dict[str, Any] | None = None,
    metadata_json: dict[str, Any] | None = None,
    explicit_kind: str | None = None,
) -> Document:
    row = Document(
        org_id=int(org_id),
        property_id=property_id,
        acquisition_deal_id=acquisition_deal_id,
        inspection_id=inspection_id,
        lease_id=lease_id,
        tenant_id=tenant_id,
        document_kind=route_document_kind(product_surface=product_surface, explicit_kind=explicit_kind),
        source="upload",
        title=title,
        storage_key=storage_key,
        external_url=external_url,
        content_type=content_type,
        parser_status=parser_status,
        parsed_summary_json=parsed_summary_json,
        metadata_json={
            **dict(metadata_json or {}),
            "product_surface": product_surface,
        },
    )
    db.add(row)
    db.flush()
    return row


def route_document_upload(
    db: Session,
    *,
    org_id: int,
    product_surface: str,
    uploads: list[dict[str, Any]],
) -> dict[str, Any]:
    created_ids: list[int] = []
    for item in uploads:
        row = create_routed_document(
            db,
            org_id=org_id,
            product_surface=product_surface,
            title=item.get("title"),
            property_id=item.get("property_id"),
            acquisition_deal_id=item.get("acquisition_deal_id"),
            inspection_id=item.get("inspection_id"),
            lease_id=item.get("lease_id"),
            tenant_id=item.get("tenant_id"),
            storage_key=item.get("storage_key"),
            external_url=item.get("external_url"),
            content_type=item.get("content_type"),
            parser_status=item.get("parser_status"),
            parsed_summary_json=item.get("parsed_summary_json"),
            metadata_json=item.get("metadata_json"),
            explicit_kind=item.get("document_kind"),
        )
        created_ids.append(int(row.id))
    db.commit()
    return {"ok": True, "created_document_ids": created_ids, "count": len(created_ids)}
