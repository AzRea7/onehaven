from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


ProductKey = Literal["intelligence", "acquire", "compliance", "tenants", "ops"]
IngestionMode = Literal["manual_property", "csv_upload", "document_upload", "api_sync"]


@dataclass(frozen=True)
class ProductSurfaceDefinition:
    key: ProductKey
    display_name: str
    buyer: str
    job_to_be_done: str
    supported_ingestion_modes: tuple[IngestionMode, ...]
    default_dashboard_route: str
    core_outputs: tuple[str, ...]
    roi_metric: str
    internal_modules: tuple[str, ...] = field(default_factory=tuple)


PRODUCT_SURFACES: dict[ProductKey, ProductSurfaceDefinition] = {
    "intelligence": ProductSurfaceDefinition(
        key="intelligence",
        display_name="OneHaven Intelligence",
        buyer="Investors, acquisition analysts, and buyer-side operators",
        job_to_be_done="Find and rank rental deals by return, risk, Section 8 fit, and compliance drag.",
        supported_ingestion_modes=("manual_property", "csv_upload", "api_sync"),
        default_dashboard_route="/intelligence",
        core_outputs=(
            "ranked_deals",
            "buy_caution_avoid_recommendation",
            "projected_return",
            "compliance_drag",
            "rehab_risk",
        ),
        roi_metric="Faster, safer deal selection",
        internal_modules=("investor", "markets", "underwriting"),
    ),
    "acquire": ProductSurfaceDefinition(
        key="acquire",
        display_name="OneHaven Acquire",
        buyer="Acquisition teams, brokers, and scaling operators",
        job_to_be_done="Move a property from interest to close with due diligence, blockers, documents, and tasks in one workspace.",
        supported_ingestion_modes=("manual_property", "csv_upload", "document_upload", "api_sync"),
        default_dashboard_route="/acquire",
        core_outputs=(
            "deal_room",
            "due_diligence_blockers",
            "missing_documents",
            "close_readiness",
        ),
        roi_metric="Shorter acquisition cycle time",
        internal_modules=("acquisition", "workflow", "documents"),
    ),
    "compliance": ProductSurfaceDefinition(
        key="compliance",
        display_name="OneHaven Compliance",
        buyer="Section 8 landlords, PM firms, and affordable housing operators",
        job_to_be_done="Know what rules apply, what is missing, what is risky, and what to fix before inspections or payment disruption.",
        supported_ingestion_modes=("manual_property", "csv_upload", "document_upload", "api_sync"),
        default_dashboard_route="/compliance",
        core_outputs=(
            "property_compliance_brief",
            "inspection_risk",
            "missing_requirements",
            "fix_plan",
            "money_at_risk",
            "confidence_and_authority",
        ),
        roi_metric="Reduced inspection failures and payment disruptions",
        internal_modules=("compliance", "inspections", "policy"),
    ),
    "tenants": ProductSurfaceDefinition(
        key="tenants",
        display_name="OneHaven Tenants",
        buyer="Voucher landlords, PM firms, and leasing operators",
        job_to_be_done="Match qualified applicants to units and move them through the voucher-ready workflow faster.",
        supported_ingestion_modes=("manual_property", "csv_upload", "document_upload", "api_sync"),
        default_dashboard_route="/tenants",
        core_outputs=(
            "ranked_applicants",
            "voucher_readiness",
            "missing_workflow_steps",
            "communication_status",
        ),
        roi_metric="Reduced vacancy time",
        internal_modules=("tenants", "communications", "leasing"),
    ),
    "ops": ProductSurfaceDefinition(
        key="ops",
        display_name="OneHaven Ops",
        buyer="Property managers and active portfolio operators",
        job_to_be_done="Run portfolio operations across leases, tasks, inspections, turnover, and maintenance.",
        supported_ingestion_modes=("manual_property", "csv_upload", "api_sync"),
        default_dashboard_route="/ops",
        core_outputs=(
            "portfolio_operations_summary",
            "tasks_due",
            "inspection_schedule",
            "turnover_readiness",
            "occupancy_snapshot",
        ),
        roi_metric="Less operational slippage across the portfolio",
        internal_modules=("ops", "tasks", "leases", "maintenance"),
    ),
}


def get_product_surface(product_key: ProductKey) -> ProductSurfaceDefinition:
    return PRODUCT_SURFACES[product_key]


def list_product_surfaces() -> list[ProductSurfaceDefinition]:
    return list(PRODUCT_SURFACES.values())


def supports_ingestion_mode(product_key: ProductKey, mode: IngestionMode) -> bool:
    return mode in PRODUCT_SURFACES[product_key].supported_ingestion_modes


def product_surface_summary(product_key: ProductKey) -> dict[str, object]:
    surface = get_product_surface(product_key)
    return {
        "key": surface.key,
        "display_name": surface.display_name,
        "buyer": surface.buyer,
        "job_to_be_done": surface.job_to_be_done,
        "supported_ingestion_modes": list(surface.supported_ingestion_modes),
        "default_dashboard_route": surface.default_dashboard_route,
        "core_outputs": list(surface.core_outputs),
        "roi_metric": surface.roi_metric,
        "internal_modules": list(surface.internal_modules),
    }
