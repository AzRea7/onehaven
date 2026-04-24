from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.domain.product_surfaces import PRODUCT_SURFACES, ProductKey, IngestionMode


OnboardingStepKey = Literal[
    "choose_use_case",
    "choose_ingestion_mode",
    "upload_or_enter_data",
    "preview_mapping",
    "run_enrichment_pipeline",
    "show_product_summary",
]


@dataclass(frozen=True)
class OnboardingStep:
    key: OnboardingStepKey
    title: str
    description: str


@dataclass(frozen=True)
class ProductOnboardingFlow:
    product_key: ProductKey
    supported_ingestion_modes: tuple[IngestionMode, ...]
    default_ingestion_mode: IngestionMode
    steps: tuple[OnboardingStep, ...] = field(default_factory=tuple)
    first_summary_route: str = "/"


COMMON_STEPS: tuple[OnboardingStep, ...] = (
    OnboardingStep(
        key="choose_use_case",
        title="Choose your use case",
        description="Pick the business outcome you want: analyze deals, close acquisitions, stay compliant, fill vacancies, or run operations.",
    ),
    OnboardingStep(
        key="choose_ingestion_mode",
        title="Choose how to bring data in",
        description="Select manual entry, CSV upload, document upload, or API sync based on what data the customer already has.",
    ),
    OnboardingStep(
        key="upload_or_enter_data",
        title="Enter or upload data",
        description="Capture the minimum viable property, portfolio, unit, deal, or document data for the selected product.",
    ),
    OnboardingStep(
        key="preview_mapping",
        title="Preview and confirm mapping",
        description="Show matched addresses, duplicates, inferred fields, missing columns, and confidence before import is committed.",
    ),
    OnboardingStep(
        key="run_enrichment_pipeline",
        title="Run enrichment",
        description="Normalize the portfolio graph, enrich the property data, and trigger product-specific processing.",
    ),
    OnboardingStep(
        key="show_product_summary",
        title="Show the first product summary",
        description="Land the user on the most valuable summary screen for the product they chose.",
    ),
)


PRODUCT_ONBOARDING_FLOWS: dict[ProductKey, ProductOnboardingFlow] = {
    "intelligence": ProductOnboardingFlow(
        product_key="intelligence",
        supported_ingestion_modes=PRODUCT_SURFACES["intelligence"].supported_ingestion_modes,
        default_ingestion_mode="manual_property",
        steps=COMMON_STEPS,
        first_summary_route="/intelligence",
    ),
    "acquire": ProductOnboardingFlow(
        product_key="acquire",
        supported_ingestion_modes=PRODUCT_SURFACES["acquire"].supported_ingestion_modes,
        default_ingestion_mode="document_upload",
        steps=COMMON_STEPS,
        first_summary_route="/acquire",
    ),
    "compliance": ProductOnboardingFlow(
        product_key="compliance",
        supported_ingestion_modes=PRODUCT_SURFACES["compliance"].supported_ingestion_modes,
        default_ingestion_mode="csv_upload",
        steps=COMMON_STEPS,
        first_summary_route="/compliance",
    ),
    "tenants": ProductOnboardingFlow(
        product_key="tenants",
        supported_ingestion_modes=PRODUCT_SURFACES["tenants"].supported_ingestion_modes,
        default_ingestion_mode="csv_upload",
        steps=COMMON_STEPS,
        first_summary_route="/tenants",
    ),
    "ops": ProductOnboardingFlow(
        product_key="ops",
        supported_ingestion_modes=PRODUCT_SURFACES["ops"].supported_ingestion_modes,
        default_ingestion_mode="csv_upload",
        steps=COMMON_STEPS,
        first_summary_route="/ops",
    ),
}


def get_onboarding_flow(product_key: ProductKey) -> ProductOnboardingFlow:
    return PRODUCT_ONBOARDING_FLOWS[product_key]


def onboarding_flow_summary(product_key: ProductKey) -> dict[str, object]:
    flow = get_onboarding_flow(product_key)
    return {
        "product_key": flow.product_key,
        "supported_ingestion_modes": list(flow.supported_ingestion_modes),
        "default_ingestion_mode": flow.default_ingestion_mode,
        "steps": [
            {"key": step.key, "title": step.title, "description": step.description}
            for step in flow.steps
        ],
        "first_summary_route": flow.first_summary_route,
    }
