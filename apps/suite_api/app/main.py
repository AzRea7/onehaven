from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.logging_config import configure_logging
from onehaven_platform.backend.src.middleware.request_id import RequestIDMiddleware
from onehaven_platform.backend.src.middleware.structured_logging import StructuredLoggingMiddleware

from apps.suite_api.app.api.health.health import router as health_router
from apps.suite_api.app.api.health.meta import router as meta_router

from products.ops.backend.src.routers.dashboard import router as dashboard_router
from products.ops.backend.src.routers.properties import router as properties_router
from products.ops.backend.src.routers.rehab import router as rehab_router
from products.ops.backend.src.routers.ops import router as ops_router

from products.intelligence.backend.src.routers.deals import router as deals_router
from products.intelligence.backend.src.routers.evaluate import router as evaluate_router
from products.intelligence.backend.src.routers.rent import router as rent_router
from products.intelligence.backend.src.routers.rent_enrich import router as rent_enrich_router
from products.intelligence.backend.src.routers.cash import router as cash_router
from products.intelligence.backend.src.routers.equity import router as equity_router

from products.acquire.backend.src.routers.imports import router as imports_router
from products.acquire.backend.src.routers.imports_alias import router as imports_alias_router
from products.acquire.backend.src.routers.acquisition import router as acquisition_router

from products.compliance.backend.src.routers.markets import router as markets_router
from products.compliance.backend.src.routers.jurisdictions import router as jurisdictions_router
from products.compliance.backend.src.routers.compliance import router as compliance_router
from products.compliance.backend.src.routers.inspections import router as inspections_router
from products.compliance.backend.src.routers.photos import router as photos_router
from products.compliance.backend.src.routers.trust import router as trust_router
from products.compliance.backend.src.routers.jurisdiction_profiles import router as jurisdiction_profiles_router
from products.compliance.backend.src.routers.policy_seed import router as policy_seed_router
from products.compliance.backend.src.routers.policy_evidence import router as policy_evidence_router
from products.compliance.backend.src.routers.policy import router as policy_router
from products.compliance.backend.src.routers.policy_catalog_admin import router as policy_catalog_admin_router

from products.tenants.backend.src.routers.tenants import router as tenants_router

from onehaven_platform.backend.src.agents.agents_router import router as agents_router
from onehaven_platform.backend.src.agents.agent_runs_router import router as agent_runs_router
from onehaven_platform.backend.src.workflow.workflow import router as workflow_router
from onehaven_platform.backend.src.audit.audit_router import router as audit_router
from onehaven_platform.backend.src.identity.interfaces.auth_router import router as auth_router
from onehaven_platform.backend.src.identity.interfaces.api_keys_router import router as api_keys_router
from onehaven_platform.backend.src.workflow.automation_router import router as automation_router
from onehaven_platform.backend.src.integrations.ingestion_router import router as ingestion_router

API_PREFIX = "/api"


def _dev_origin_allowlist() -> list[str]:
    return [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]


def _cors_origins() -> list[str]:
    val = getattr(settings, "cors_allow_origins", None)

    if val is None:
        return _dev_origin_allowlist()

    if isinstance(val, str):
        v = val.strip()
        if v == "*":
            return _dev_origin_allowlist()
        return [x.strip() for x in v.split(",") if x.strip()]

    if isinstance(val, list) and val:
        cleaned = [str(x).strip() for x in val if str(x).strip()]
        if cleaned == ["*"]:
            return _dev_origin_allowlist()
        return cleaned

    return _dev_origin_allowlist()


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title="OneHaven Decision Engine",
        version=getattr(settings, "decision_version", "dev"),
    )

    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(StructuredLoggingMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health_router, prefix=API_PREFIX)
    app.include_router(meta_router, prefix=API_PREFIX)

    app.include_router(dashboard_router, prefix=API_PREFIX)
    app.include_router(properties_router, prefix=API_PREFIX)
    app.include_router(rehab_router, prefix=API_PREFIX)
    app.include_router(ops_router, prefix=API_PREFIX)

    app.include_router(deals_router, prefix=API_PREFIX)
    app.include_router(evaluate_router, prefix=API_PREFIX)
    app.include_router(rent_router, prefix=API_PREFIX)
    app.include_router(rent_enrich_router, prefix=API_PREFIX)
    app.include_router(cash_router, prefix=API_PREFIX)
    app.include_router(equity_router, prefix=API_PREFIX)

    app.include_router(imports_router, prefix=API_PREFIX)
    app.include_router(imports_alias_router, prefix=API_PREFIX)
    app.include_router(acquisition_router, prefix=API_PREFIX)

    app.include_router(jurisdictions_router, prefix=API_PREFIX)
    app.include_router(compliance_router, prefix=API_PREFIX)
    app.include_router(inspections_router, prefix=API_PREFIX)
    app.include_router(photos_router, prefix=API_PREFIX)
    app.include_router(trust_router, prefix=API_PREFIX)
    app.include_router(jurisdiction_profiles_router, prefix=API_PREFIX)
    app.include_router(policy_seed_router, prefix=API_PREFIX)
    app.include_router(policy_router, prefix=API_PREFIX)
    app.include_router(policy_evidence_router, prefix=API_PREFIX)
    app.include_router(policy_catalog_admin_router, prefix=API_PREFIX)
    app.include_router(markets_router, prefix=API_PREFIX)

    app.include_router(tenants_router, prefix=API_PREFIX)

    app.include_router(agents_router, prefix=API_PREFIX)
    app.include_router(agent_runs_router, prefix=API_PREFIX)
    app.include_router(workflow_router, prefix=API_PREFIX)
    app.include_router(audit_router, prefix=API_PREFIX)
    app.include_router(auth_router, prefix=API_PREFIX)
    app.include_router(api_keys_router, prefix=API_PREFIX)
    app.include_router(automation_router, prefix=API_PREFIX)
    app.include_router(ingestion_router, prefix=API_PREFIX)

    return app


app = create_app()