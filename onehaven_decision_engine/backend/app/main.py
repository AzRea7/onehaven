from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from .logging_config import configure_logging
from .middleware.request_id import RequestIDMiddleware
from .middleware.structured_logging import StructuredLoggingMiddleware

from .routers.markets import router as markets_router
from .routers.ingestion import router as ingestion_router
from .routers.health import router as health_router
from .routers.meta import router as meta_router
from .routers.dashboard import router as dashboard_router
from .routers.photos import router as photos_router

from .routers.properties import router as properties_router
from .routers.deals import router as deals_router
from .routers.jurisdictions import router as jurisdictions_router
from .routers.evaluate import router as evaluate_router

from .routers.imports import router as imports_router
from .routers.imports_alias import router as imports_alias_router
from .routers.rent import router as rent_router
from .routers.rent_enrich import router as rent_enrich_router

from .routers.compliance import router as compliance_router
from .routers.inspections import router as inspections_router

from .routers.rehab import router as rehab_router
from .routers.tenants import router as tenants_router
from .routers.cash import router as cash_router
from .routers.equity import router as equity_router
from .routers.ops import router as ops_router
from .routers.acquisition import router as acquisition_router

from .routers.agents import router as agents_router
from .routers.agent_runs import router as agent_runs_router
from .routers.workflow import router as workflow_router
from .routers.audit import router as audit_router
from .routers.trust import router as trust_router

from .routers.auth import router as auth_router
from .routers.api_keys import router as api_keys_router
from .routers.automation import router as automation_router

from .routers.jurisdiction_profiles import router as jurisdiction_profiles_router
from .routers.policy_seed import router as policy_seed_router
from .routers.policy_evidence import router as policy_evidence_router
from .routers.policy import router as policy_router
from .routers.policy_catalog_admin import router as policy_catalog_admin_router

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

    origins = _cors_origins()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health_router, prefix=API_PREFIX)
    app.include_router(meta_router, prefix=API_PREFIX)
    app.include_router(dashboard_router, prefix=API_PREFIX)

    app.include_router(properties_router, prefix=API_PREFIX)
    app.include_router(deals_router, prefix=API_PREFIX)
    app.include_router(jurisdictions_router, prefix=API_PREFIX)
    app.include_router(evaluate_router, prefix=API_PREFIX)

    app.include_router(imports_router, prefix=API_PREFIX)
    app.include_router(imports_alias_router, prefix=API_PREFIX)
    app.include_router(rent_router, prefix=API_PREFIX)
    app.include_router(rent_enrich_router, prefix=API_PREFIX)

    app.include_router(compliance_router, prefix=API_PREFIX)
    app.include_router(inspections_router, prefix=API_PREFIX)
    app.include_router(photos_router, prefix=API_PREFIX)

    app.include_router(rehab_router, prefix=API_PREFIX)
    app.include_router(tenants_router, prefix=API_PREFIX)
    app.include_router(cash_router, prefix=API_PREFIX)
    app.include_router(equity_router, prefix=API_PREFIX)
    app.include_router(ops_router, prefix=API_PREFIX)
    app.include_router(acquisition_router, prefix=API_PREFIX)

    app.include_router(agents_router, prefix=API_PREFIX)
    app.include_router(agent_runs_router, prefix=API_PREFIX)
    app.include_router(workflow_router, prefix=API_PREFIX)
    app.include_router(audit_router, prefix=API_PREFIX)
    app.include_router(trust_router, prefix=API_PREFIX)

    app.include_router(auth_router, prefix=API_PREFIX)
    app.include_router(api_keys_router, prefix=API_PREFIX)
    app.include_router(automation_router, prefix=API_PREFIX)

    app.include_router(jurisdiction_profiles_router, prefix=API_PREFIX)
    app.include_router(policy_seed_router, prefix=API_PREFIX)
    app.include_router(policy_router, prefix=API_PREFIX)
    app.include_router(policy_evidence_router, prefix=API_PREFIX)
    app.include_router(policy_catalog_admin_router, prefix=API_PREFIX)

    app.include_router(ingestion_router, prefix=API_PREFIX)
    app.include_router(markets_router, prefix=API_PREFIX)

    return app


app = create_app()