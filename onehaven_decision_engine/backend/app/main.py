# backend/app/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .logging_config import configure_logging
from .middleware.request_id import RequestIDMiddleware
from .middleware.structured_logging import StructuredLoggingMiddleware

from .routers.health import router as health_router
from .routers.meta import router as meta_router
from .routers.dashboard import router as dashboard_router

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

from .routers.agents import router as agents_router
from .routers.agent_runs import router as agent_runs_router
from .routers.workflow import router as workflow_router
from .routers.audit import router as audit_router
from .routers.trust import router as trust_router

# SaaS auth + api keys
from .routers.auth import router as auth_router
from .routers.api_keys import router as api_keys_router

API_PREFIX = "/api"


def _dev_origin_allowlist() -> list[str]:
    # covers: Vite dev, nginx dev/prod, and common localhost variants
    return [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]


def _cors_origins() -> list[str]:
    """
    settings.cors_allow_origins can be:
      - "*" (string)  -> DANGEROUS with cookies; we auto-fix to dev allowlist
      - "http://localhost:5173,http://127.0.0.1:5173" (string CSV)
      - ["http://localhost:5173", ...] (list)
    """
    val = getattr(settings, "cors_allow_origins", None)

    # sensible dev default (works with Vite on 5173 + nginx on 8080)
    if val is None:
        return _dev_origin_allowlist()

    if isinstance(val, str):
        v = val.strip()
        if v == "*":
            # ✅ IMPORTANT:
            # "*" + cookies/credentials is blocked by browsers.
            # Instead of silently breaking auth, treat "*" as "dev allowlist".
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

    # Observability first
    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(StructuredLoggingMiddleware)

    origins = _cors_origins()

    # ✅ We are intentionally NOT supporting allow_origins=["*"] here,
    # because cookie-based auth requires allow_credentials=True,
    # and browsers disallow "*" with credentials.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Core
    app.include_router(health_router, prefix=API_PREFIX)
    app.include_router(meta_router, prefix=API_PREFIX)
    app.include_router(dashboard_router, prefix=API_PREFIX)

    # Decision engine pipeline
    app.include_router(properties_router, prefix=API_PREFIX)
    app.include_router(deals_router, prefix=API_PREFIX)
    app.include_router(jurisdictions_router, prefix=API_PREFIX)
    app.include_router(evaluate_router, prefix=API_PREFIX)

    # Ingest + rent
    app.include_router(imports_router, prefix=API_PREFIX)
    app.include_router(imports_alias_router, prefix=API_PREFIX)
    app.include_router(rent_router, prefix=API_PREFIX)
    app.include_router(rent_enrich_router, prefix=API_PREFIX)

    # Compliance
    app.include_router(compliance_router, prefix=API_PREFIX)
    app.include_router(inspections_router, prefix=API_PREFIX)

    # Ops
    app.include_router(rehab_router, prefix=API_PREFIX)
    app.include_router(tenants_router, prefix=API_PREFIX)
    app.include_router(cash_router, prefix=API_PREFIX)
    app.include_router(equity_router, prefix=API_PREFIX)
    app.include_router(ops_router, prefix=API_PREFIX)

    # Agents + workflow
    app.include_router(agents_router, prefix=API_PREFIX)
    app.include_router(agent_runs_router, prefix=API_PREFIX)
    app.include_router(workflow_router, prefix=API_PREFIX)
    app.include_router(audit_router, prefix=API_PREFIX)
    app.include_router(trust_router, prefix=API_PREFIX)

    # SaaS auth + api keys
    app.include_router(auth_router, prefix=API_PREFIX)
    app.include_router(api_keys_router, prefix=API_PREFIX)

    return app


app = create_app()
