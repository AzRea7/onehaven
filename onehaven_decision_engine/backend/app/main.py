# backend/app/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

# Ops Tabs
from .routers.rehab import router as rehab_router
from .routers.tenants import router as tenants_router
from .routers.cash import router as cash_router
from .routers.equity import router as equity_router

# Agents
from .routers.agents import router as agents_router
from .routers.auth import router as auth_router
from .routers.workflow import router as workflow_router
from .routers.audit import router as audit_router


app = FastAPI(title="OneHaven Decision Engine")

API_PREFIX = "/api"

# CORS: allow local frontend dev + “OpenClaw-style” dashboard hosting later
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later (env-based)
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

# Agents
app.include_router(agents_router, prefix=API_PREFIX)
app.include_router(auth_router, prefix=API_PREFIX)
app.include_router(workflow_router, prefix=API_PREFIX)
app.include_router(audit_router, prefix=API_PREFIX)
