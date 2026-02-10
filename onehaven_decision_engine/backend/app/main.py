from __future__ import annotations

from fastapi import FastAPI

from .routers.health import router as health_router
from .routers.properties import router as properties_router
from .routers.deals import router as deals_router
from .routers.jurisdictions import router as jurisdictions_router
from .routers.evaluate import router as evaluate_router
from .routers.inspections import router as inspections_router
from .routers.imports import router as imports_router
from .routers.rent_enrich import router as rent_enrich_router  # ✅ NEW

app = FastAPI(
    title="Decision Engine for Regulated Residential Cash-Flow Assets",
    version="0.3.0",
)

app.include_router(health_router)
app.include_router(properties_router)
app.include_router(deals_router)
app.include_router(jurisdictions_router)
app.include_router(evaluate_router)
app.include_router(inspections_router)
app.include_router(imports_router)
app.include_router(rent_enrich_router)  # ✅ NEW
