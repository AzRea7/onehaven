# backend/app/routers/meta.py
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter

router = APIRouter(prefix="/meta", tags=["meta"])

REPO_ROOT = Path(__file__).resolve().parents[3]  # .../onehaven_decision_engine
DOCS_DIR = REPO_ROOT / "docs"


def _read_doc(name: str) -> str:
    p = DOCS_DIR / name
    if not p.exists():
        return f"missing doc: {name}"
    return p.read_text(encoding="utf-8")


@router.get("/disclaimer", response_model=dict)
def disclaimer():
    return {
        "statement": "Operational intelligence based on public rules and historical outcomes. Not legal advice.",
        "docs": {
            "operating_principles": "/meta/docs/operating_principles",
            "terms": "/meta/docs/terms",
            "pricing": "/meta/docs/pricing",
        },
    }


@router.get("/docs/operating_principles", response_model=dict)
def operating_principles():
    return {"name": "operating_principles.md", "content": _read_doc("operating_principles.md")}


@router.get("/docs/terms", response_model=dict)
def terms():
    return {"name": "terms.md", "content": _read_doc("terms.md")}


@router.get("/docs/pricing", response_model=dict)
def pricing():
    return {"name": "pricing.md", "content": _read_doc("pricing.md")}
