from __future__ import annotations

from typing import Any, Iterable, Optional

PANES: list[str] = [
    "investor",
    "acquisition",
    "compliance",
    "tenants",
    "management",
    "admin",
]

_PANE_RANK: dict[str, int] = {pane: idx for idx, pane in enumerate(PANES)}
_PANE_ALIASES: dict[str, str] = {
    "investor": "investor",
    "analysis": "investor",
    "underwriting": "investor",
    "acquisition": "acquisition",
    "buy": "acquisition",
    "pipeline": "acquisition",
    "offer": "acquisition",
    "compliance": "compliance",
    "s8": "compliance",
    "tenants": "tenants",
    "tenant": "tenants",
    "leasing": "tenants",
    "management": "management",
    "ops": "management",
    "admin": "admin",
}

_PANE_META = {
    "investor": {"label": "Investor", "description": "Discovery, shortlist, underwriting, and pre-handoff decisioning.", "default_roles": ["admin", "owner", "analyst", "viewer"]},
    "acquisition": {"label": "Acquire", "description": "Pre-offer pursuit through close and ownership handoff.", "default_roles": ["admin", "owner", "analyst", "acquisitions"]},
    "compliance": {"label": "Compliance / S8", "description": "Rehab, jurisdiction readiness, inspections, and voucher compliance workflow.", "default_roles": ["admin", "owner", "compliance", "inspector", "operator"]},
    "tenants": {"label": "Tenant Placement", "description": "Marketing, screening, matching, leasing, and move-in workflow.", "default_roles": ["admin", "owner", "leasing", "operator", "compliance"]},
    "management": {"label": "Administration / Management", "description": "Occupied operations, cashflow, maintenance, and property management.", "default_roles": ["admin", "owner", "manager", "operator", "accounting"]},
    "admin": {"label": "Admin", "description": "Org-level controls, settings, permissions, and platform operations.", "default_roles": ["admin", "owner"]},
}


def clamp_pane(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in _PANE_RANK else _PANE_ALIASES.get(raw, "investor")


def pane_rank(value: Optional[str]) -> int:
    return _PANE_RANK[clamp_pane(value)]


def pane_meta(value: Optional[str]) -> dict[str, Any]:
    key = clamp_pane(value)
    meta = _PANE_META.get(key, {})
    return {"key": key, "rank": pane_rank(key), "label": meta.get("label", key.title()), "description": meta.get("description", ""), "default_roles": list(meta.get("default_roles", []))}


def pane_label(value: Optional[str]) -> str:
    return pane_meta(value)["label"]


def pane_catalog() -> list[dict[str, Any]]:
    return [pane_meta(p) for p in PANES]


def stage_to_pane(stage: Optional[str], *, turnover_target: str = "compliance") -> str:
    raw = str(stage or "").strip().lower()
    if raw in {"discovered", "shortlisted", "underwritten"}:
        return "investor"
    if raw in {"pursuing", "offer_prep", "offer_ready", "offer_submitted", "negotiating", "under_contract", "due_diligence", "closing", "owned"}:
        return "acquisition"
    if raw in {"rehab", "compliance_readying", "inspection_pending"}:
        return "compliance"
    if raw in {"tenant_marketing", "tenant_screening", "leased"}:
        return "tenants"
    if raw in {"occupied", "maintenance"}:
        return "management"
    if raw == "turnover":
        target = clamp_pane(turnover_target)
        return target if target in {"investor", "compliance", "management"} else "compliance"
    return "investor"


def next_stage_to_pane(next_stage: Optional[str], *, turnover_target: str = "compliance") -> Optional[str]:
    return None if next_stage is None else stage_to_pane(next_stage, turnover_target=turnover_target)


def _normalize_roles(roles: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for role in roles:
        value = str(role or "").strip().lower()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def principal_roles(principal: Any) -> list[str]:
    if principal is None:
        return ["admin", "owner", "analyst", "operator", "manager", "compliance", "leasing", "viewer"]
    roles: list[str] = []
    if getattr(principal, "role", None):
        roles.append(str(getattr(principal, "role")))
    if isinstance(getattr(principal, "roles", None), (list, tuple, set)):
        roles.extend(str(x) for x in getattr(principal, "roles"))
    if bool(getattr(principal, "is_admin", False)):
        roles.append("admin")
    if bool(getattr(principal, "is_owner", False)):
        roles.append("owner")
    normalized = _normalize_roles(roles)
    return normalized or ["viewer"]


def allowed_panes_for_roles(roles: Iterable[str]) -> list[str]:
    role_set = set(_normalize_roles(roles))
    if "admin" in role_set or "owner" in role_set:
        return list(PANES)
    allowed = [pane for pane in PANES if role_set.intersection(set(_PANE_META.get(pane, {}).get("default_roles", [])))]
    return allowed or ["investor"]


def allowed_panes_for_principal(principal: Any) -> list[str]:
    return allowed_panes_for_roles(principal_roles(principal))
