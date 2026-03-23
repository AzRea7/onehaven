from __future__ import annotations

from typing import Any, Iterable, Optional

PANES: list[str] = [
    "acquisition",
    "investor",
    "compliance",
    "tenants",
    "management",
    "admin",
]

_PANE_RANK: dict[str, int] = {pane: idx for idx, pane in enumerate(PANES)}

_PANE_ALIASES: dict[str, str] = {
    "acquisition": "acquisition",
    "buy": "acquisition",
    "buybox": "acquisition",
    "pipeline": "acquisition",
    "investor": "investor",
    "analysis": "investor",
    "underwriting": "investor",
    "compliance": "compliance",
    "s8": "compliance",
    "section8": "compliance",
    "inspection": "compliance",
    "tenants": "tenants",
    "tenant": "tenants",
    "placement": "tenants",
    "leasing": "tenants",
    "management": "management",
    "admin": "admin",
    "administration": "admin",
    "ops": "management",
    "operations": "management",
}

_PANE_META: dict[str, dict[str, Any]] = {
    "acquisition": {
        "label": "Acquisition",
        "description": "Active pursuit of properties that are moving toward purchase or close.",
        "default_roles": ["admin", "owner", "acquisitions", "analyst"],
    },
    "investor": {
        "label": "Investor",
        "description": "Discovery, shortlist, underwriting, and investor-grade property analysis.",
        "default_roles": ["admin", "owner", "analyst", "viewer"],
    },
    "compliance": {
        "label": "Compliance / S8",
        "description": "Rehab, jurisdiction readiness, inspections, and voucher compliance workflow.",
        "default_roles": ["admin", "owner", "compliance", "inspector", "operator"],
    },
    "tenants": {
        "label": "Tenant Placement",
        "description": "Marketing, screening, matching, leasing, and move-in workflow.",
        "default_roles": ["admin", "owner", "leasing", "operator", "compliance"],
    },
    "management": {
        "label": "Administration / Management",
        "description": "Occupied operations, cashflow, maintenance, and property management.",
        "default_roles": ["admin", "owner", "manager", "operator", "accounting"],
    },
    "admin": {
        "label": "Admin",
        "description": "Org-level controls, settings, contracts, permissions, and platform operations.",
        "default_roles": ["admin", "owner"],
    },
}


def clamp_pane(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if raw in _PANE_RANK:
        return raw
    if raw in _PANE_ALIASES:
        return _PANE_ALIASES[raw]
    return "investor"


def pane_rank(value: Optional[str]) -> int:
    return _PANE_RANK[clamp_pane(value)]


def pane_meta(value: Optional[str]) -> dict[str, Any]:
    key = clamp_pane(value)
    meta = _PANE_META.get(key, {})
    return {
        "key": key,
        "rank": pane_rank(key),
        "label": meta.get("label", key.title()),
        "description": meta.get("description", ""),
        "default_roles": list(meta.get("default_roles", [])),
    }


def pane_label(value: Optional[str]) -> str:
    return pane_meta(value)["label"]


def pane_catalog() -> list[dict[str, Any]]:
    return [pane_meta(pane) for pane in PANES]


def stage_to_pane(stage: Optional[str], *, turnover_target: str = "compliance") -> str:
    raw = str(stage or "").strip().lower()

    if raw in {"discovered", "shortlisted", "underwritten"}:
        return "investor"

    if raw in {"offer"}:
        return "acquisition"

    if raw in {"acquired", "rehab", "compliance_readying", "inspection_pending"}:
        return "compliance"

    if raw in {"tenant_marketing", "tenant_screening", "leased"}:
        return "tenants"

    if raw in {"occupied", "maintenance"}:
        return "management"

    if raw == "turnover":
        target = clamp_pane(turnover_target)
        if target in {"investor", "compliance", "management"}:
            return target
        return "compliance"

    return "investor"


def _normalize_roles(roles: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for role in roles:
        value = str(role or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def principal_roles(principal: Any) -> list[str]:
    if principal is None:
        return ["admin", "owner", "analyst", "operator", "manager", "compliance", "leasing", "viewer"]

    roles: list[str] = []

    raw_role = getattr(principal, "role", None)
    if raw_role:
        roles.append(str(raw_role))

    raw_roles = getattr(principal, "roles", None)
    if isinstance(raw_roles, (list, tuple, set)):
        roles.extend(str(x) for x in raw_roles)

    if bool(getattr(principal, "is_admin", False)):
        roles.append("admin")

    if bool(getattr(principal, "is_owner", False)):
        roles.append("owner")

    normalized = _normalize_roles(roles)
    return normalized or ["viewer"]


def allowed_panes_for_roles(roles: Iterable[str]) -> list[str]:
    role_set = set(_normalize_roles(roles))
    allowed: list[str] = []

    for pane in PANES:
        defaults = set(_PANE_META.get(pane, {}).get("default_roles", []))
        if role_set.intersection(defaults):
            allowed.append(pane)

    if "admin" in role_set or "owner" in role_set:
        return list(PANES)

    return allowed or ["investor"]


def allowed_panes_for_principal(principal: Any) -> list[str]:
    return allowed_panes_for_roles(principal_roles(principal))
