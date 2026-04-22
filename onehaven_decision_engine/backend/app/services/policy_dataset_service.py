from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from app.services.policy_catalog_admin_service import merged_catalog_for_market


def _norm_state(v: Optional[str]) -> str:
    return (v or "MI").strip().upper()


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip().lower()
    return out or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip()
    return out or None


def _dataset_category_hints(item: Any) -> list[str]:
    text = " ".join(
        [
            str(getattr(item, "title", "") or ""),
            str(getattr(item, "notes", "") or ""),
            str(getattr(item, "url", "") or ""),
            str(getattr(item, "source_kind", "") or ""),
        ]
    ).lower()
    out: list[str] = []
    checks = {
        "lead": ["lead", "lbp"],
        "source_of_income": ["source of income", "voucher discrimination"],
        "permits": ["permit"],
        "documents": ["document", "application", "packet", "submit"],
        "contacts": ["contact", "office", "department", "division"],
        "rental_license": ["license", "registration certificate", "rental certificate"],
        "fees": ["fee", "payment"],
        "program_overlay": ["voucher", "hcv", "nspire", "overlay", "hap"],
        "inspection": ["inspection", "nspire", "hqs"],
        "occupancy": ["occupancy", "certificate of occupancy", "re-occupancy"],
        "registration": ["registration"],
        "zoning": ["zoning", "land use"],
        "safety": ["housing code", "property maintenance", "habitability", "fire safety"],
    }
    for category, patterns in checks.items():
        if any(p in text for p in patterns):
            out.append(category)
    return sorted(set(out))


def dataset_family_for_item(item: Any) -> str:
    source_kind = str(getattr(item, "source_kind", "") or "").strip().lower()
    url = str(getattr(item, "url", "") or "").strip().lower()
    if "pha" in source_kind or "voucher" in source_kind:
        return "program_dataset"
    if "municipal" in source_kind or "city" in source_kind or "county" in source_kind:
        return "municipal_dataset"
    if "state" in source_kind or "mshda" in url or "michigan.gov" in url:
        return "state_dataset"
    if "federal" in source_kind or "hud.gov" in url or "ecfr.gov" in url or "federalregister.gov" in url:
        return "federal_dataset"
    return "catalog_dataset"


def _dataset_publication_type(item: Any) -> str:
    url = str(getattr(item, "url", "") or "").strip().lower()
    source_kind = str(getattr(item, "source_kind", "") or "").strip().lower()
    title = str(getattr(item, "title", "") or "").strip().lower()
    if url.endswith(".pdf") or "pdf" in source_kind:
        return "pdf"
    if any(token in url for token in ("api", "json", "csv", "xml")):
        return "api"
    if "checklist" in title or "packet" in title or "form" in title:
        return "document"
    return "web_page"


def _dataset_truth_policy(item: Any) -> dict[str, Any]:
    publication_type = _dataset_publication_type(item)
    is_authoritative = bool(getattr(item, "is_authoritative", False))
    dataset_family = dataset_family_for_item(item)

    if publication_type == "pdf":
        return {
            "publication_type": publication_type,
            "dataset_use_role": "evidence_backed_assertions",
            "truth_role": "evidence_only",
            "projectable_truth": False,
            "requires_validation": True,
            "requires_binding_authority": True,
            "source_authority_score": 0.80 if is_authoritative else 0.55,
        }
    if publication_type == "api":
        return {
            "publication_type": publication_type,
            "dataset_use_role": "structured_truth_input",
            "truth_role": "primary_or_supporting_truth",
            "projectable_truth": bool(is_authoritative),
            "requires_validation": True,
            "requires_binding_authority": True,
            "source_authority_score": 0.95 if is_authoritative else 0.70,
        }
    return {
        "publication_type": publication_type,
        "dataset_use_role": "catalog_reference",
        "truth_role": "supporting_reference",
        "projectable_truth": False,
        "requires_validation": True,
        "requires_binding_authority": True,
        "source_authority_score": 0.85 if is_authoritative and dataset_family in {"federal_dataset", "state_dataset", "municipal_dataset"} else 0.60,
    }


def dataset_priority_for_item(item: Any) -> int:
    raw = int(getattr(item, "priority", 100) or 100)
    family = dataset_family_for_item(item)
    family_boost = {
        "federal_dataset": 0,
        "state_dataset": 5,
        "municipal_dataset": 10,
        "program_dataset": 15,
        "catalog_dataset": 20,
    }.get(family, 25)
    truth_policy = _dataset_truth_policy(item)
    evidence_penalty = 5 if truth_policy["publication_type"] == "pdf" else 0
    return raw + family_boost + evidence_penalty


def policy_catalog_dataset_for_market(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    focus: str = "se_mi_extended",
) -> list[dict[str, Any]]:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        focus=focus,
    )
    rows = []
    for item in items:
        truth_policy = _dataset_truth_policy(item)
        rows.append(
            {
                "url": getattr(item, "url", None),
                "title": getattr(item, "title", None),
                "publisher": getattr(item, "publisher", None),
                "state": getattr(item, "state", None),
                "county": getattr(item, "county", None),
                "city": getattr(item, "city", None),
                "pha_name": getattr(item, "pha_name", None),
                "program_type": getattr(item, "program_type", None),
                "source_kind": getattr(item, "source_kind", None),
                "is_authoritative": bool(getattr(item, "is_authoritative", False)),
                "priority": int(getattr(item, "priority", 100) or 100),
                "dataset_family": dataset_family_for_item(item),
                "dataset_priority": dataset_priority_for_item(item),
                "category_hints": _dataset_category_hints(item),
                **truth_policy,
            }
        )
    rows.sort(
        key=lambda r: (
            int(r["dataset_priority"]),
            0 if r["is_authoritative"] else 1,
            0 if r["projectable_truth"] else 1,
            r["title"] or "",
            r["url"] or "",
        )
    )
    return rows


def dataset_snapshot_for_market(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    include_global: bool = True,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    rows = policy_catalog_dataset_for_market(
        db,
        org_id=org_id if include_global else org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    counts: dict[str, int] = {}
    hinted: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    for row in rows:
        family = str(row.get("dataset_family") or "unknown")
        counts[family] = counts.get(family, 0) + 1
        role = str(row.get("dataset_use_role") or "unknown")
        role_counts[role] = role_counts.get(role, 0) + 1
        for category in list(row.get("category_hints") or []):
            hinted[category] = hinted.get(category, 0) + 1
    return {
        "ok": True,
        "market": {
            "state": _norm_state(state),
            "county": _norm_lower(county),
            "city": _norm_lower(city),
            "pha_name": _norm_text(pha_name),
        },
        "rows": rows,
        "summary": {
            "dataset_count": len(rows),
            "dataset_family_counts": counts,
            "dataset_use_role_counts": role_counts,
            "category_hint_counts": hinted,
            "service_role": "dataset_registry_with_truth_boundary",
            "truth_model": "evidence_first",
            "pdfs_are_primary_truth": False,
        },
    }
