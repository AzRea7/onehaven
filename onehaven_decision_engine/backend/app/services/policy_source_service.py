# backend/app/services/policy_source_service.py
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import category_label
from app.policy_models import PolicyCatalogEntry, PolicySource, PolicySourceVersion
from app.services.policy_discovery_service import (
    expected_inventory_hints,
    mark_inventory_not_found,
    record_discovery_attempt,
    summarize_inventory_for_scope,
    sync_policy_source_into_inventory,
    upsert_source_inventory_record,
)
from app.services.policy_crawl_service import sync_crawl_result_to_inventory
from app.services.policy_change_detection_service import (
    build_source_change_summary,
    compute_next_retry_due,
    determine_source_refresh_state,
)


AUTHORITY_TIER_RANKS: dict[str, int] = {
    "derived_or_inferred": 25,
    "semi_authoritative_operational": 60,
    "approved_official_supporting": 85,
    "authoritative_official": 100,
}


DEFAULT_TIMEOUT_SECONDS = 20.0
DISCOVERY_NOTE_MARKER = "[discovered]"
CURATED_NOTE_MARKER = "[curated]"
DEFAULT_DISCOVERY_MAX_CANDIDATES = 24


@dataclass(frozen=True)
class PolicySourceDiscoveryCandidate:
    url: str
    title: str
    publisher: str | None
    source_type: str
    category_hints: list[str]
    search_terms: list[str]
    authority_kind: str
    authority_score: float
    discovered_via: str
    authority_tier: str = "derived_or_inferred"
    authority_rank: int = 25
    authority_class: str | None = None
    authority_reason: str | None = None
    publication_type: str | None = None
    domain_name: str | None = None
    should_fetch: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "title": self.title,
            "publisher": self.publisher,
            "source_type": self.source_type,
            "category_hints": list(self.category_hints),
            "search_terms": list(self.search_terms),
            "authority_kind": self.authority_kind,
            "authority_score": float(self.authority_score),
            "authority_tier": self.authority_tier,
            "authority_rank": int(self.authority_rank),
            "authority_class": self.authority_class,
            "authority_reason": self.authority_reason,
            "publication_type": self.publication_type,
            "domain_name": self.domain_name,
            "discovered_via": self.discovered_via,
            "should_fetch": bool(self.should_fetch),
        }


def _utcnow() -> datetime:
    return datetime.utcnow()


def _norm_state(value: Optional[str]) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip().lower()
    return out or None


def _norm_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip()
    return out or None


def _slugify(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw).strip("-")
    return raw or "unknown"


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _json_loads_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _json_loads_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _source_name_from_url(url: str) -> str:
    host = urlparse(url).netloc.strip().lower()
    if not host:
        return "unknown_source"
    return host


def _publication_type_from_url(url: str) -> str:
    lower = (url or "").strip().lower()
    if lower.endswith(".pdf"):
        return "official_document"
    if any(token in lower for token in ("ordinance", "code", "statute", "regulation", "ecfr", "federalregister")):
        return "legal_code"
    if any(token in lower for token in ("forms", "packet", "application", "checklist")):
        return "official_form"
    if any(token in lower for token in ("notice", "bulletin", "faq", "program")):
        return "guidance_page"
    return "web_page"


def _authority_rank_for_tier(tier: str) -> int:
    return int(AUTHORITY_TIER_RANKS.get(str(tier or "derived_or_inferred"), 25))


def _classify_authority_tier(*, url: str, publisher: Optional[str] = None, title: Optional[str] = None, source_type: Optional[str] = None, source_kind: Optional[str] = None) -> dict[str, Any]:
    host = urlparse(url).netloc.strip().lower()
    publisher_text = (publisher or "").strip().lower()
    title_text = (title or "").strip().lower()
    source_type_text = (source_type or "").strip().lower()
    source_kind_text = (source_kind or "").strip().lower()
    publication_type = _publication_type_from_url(url)
    authority_tier = "derived_or_inferred"
    authority_class = "private_site"
    authority_kind = "private_site"
    authority_reason = "No government or approved official signal detected."
    authority_score = 0.35
    if host.endswith(".gov") or ".gov." in host or host.endswith(".mi.us") or host.endswith(".us"):
        authority_tier = "authoritative_official"
        authority_class = "official_government"
        authority_kind = "official_government"
        authority_reason = "Official government domain."
        authority_score = 0.99
    elif any(token in source_kind_text for token in ("federal_anchor", "state_anchor", "municipal_code")):
        authority_tier = "authoritative_official"
        authority_class = "legal_primary"
        authority_kind = "catalog_primary"
        authority_reason = "Catalog source kind indicates primary official/legal source."
        authority_score = 0.96
    elif any(token in source_kind_text for token in ("city_program_page", "county_program_page", "state_program_page", "housing_authority", "pha_plan", "pha_guidance")) or "housing authority" in publisher_text or "housing commission" in publisher_text or "housing authority" in title_text:
        authority_tier = "approved_official_supporting"
        authority_class = "official_supporting"
        authority_kind = "official_supporting"
        authority_reason = "Approved supporting official/program source."
        authority_score = 0.86
    elif any(token in source_type_text for token in ("program", "city", "county", "state", "federal")) or host.endswith(".org"):
        authority_tier = "semi_authoritative_operational"
        authority_class = "operational_program" if source_type_text else "organizational"
        authority_kind = authority_class
        authority_reason = "Operational or organizational source, useful but not primary legal authority."
        authority_score = 0.65
    authority_rank = _authority_rank_for_tier(authority_tier)
    return {
        "authority_kind": authority_kind,
        "authority_score": float(round(authority_score, 3)),
        "authority_tier": authority_tier,
        "authority_rank": authority_rank,
        "authority_class": authority_class,
        "authority_reason": authority_reason,
        "publication_type": publication_type,
        "domain_name": host,
        "approved_supporting_source": authority_tier == "approved_official_supporting",
        "semi_authoritative": authority_tier == "semi_authoritative_operational",
        "derived_or_inferred": authority_tier == "derived_or_inferred",
        "is_official": authority_tier == "authoritative_official",
    }


def _fingerprint_for_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _refresh_run_id(*, source_id: int | None = None) -> str:
    suffix = f"{int(source_id)}" if source_id is not None else "unknown"
    return f"source-refresh-{suffix}-{int(_utcnow().timestamp())}"


def _jurisdiction_slug(
    *,
    source_type: str,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    program_type: Optional[str],
) -> str:
    st = state.lower()
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    program = _norm_text(program_type)

    if source_type == "federal":
        return f"federal:{st}"
    if source_type == "state":
        return f"state:{st}"
    if source_type == "county":
        return f"county:{st}:{cnty or 'unknown'}"
    if source_type == "city":
        return f"city:{st}:{cnty or 'unknown'}:{cty or 'unknown'}"
    if source_type == "program":
        base = (pha or program or "program").strip().lower().replace(" ", "-")
        return f"program:{st}:{base}"
    if cnty or cty:
        return f"local:{st}:{cnty or '-'}:{cty or '-'}"
    return st


def _source_type_from_entry(entry: PolicyCatalogEntry) -> str:
    kind = (entry.source_kind or "").strip().lower()
    url = (entry.url or "").strip().lower()
    publisher = (entry.publisher or "").strip().lower()
    title = (entry.title or "").strip().lower()

    if any(token in kind for token in ("federal", "hud", "ecfr", "federal_anchor")):
        return "federal"
    if any(token in kind for token in ("state", "mshda", "state_anchor")):
        return "state"
    if "county" in kind:
        return "county"
    if any(token in kind for token in ("pha", "housing_authority", "voucher", "program")):
        return "program"
    if any(domain in url for domain in ("hud.gov", "ecfr.gov", "federalregister.gov")):
        return "federal"
    if "michigan.gov" in url or "legislature.mi.gov" in url:
        return "state"
    if "housing" in title and "authority" in title:
        return "program"
    if "housing commission" in publisher:
        return "program"
    if entry.city:
        return "city"
    if entry.county:
        return "county"
    return "local"


def _fetch_method_from_url(url: str) -> str:
    lower = (url or "").lower()
    if lower.endswith(".pdf"):
        return "http_pdf"
    if lower.startswith("http://") or lower.startswith("https://"):
        return "http_get"
    return "manual"


def _trust_level(entry: PolicyCatalogEntry) -> float:
    if bool(entry.is_authoritative):
        if entry.priority <= 20:
            return 0.98
        if entry.priority <= 50:
            return 0.92
        return 0.85
    if entry.priority <= 50:
        return 0.70
    return 0.55


def _refresh_interval_days(entry: PolicyCatalogEntry) -> int:
    kind = (entry.source_kind or "").lower()
    if "federal" in kind or "pha" in kind or "voucher" in kind:
        return 14
    if "municipal" in kind or "inspection" in kind or "registration" in kind:
        return 21
    if "state" in kind:
        return 30
    return 30


def _effective_refresh_interval_days(source: PolicySource) -> int:
    try:
        value = int(getattr(source, "refresh_interval_days", 0) or 0)
        if value > 0:
            return value
    except Exception:
        pass
    return 30


def _compute_next_refresh_due_at(source: PolicySource, *, from_dt: Optional[datetime] = None) -> datetime:
    base = from_dt or getattr(source, "last_fetched_at", None) or _utcnow()
    return base + timedelta(days=_effective_refresh_interval_days(source))


def _safe_text_from_http_response(resp: httpx.Response) -> str:
    content_type = (resp.headers.get("content-type") or "").lower()
    if "text" in content_type or "json" in content_type or "html" in content_type or "xml" in content_type:
        return resp.text or ""
    try:
        return resp.text or ""
    except Exception:
        return ""


def _sync_registry_defaults(source: PolicySource) -> None:
    if not getattr(source, "source_name", None):
        source.source_name = getattr(source, "publisher", None) or getattr(source, "title", None) or _source_name_from_url(source.url)
    if not getattr(source, "source_type", None):
        source.source_type = "local"
    if not getattr(source, "jurisdiction_slug", None):
        source.jurisdiction_slug = _jurisdiction_slug(
            source_type=str(getattr(source, "source_type", None) or "local"),
            state=_norm_state(getattr(source, "state", None)),
            county=getattr(source, "county", None),
            city=getattr(source, "city", None),
            pha_name=getattr(source, "pha_name", None),
            program_type=getattr(source, "program_type", None),
        )
    if not getattr(source, "fetch_method", None):
        source.fetch_method = _fetch_method_from_url(source.url)
    if not getattr(source, "fingerprint_algo", None):
        source.fingerprint_algo = "sha256"
    if not getattr(source, "registry_status", None):
        source.registry_status = "active"
    authority = _classify_authority_tier(
        url=getattr(source, "url", "") or "",
        publisher=getattr(source, "publisher", None),
        title=getattr(source, "title", None),
        source_type=getattr(source, "source_type", None),
        source_kind=_json_loads_dict(getattr(source, "registry_meta_json", None)).get("source_kind"),
    )
    if not getattr(source, "authority_tier", None):
        source.authority_tier = authority["authority_tier"]
    if not getattr(source, "authority_rank", None):
        source.authority_rank = authority["authority_rank"]
    if not getattr(source, "authority_class", None):
        source.authority_class = authority["authority_class"]
    if not getattr(source, "authority_reason", None):
        source.authority_reason = authority["authority_reason"]
    if not getattr(source, "publication_type", None):
        source.publication_type = authority["publication_type"]
    if not getattr(source, "domain_name", None):
        source.domain_name = authority["domain_name"]
    source.approved_supporting_source = bool(getattr(source, "approved_supporting_source", False) or authority["approved_supporting_source"])
    source.semi_authoritative = bool(getattr(source, "semi_authoritative", False) or authority["semi_authoritative"])
    source.derived_or_inferred = bool(authority["derived_or_inferred"])
    source.authority_score = max(float(getattr(source, "authority_score", 0.0) or 0.0), float(authority["authority_score"]))
    if getattr(source, "next_refresh_due_at", None) is None:
        source.next_refresh_due_at = _compute_next_refresh_due_at(source)
    if getattr(source, "source_metadata_json", None) is None:
        source.source_metadata_json = "{}"
    if getattr(source, "fetch_config_json", None) is None:
        source.fetch_config_json = "{}"
    if getattr(source, "registry_meta_json", None) is None:
        source.registry_meta_json = "{}"


def _append_note_marker(existing_notes: Optional[str], marker: str) -> str:
    notes = (existing_notes or "").strip()
    if marker.lower() in notes.lower():
        return notes
    if not notes:
        return marker
    return f"{notes} {marker}"


def _base_domains_for_scope(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[dict[str, Any]]:
    state_lower = _norm_state(state).lower()
    county_slug = _slugify(county)
    city_slug = _slugify(city)
    pha_slug = _slugify(pha_name)

    candidates: list[dict[str, Any]] = [
        {"domain": "www.hud.gov", "source_type": "federal", "publisher": "HUD"},
        {"domain": "www.ecfr.gov", "source_type": "federal", "publisher": "eCFR"},
        {"domain": "www.michigan.gov", "source_type": "state", "publisher": "State of Michigan"},
        {"domain": "www.legislature.mi.gov", "source_type": "state", "publisher": "Michigan Legislature"},
    ]

    if county_slug != "unknown":
        candidates.extend(
            [
                {"domain": f"www.{county_slug}countymi.gov", "source_type": "county", "publisher": f"{county} County"},
                {"domain": f"www.{county_slug}county.gov", "source_type": "county", "publisher": f"{county} County"},
                {"domain": f"www.co.{county_slug}.mi.us", "source_type": "county", "publisher": f"{county} County"},
            ]
        )

    if city_slug != "unknown":
        candidates.extend(
            [
                {"domain": f"www.cityof{city_slug}.org", "source_type": "city", "publisher": city},
                {"domain": f"www.cityof{city_slug}.com", "source_type": "city", "publisher": city},
                {"domain": f"www.{city_slug}.mi.us", "source_type": "city", "publisher": city},
                {"domain": f"www.{city_slug}{state_lower}.gov", "source_type": "city", "publisher": city},
                {"domain": f"www.ci.{city_slug}.mi.us", "source_type": "city", "publisher": city},
            ]
        )

    if pha_slug != "unknown":
        candidates.extend(
            [
                {"domain": f"www.{pha_slug}.org", "source_type": "program", "publisher": pha_name},
                {"domain": f"www.{pha_slug}.gov", "source_type": "program", "publisher": pha_name},
            ]
        )

    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for candidate in candidates:
        domain = str(candidate.get("domain") or "").strip().lower()
        if not domain or domain in seen:
            continue
        seen.add(domain)
        output.append(candidate)
    return output


def _discovery_terms_for_category(category: str) -> list[str]:
    lookup: dict[str, list[str]] = {
        "registration": ["rental registration", "landlord registration", "registration form"],
        "inspection": ["rental inspection", "inspection checklist", "rental inspection program"],
        "occupancy": ["certificate of occupancy", "occupancy certificate", "occupancy permit"],
        "safety": ["property maintenance code", "housing code", "safety checklist"],
        "lead": ["lead paint", "lead safe", "lead hazard"],
        "permits": ["building permit", "permits", "rehab permit"],
        "section8": ["housing choice voucher", "section 8 landlord", "voucher inspection"],
        "program_overlay": ["administrative plan", "voucher landlord packet", "HAP contract"],
        "documents": ["application packet", "rental forms", "landlord forms"],
        "fees": ["fee schedule", "inspection fees", "license fee"],
        "contacts": ["housing department", "rental inspections", "landlord contact"],
        "rental_license": ["rental license", "rental certificate", "landlord license"],
        "source_of_income": ["source of income", "fair housing", "income discrimination"],
        "zoning": ["zoning code", "land use", "zoning ordinance"],
        "tax": ["property tax", "tax assessor"],
        "utilities": ["water billing", "sewer billing", "utilities"],
    }
    return lookup.get(category, [category_label(category)])


def _paths_for_category(category: str) -> list[str]:
    lookup: dict[str, list[str]] = {
        "registration": [
            "/departments/rental-inspections",
            "/departments/rental-inspections-division",
            "/rental-registration",
            "/landlord-registration",
            "/documents/rental-registration.pdf",
        ],
        "inspection": [
            "/departments/rental-inspections",
            "/inspections/rental",
            "/rental-inspection",
            "/documents/rental-inspection-checklist.pdf",
        ],
        "occupancy": [
            "/certificate-of-occupancy",
            "/occupancy",
            "/building/certificate-of-occupancy",
            "/documents/certificate-of-occupancy.pdf",
        ],
        "safety": [
            "/property-maintenance",
            "/housing-code",
            "/code-enforcement",
            "/documents/property-maintenance-code.pdf",
        ],
        "lead": [
            "/lead",
            "/lead-safe",
            "/documents/lead-safe.pdf",
        ],
        "permits": [
            "/permits",
            "/building/permits",
            "/documents/permit-application.pdf",
        ],
        "section8": [
            "/housing-choice-voucher",
            "/section-8",
            "/landlords/housing-choice-voucher",
            "/documents/hcv-landlord-packet.pdf",
        ],
        "program_overlay": [
            "/administrative-plan",
            "/documents/administrative-plan.pdf",
            "/landlords",
            "/voucher-program",
        ],
        "documents": [
            "/documents",
            "/forms",
            "/landlords/forms",
            "/documents/rental-application-packet.pdf",
        ],
        "fees": [
            "/fees",
            "/documents/fee-schedule.pdf",
            "/rental-fees",
        ],
        "contacts": [
            "/contact",
            "/contact-us",
            "/departments",
            "/housing",
        ],
        "rental_license": [
            "/rental-license",
            "/rental-licensing",
            "/documents/rental-license-application.pdf",
        ],
        "source_of_income": [
            "/fair-housing",
            "/civil-rights",
            "/documents/fair-housing.pdf",
        ],
    }
    return lookup.get(category, ["/"])


def _title_for_candidate(
    *,
    city: Optional[str],
    county: Optional[str],
    pha_name: Optional[str],
    category: str,
    source_type: str,
) -> str:
    scope = city or county or pha_name or "jurisdiction"
    return f"{scope} {category_label(category)} source ({source_type})"


def classify_discovery_authority(
    *,
    url: str,
    publisher: Optional[str] = None,
    title: Optional[str] = None,
    source_type: Optional[str] = None,
    source_kind: Optional[str] = None,
) -> dict[str, Any]:
    return _classify_authority_tier(
        url=url,
        publisher=publisher,
        title=title,
        source_type=source_type,
        source_kind=source_kind,
    )


def _probe_discovery_candidate(
    *,
    url: str,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    try:
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
            resp = client.get(url)
            content_type = resp.headers.get("content-type")
            ok = 200 <= int(resp.status_code) < 400
            text = _safe_text_from_http_response(resp)
            return {
                "ok": ok,
                "http_status": int(resp.status_code),
                "content_type": content_type,
                "title": _extract_title(text),
                "fetch_error": None if ok else f"http_status_{int(resp.status_code)}",
            }
    except Exception as exc:
        return {
            "ok": False,
            "http_status": None,
            "content_type": None,
            "title": None,
            "fetch_error": f"{type(exc).__name__}: {exc}",
        }


def _extract_title(text: str) -> Optional[str]:
    raw = text or ""
    match = re.search(r"<title>\s*(.*?)\s*</title>", raw, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return title or None


def policy_source_origin(source: PolicySource) -> str:
    metadata = _json_loads_dict(getattr(source, "source_metadata_json", None))
    discovery = metadata.get("discovery") if isinstance(metadata.get("discovery"), dict) else {}
    if discovery.get("mode") == "discovered":
        return "discovered"
    notes = (getattr(source, "notes", None) or "").lower()
    if DISCOVERY_NOTE_MARKER.lower() in notes:
        return "discovered"
    return "curated"


def policy_source_discovery_metadata(source: PolicySource) -> dict[str, Any]:
    metadata = _json_loads_dict(getattr(source, "source_metadata_json", None))
    return metadata.get("discovery", {}) if isinstance(metadata.get("discovery"), dict) else {}



def policy_source_needs_refresh(
    source: PolicySource,
    *,
    force: bool = False,
    now: Optional[datetime] = None,
) -> bool:
    if force:
        return True

    now = now or _utcnow()
    status = (getattr(source, "registry_status", None) or "active").strip().lower()
    refresh_state = (getattr(source, "refresh_state", None) or "pending").strip().lower()
    if refresh_state == "blocked":
        return False
    if status not in {"active", "candidate", "warning"}:
        return False

    validation_due_at = getattr(source, "validation_due_at", None)
    if bool(getattr(source, "revalidation_required", False)) and (
        validation_due_at is None or validation_due_at <= now
    ):
        return True

    if getattr(source, "last_fetched_at", None) is None:
        return True

    due_at = getattr(source, "next_refresh_due_at", None)
    if due_at is None:
        due_at = _compute_next_refresh_due_at(source, from_dt=getattr(source, "last_fetched_at", None))
        source.next_refresh_due_at = due_at

    freshness_status = (getattr(source, "freshness_status", None) or "").strip().lower()
    if freshness_status in {"stale", "fetch_failed", "unknown", "error"}:
        return True

    return now >= due_at


def merged_catalog_for_market(

    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicyCatalogEntry]:
    from app.services.policy_catalog_admin_service import merged_catalog_for_market as _merged_catalog_for_market

    return _merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )


def ensure_policy_source_from_catalog_entry(
    db: Session,
    *,
    entry: PolicyCatalogEntry,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
) -> PolicySource:
    state = _norm_state(entry.state)
    county = _norm_lower(entry.county)
    city = _norm_lower(entry.city)
    pha_name = _norm_text(entry.pha_name)
    program_type = _norm_text(entry.program_type)

    stmt = select(PolicySource).where(PolicySource.url == entry.url)
    if org_id is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
    existing = db.scalar(stmt.order_by(PolicySource.id.asc()))

    source_type = _source_type_from_entry(entry)
    authority = _classify_authority_tier(
        url=entry.url,
        publisher=entry.publisher,
        title=entry.title,
        source_type=source_type,
        source_kind=entry.source_kind,
    )
    if existing is None:
        source = PolicySource(
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
            publisher=entry.publisher,
            title=entry.title,
            url=entry.url,
            content_type=None,
            http_status=None,
            retrieved_at=None,
            content_sha256=None,
            raw_path=None,
            extracted_text=None,
            notes=_append_note_marker(entry.notes, CURATED_NOTE_MARKER),
            is_authoritative=bool(entry.is_authoritative),
            authority_score=float(authority["authority_score"]),
            authority_tier=str(authority["authority_tier"]),
            authority_rank=int(authority["authority_rank"]),
            authority_class=authority.get("authority_class"),
            authority_reason=authority.get("authority_reason"),
            publication_type=authority.get("publication_type"),
            domain_name=authority.get("domain_name"),
            approved_supporting_source=bool(authority.get("approved_supporting_source", False)),
            semi_authoritative=bool(authority.get("semi_authoritative", False)),
            derived_or_inferred=bool(authority.get("derived_or_inferred", False)),
            normalized_categories_json="[]",
            freshness_status="unknown",
            freshness_reason="not_fetched",
            freshness_checked_at=None,
            published_at=None,
            effective_date=None,
            last_verified_at=None,
            source_name=entry.publisher or entry.title or _source_name_from_url(entry.url),
            source_type=source_type,
            jurisdiction_slug=_jurisdiction_slug(
                source_type=source_type,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                program_type=program_type,
            ),
            fetch_method=_fetch_method_from_url(entry.url),
            trust_level=_trust_level(entry),
            refresh_interval_days=_refresh_interval_days(entry),
            last_fetched_at=None,
            registry_status="active",
            fetch_config_json=_json_dumps({"focus": focus}),
            registry_meta_json=_json_dumps(
                {
                    "catalog_entry_id": entry.id,
                    "baseline_url": entry.baseline_url,
                    "source_kind": entry.source_kind,
                    "priority": entry.priority,
                    "origin_mode": "curated",
                }
            ),
            fingerprint_algo="sha256",
            current_fingerprint=None,
            last_changed_at=None,
            next_refresh_due_at=None,
            last_fetch_error=None,
            last_http_status=None,
            last_seen_same_fingerprint_at=None,
            source_metadata_json=_json_dumps(
                {
                    "discovery": {
                        "mode": "curated",
                        "category_hints": _json_loads_list(getattr(entry, "normalized_categories_json", None)),
                        "authority_kind": "catalog_curated",
                        "authority_tier": authority["authority_tier"],
                        "authority_rank": authority["authority_rank"],
                        "authority_class": authority["authority_class"],
                        "authority_reason": authority["authority_reason"],
                        "publication_type": authority["publication_type"],
                        "domain_name": authority["domain_name"],
                    }
                }
            ),
            last_verified_by_user_id=None,
        )
        _sync_registry_defaults(source)
        db.add(source)
        db.flush()
        inventory_hints = expected_inventory_hints(state=_norm_state(state), county=county, city=city, pha_name=pha_name, include_section8=True)
        upsert_source_inventory_record(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=county,
            city=city,
            pha_name=pha_name,
            program_type="section8" if "section8" in set(candidate.category_hints) else None,
            url=candidate.url,
            title=probe_result.get("title") or candidate.title,
            publisher=candidate.publisher,
            source_type=candidate.source_type,
            publication_type=candidate.publication_type,
            category_hints=list(candidate.category_hints),
            search_terms=list(candidate.search_terms),
            expected_categories=inventory_hints.get("expected_categories"),
            expected_tiers=inventory_hints.get("expected_tiers"),
            authority_tier=candidate.authority_tier,
            authority_rank=candidate.authority_rank,
            authority_score=candidate.authority_score,
            lifecycle_state="active" if probe_result.get("ok") else "discovered",
            crawl_status="pending" if not probe_result.get("ok") else "queued",
            inventory_origin="discovered",
            policy_source_id=int(source.id),
            is_curated=False,
            is_official_candidate=bool(candidate.authority_rank >= 85),
            probe_result=probe_result,
            metadata={"focus": focus},
        )
        return source

    existing.state = state
    existing.county = county
    existing.city = city
    existing.pha_name = pha_name
    existing.program_type = program_type
    existing.publisher = entry.publisher
    existing.title = entry.title
    existing.notes = _append_note_marker(entry.notes or existing.notes, CURATED_NOTE_MARKER)
    existing.is_authoritative = bool(entry.is_authoritative)
    existing.source_name = entry.publisher or entry.title or existing.source_name or _source_name_from_url(entry.url)
    existing.source_type = source_type
    existing.jurisdiction_slug = _jurisdiction_slug(
        source_type=source_type,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
    )
    existing.fetch_method = existing.fetch_method or _fetch_method_from_url(entry.url)
    existing.trust_level = max(float(existing.trust_level or 0.0), _trust_level(entry))
    existing.authority_score = max(float(existing.authority_score or 0.0), float(authority["authority_score"]))
    existing.authority_tier = authority["authority_tier"]
    existing.authority_rank = authority["authority_rank"]
    existing.authority_class = authority["authority_class"]
    existing.authority_reason = authority["authority_reason"]
    existing.publication_type = authority["publication_type"]
    existing.domain_name = authority["domain_name"]
    existing.approved_supporting_source = bool(authority.get("approved_supporting_source", False))
    existing.semi_authoritative = bool(authority.get("semi_authoritative", False))
    existing.derived_or_inferred = bool(authority.get("derived_or_inferred", False))
    existing.refresh_interval_days = max(1, _refresh_interval_days(entry))

    meta = _json_loads_dict(existing.registry_meta_json)
    meta.update(
        {
            "catalog_entry_id": entry.id,
            "baseline_url": entry.baseline_url,
            "source_kind": entry.source_kind,
            "priority": entry.priority,
            "origin_mode": "curated",
        }
    )
    existing.registry_meta_json = _json_dumps(meta)

    source_metadata = _json_loads_dict(existing.source_metadata_json)
    source_metadata["discovery"] = {
        "mode": "curated",
        "category_hints": _json_loads_list(getattr(entry, "normalized_categories_json", None)),
        "authority_kind": "catalog_curated",
    }
    existing.source_metadata_json = _json_dumps(source_metadata)

    _sync_registry_defaults(existing)
    db.flush()
    inventory_hints = expected_inventory_hints(state=_norm_state(state), county=county, city=city, pha_name=pha_name, include_section8=True)
    upsert_source_inventory_record(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=getattr(existing, "program_type", None),
        url=candidate.url,
        title=probe_result.get("title") or candidate.title or existing.title,
        publisher=candidate.publisher or existing.publisher,
        source_type=existing.source_type,
        publication_type=existing.publication_type,
        category_hints=list(candidate.category_hints),
        search_terms=list(candidate.search_terms),
        expected_categories=inventory_hints.get("expected_categories"),
        expected_tiers=inventory_hints.get("expected_tiers"),
        authority_tier=existing.authority_tier,
        authority_rank=existing.authority_rank,
        authority_score=existing.authority_score,
        lifecycle_state=(existing.registry_status or "active").lower(),
        crawl_status="queued" if probe_result.get("ok") else "pending",
        inventory_origin="discovered",
        policy_source_id=int(existing.id),
        is_curated=bool(policy_source_origin(existing) == "curated"),
        is_official_candidate=bool(existing.authority_rank >= 85),
        probe_result=probe_result,
        metadata={"focus": focus},
    )
    return existing


def _existing_source_urls(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> set[str]:
    rows = list_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    return {str(row.url or "").strip().lower() for row in rows if getattr(row, "url", None)}


def _candidate_urls_for_scope(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    missing_categories: list[str],
) -> list[PolicySourceDiscoveryCandidate]:
    base_domains = _base_domains_for_scope(state=state, county=county, city=city, pha_name=pha_name)
    output: list[PolicySourceDiscoveryCandidate] = []

    for category in missing_categories:
        search_terms = _discovery_terms_for_category(category)
        paths = _paths_for_category(category)
        for base in base_domains:
            domain = str(base.get("domain") or "").strip()
            source_type = str(base.get("source_type") or "local").strip()
            publisher = base.get("publisher")
            for path in paths:
                url = f"https://{domain}{path}"
                authority = classify_discovery_authority(url=url, publisher=publisher, title=category_label(category), source_type=source_type)
                output.append(
                    PolicySourceDiscoveryCandidate(
                        url=url,
                        title=_title_for_candidate(
                            city=city,
                            county=county,
                            pha_name=pha_name,
                            category=category,
                            source_type=source_type,
                        ),
                        publisher=str(publisher).strip() if publisher else None,
                        source_type=source_type,
                        category_hints=[category],
                        search_terms=list(search_terms),
                        authority_kind=str(authority["authority_kind"]),
                        authority_score=float(authority["authority_score"]),
                        authority_tier=str(authority["authority_tier"]),
                        authority_rank=int(authority["authority_rank"]),
                        authority_class=authority.get("authority_class"),
                        authority_reason=authority.get("authority_reason"),
                        publication_type=authority.get("publication_type"),
                        domain_name=authority.get("domain_name"),
                        discovered_via="missing_category_generation",
                        should_fetch=True,
                    )
                )

    if pha_name:
        for suffix in (
            "/housing-choice-voucher",
            "/documents/administrative-plan.pdf",
            "/landlords",
            "/section-8",
        ):
            domain = f"www.{_slugify(pha_name)}.org"
            url = f"https://{domain}{suffix}"
            authority = classify_discovery_authority(url=url, publisher=pha_name, title="Section 8 overlay", source_type="program")
            output.append(
                PolicySourceDiscoveryCandidate(
                    url=url,
                    title=f"{pha_name} program overlay source",
                    publisher=pha_name,
                    source_type="program",
                    category_hints=["section8", "program_overlay", "contacts"],
                    search_terms=["section 8", "housing choice voucher", "administrative plan"],
                    authority_kind=str(authority["authority_kind"]),
                    authority_score=float(authority["authority_score"]),
                    authority_tier=str(authority["authority_tier"]),
                    authority_rank=int(authority["authority_rank"]),
                    authority_class=authority.get("authority_class"),
                    authority_reason=authority.get("authority_reason"),
                    publication_type=authority.get("publication_type"),
                    domain_name=authority.get("domain_name"),
                    discovered_via="pha_overlay_generation",
                    should_fetch=True,
                )
            )

    deduped: dict[str, PolicySourceDiscoveryCandidate] = {}
    for row in output:
        key = row.url.strip().lower()
        existing = deduped.get(key)
        if existing is None or row.authority_score > existing.authority_score:
            deduped[key] = row

    return sorted(
        deduped.values(),
        key=lambda item: (-item.authority_score, item.url),
    )


def _upsert_discovered_source(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    candidate: PolicySourceDiscoveryCandidate,
    probe_result: dict[str, Any],
    focus: str,
) -> PolicySource:
    stmt = select(PolicySource).where(PolicySource.url == candidate.url)
    if org_id is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
    existing = db.scalar(stmt.order_by(PolicySource.id.asc()))

    metadata_payload = {
        "discovery": {
            "mode": "discovered",
            "category_hints": list(candidate.category_hints),
            "search_terms": list(candidate.search_terms),
            "authority_kind": candidate.authority_kind,
            "authority_score": candidate.authority_score,
            "authority_tier": candidate.authority_tier,
            "authority_rank": candidate.authority_rank,
            "authority_class": candidate.authority_class,
            "authority_reason": candidate.authority_reason,
            "publication_type": candidate.publication_type,
            "domain_name": candidate.domain_name,
            "discovered_via": candidate.discovered_via,
            "discovered_at": _utcnow().isoformat(),
            "probe_result": {
                "ok": probe_result.get("ok"),
                "http_status": probe_result.get("http_status"),
                "content_type": probe_result.get("content_type"),
                "fetch_error": probe_result.get("fetch_error"),
            },
        }
    }

    registry_meta = {
        "origin_mode": "discovered",
        "focus": focus,
        "category_hints": list(candidate.category_hints),
        "authority_kind": candidate.authority_kind,
        "authority_score": candidate.authority_score,
        "authority_tier": candidate.authority_tier,
        "authority_rank": candidate.authority_rank,
        "authority_class": candidate.authority_class,
        "authority_reason": candidate.authority_reason,
        "publication_type": candidate.publication_type,
        "domain_name": candidate.domain_name,
    }

    if existing is None:
        source = PolicySource(
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
            program_type="section8" if "section8" in set(candidate.category_hints) else None,
            publisher=candidate.publisher,
            title=probe_result.get("title") or candidate.title,
            url=candidate.url,
            content_type=probe_result.get("content_type"),
            http_status=probe_result.get("http_status"),
            retrieved_at=None,
            content_sha256=None,
            raw_path=None,
            extracted_text=None,
            notes=_append_note_marker(None, DISCOVERY_NOTE_MARKER),
            is_authoritative=bool(candidate.authority_score >= 0.85),
            authority_score=float(candidate.authority_score),
            authority_tier=str(candidate.authority_tier),
            authority_rank=int(candidate.authority_rank),
            authority_class=candidate.authority_class,
            authority_reason=candidate.authority_reason,
            publication_type=candidate.publication_type,
            domain_name=candidate.domain_name,
            approved_supporting_source=bool(candidate.authority_tier == "approved_official_supporting"),
            semi_authoritative=bool(candidate.authority_tier == "semi_authoritative_operational"),
            derived_or_inferred=bool(candidate.authority_tier == "derived_or_inferred"),
            normalized_categories_json="[]",
            freshness_status="unknown",
            freshness_reason="discovered_not_fetched",
            freshness_checked_at=_utcnow(),
            published_at=None,
            effective_date=None,
            last_verified_at=None,
            source_name=candidate.publisher or _source_name_from_url(candidate.url),
            source_type=candidate.source_type,
            jurisdiction_slug=_jurisdiction_slug(
                source_type=candidate.source_type,
                state=_norm_state(state),
                county=county,
                city=city,
                pha_name=pha_name,
                program_type="section8" if "section8" in set(candidate.category_hints) else None,
            ),
            fetch_method=_fetch_method_from_url(candidate.url),
            trust_level=float(round(max(0.35, candidate.authority_score), 3)),
            refresh_interval_days=21,
            last_fetched_at=None,
            registry_status="candidate" if not probe_result.get("ok") else "active",
            fetch_config_json=_json_dumps({"focus": focus, "discovery_candidate": True}),
            registry_meta_json=_json_dumps(registry_meta),
            fingerprint_algo="sha256",
            current_fingerprint=None,
            last_changed_at=None,
            next_refresh_due_at=_utcnow(),
            last_fetch_error=probe_result.get("fetch_error"),
            last_http_status=probe_result.get("http_status"),
            last_seen_same_fingerprint_at=None,
            source_metadata_json=_json_dumps(metadata_payload),
            last_verified_by_user_id=None,
        )
        _sync_registry_defaults(source)
        db.add(source)
        db.flush()
        inventory_hints = expected_inventory_hints(state=_norm_state(state), county=county, city=city, pha_name=pha_name, include_section8=True)
        upsert_source_inventory_record(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=county,
            city=city,
            pha_name=pha_name,
            program_type="section8" if "section8" in set(candidate.category_hints) else None,
            url=candidate.url,
            title=probe_result.get("title") or candidate.title,
            publisher=candidate.publisher,
            source_type=candidate.source_type,
            publication_type=candidate.publication_type,
            category_hints=list(candidate.category_hints),
            search_terms=list(candidate.search_terms),
            expected_categories=inventory_hints.get("expected_categories"),
            expected_tiers=inventory_hints.get("expected_tiers"),
            authority_tier=candidate.authority_tier,
            authority_rank=candidate.authority_rank,
            authority_score=candidate.authority_score,
            lifecycle_state="active" if probe_result.get("ok") else "discovered",
            crawl_status="pending" if not probe_result.get("ok") else "queued",
            inventory_origin="discovered",
            policy_source_id=int(source.id),
            is_curated=False,
            is_official_candidate=bool(candidate.authority_rank >= 85),
            probe_result=probe_result,
            metadata={"focus": focus},
        )
        return source

    existing.state = _norm_state(state)
    existing.county = _norm_lower(county)
    existing.city = _norm_lower(city)
    existing.pha_name = _norm_text(pha_name)
    existing.publisher = candidate.publisher or existing.publisher
    existing.title = probe_result.get("title") or candidate.title or existing.title
    existing.notes = _append_note_marker(existing.notes, DISCOVERY_NOTE_MARKER)
    existing.source_name = candidate.publisher or existing.source_name or _source_name_from_url(candidate.url)
    existing.source_type = candidate.source_type or existing.source_type
    existing.jurisdiction_slug = _jurisdiction_slug(
        source_type=str(existing.source_type or candidate.source_type or "local"),
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=getattr(existing, "program_type", None),
    )
    existing.fetch_method = existing.fetch_method or _fetch_method_from_url(candidate.url)
    existing.trust_level = max(float(existing.trust_level or 0.0), float(candidate.authority_score))
    existing.is_authoritative = bool(existing.is_authoritative or candidate.authority_score >= 0.85)
    existing.authority_score = max(float(existing.authority_score or 0.0), float(candidate.authority_score))
    existing.authority_tier = candidate.authority_tier or existing.authority_tier
    existing.authority_rank = max(int(getattr(existing, "authority_rank", 0) or 0), int(candidate.authority_rank or 0))
    existing.authority_class = candidate.authority_class or existing.authority_class
    existing.authority_reason = candidate.authority_reason or existing.authority_reason
    existing.publication_type = candidate.publication_type or existing.publication_type
    existing.domain_name = candidate.domain_name or existing.domain_name
    existing.approved_supporting_source = bool(existing.approved_supporting_source or candidate.authority_tier == "approved_official_supporting")
    existing.semi_authoritative = bool(existing.semi_authoritative or candidate.authority_tier == "semi_authoritative_operational")
    existing.derived_or_inferred = bool(candidate.authority_tier == "derived_or_inferred")
    existing.registry_status = existing.registry_status or ("candidate" if not probe_result.get("ok") else "active")
    existing.last_fetch_error = probe_result.get("fetch_error") or existing.last_fetch_error
    existing.last_http_status = probe_result.get("http_status") or existing.last_http_status
    existing.content_type = probe_result.get("content_type") or existing.content_type
    existing.freshness_checked_at = _utcnow()
    existing.next_refresh_due_at = _utcnow()

    meta = _json_loads_dict(existing.registry_meta_json)
    meta.update(registry_meta)
    existing.registry_meta_json = _json_dumps(meta)

    source_metadata = _json_loads_dict(existing.source_metadata_json)
    existing_discovery = source_metadata.get("discovery") if isinstance(source_metadata.get("discovery"), dict) else {}
    merged_hints = sorted(set(_json_loads_list(existing_discovery.get("category_hints")) + list(candidate.category_hints)))
    source_metadata["discovery"] = {
        **existing_discovery,
        **metadata_payload["discovery"],
        "category_hints": merged_hints,
    }
    existing.source_metadata_json = _json_dumps(source_metadata)

    _sync_registry_defaults(existing)
    db.flush()
    inventory_hints = expected_inventory_hints(state=_norm_state(state), county=county, city=city, pha_name=pha_name, include_section8=True)
    upsert_source_inventory_record(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=getattr(existing, "program_type", None),
        url=candidate.url,
        title=probe_result.get("title") or candidate.title or existing.title,
        publisher=candidate.publisher or existing.publisher,
        source_type=existing.source_type,
        publication_type=existing.publication_type,
        category_hints=list(candidate.category_hints),
        search_terms=list(candidate.search_terms),
        expected_categories=inventory_hints.get("expected_categories"),
        expected_tiers=inventory_hints.get("expected_tiers"),
        authority_tier=existing.authority_tier,
        authority_rank=existing.authority_rank,
        authority_score=existing.authority_score,
        lifecycle_state=(existing.registry_status or "active").lower(),
        crawl_status="queued" if probe_result.get("ok") else "pending",
        inventory_origin="discovered",
        policy_source_id=int(existing.id),
        is_curated=bool(policy_source_origin(existing) == "curated"),
        is_official_candidate=bool(existing.authority_rank >= 85),
        probe_result=probe_result,
        metadata={"focus": focus},
    )
    return existing


def build_source_discovery_candidates(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    missing_categories: list[str] | None = None,
    max_candidates: int = DEFAULT_DISCOVERY_MAX_CANDIDATES,
) -> list[dict[str, Any]]:
    categories = [str(item).strip().lower() for item in (missing_categories or []) if str(item).strip()]
    if not categories:
        return []
    candidates = _candidate_urls_for_scope(
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        missing_categories=categories,
    )
    return [row.as_dict() for row in candidates[: max(1, int(max_candidates))]]


def discover_policy_sources_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    missing_categories: list[str] | None = None,
    focus: str = "se_mi_extended",
    max_candidates: int = DEFAULT_DISCOVERY_MAX_CANDIDATES,
    probe: bool = True,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    missing = [str(item).strip().lower() for item in (missing_categories or []) if str(item).strip()]

    if not missing:
        return {
            "ok": True,
            "discovery_triggered": False,
            "reason": "no_missing_categories",
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
            "missing_categories": [],
            "candidate_count": 0,
            "created_count": 0,
            "existing_count": 0,
            "created_source_ids": [],
            "candidates": [],
            "results": [],
        }

    existing_urls = _existing_source_urls(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    raw_candidates = _candidate_urls_for_scope(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        missing_categories=missing,
    )

    selected = []
    seen_urls: set[str] = set(existing_urls)
    for candidate in raw_candidates:
        key = candidate.url.strip().lower()
        if key in seen_urls:
            continue
        seen_urls.add(key)
        selected.append(candidate)
        if len(selected) >= max(1, int(max_candidates)):
            break

    results: list[dict[str, Any]] = []
    created_source_ids: list[int] = []
    created_count = 0
    existing_count = 0

    for candidate in selected:
        probe_result = _probe_discovery_candidate(url=candidate.url, timeout_seconds=timeout_seconds) if probe else {
            "ok": False,
            "http_status": None,
            "content_type": None,
            "title": None,
            "fetch_error": "probe_skipped",
        }

        should_persist = bool(probe_result.get("ok")) or candidate.authority_score >= 0.85
        persisted_source_id: int | None = None

        if should_persist:
            source = _upsert_discovered_source(
                db,
                org_id=org_id,
                state=st,
                county=cnty,
                city=cty,
                pha_name=pha,
                candidate=candidate,
                probe_result=probe_result,
                focus=focus,
            )
            persisted_source_id = int(source.id)
            created_source_ids.append(persisted_source_id)
            if policy_source_origin(source) == "discovered":
                created_count += 1
            else:
                existing_count += 1

        results.append(
            {
                "candidate": candidate.as_dict(),
                "probe_result": probe_result,
                "persisted": should_persist,
                "policy_source_id": persisted_source_id,
            }
        )

    inventory_hints = expected_inventory_hints(state=st, county=cnty, city=cty, pha_name=pha, include_section8=True)
    discovered_urls = [candidate.url for candidate in selected]
    search_query = " | ".join(missing) if missing else None
    record_discovery_attempt(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type="section8" if "section8" in set(missing) else None,
        query_text=search_query,
        searched_categories=missing,
        searched_tiers=inventory_hints.get("expected_tiers"),
        result_urls=discovered_urls,
        attempt_type="discovery",
        status="completed",
        not_found=not bool(discovered_urls),
        metadata={"focus": focus, "probe": probe, "candidate_count": len(selected)},
    )
    if not discovered_urls and missing:
        mark_inventory_not_found(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            program_type="section8" if "section8" in set(missing) else None,
            expected_categories=missing,
            expected_tiers=inventory_hints.get("expected_tiers"),
            search_terms=missing,
            metadata={"focus": focus, "reason": "no_candidates"},
        )
    db.commit()

    return {
        "ok": True,
        "discovery_triggered": True,
        "reason": "missing_categories",
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "missing_categories": missing,
        "candidate_count": len(selected),
        "created_count": created_count,
        "existing_count": existing_count,
        "created_source_ids": sorted(set(created_source_ids)),
        "candidates": [candidate.as_dict() for candidate in selected],
        "results": results,
        "inventory_summary": summarize_inventory_for_scope(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            program_type="section8" if "section8" in set(missing) else None,
        ),
    }


def queue_policy_source_discovery(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    missing_categories: list[str] | None = None,
    focus: str = "se_mi_extended",
    max_candidates: int = DEFAULT_DISCOVERY_MAX_CANDIDATES,
) -> dict[str, Any]:
    return discover_policy_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        missing_categories=missing_categories,
        focus=focus,
        max_candidates=max_candidates,
        probe=False,
    )


def collect_catalog_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicySource]:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    rows: list[PolicySource] = []
    inventory_hints = expected_inventory_hints(
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
    )
    for item in items:
        source = ensure_policy_source_from_catalog_entry(
            db,
            entry=item,
            org_id=org_id,
            focus=focus,
        )
        sync_policy_source_into_inventory(
            db,
            source=source,
            org_id=org_id,
            expected_categories=inventory_hints.get("expected_categories"),
            expected_tiers=inventory_hints.get("expected_tiers"),
            inventory_origin="catalog_sync",
            is_curated=True,
        )
        rows.append(source)
    db.commit()
    return rows


def list_sources_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> list[PolicySource]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicySource).where(PolicySource.state == st)
    if org_id is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))

    rows = list(db.scalars(stmt.order_by(PolicySource.is_authoritative.desc(), PolicySource.id.asc())).all())
    out: list[PolicySource] = []

    for row in rows:
        row_county = _norm_lower(getattr(row, "county", None))
        row_city = _norm_lower(getattr(row, "city", None))
        row_pha = _norm_text(getattr(row, "pha_name", None))

        if row_county is not None and row_county != cnty:
            continue
        if row_city is not None and row_city != cty:
            continue
        if pha is not None and row_pha not in {None, pha}:
            continue

        _sync_registry_defaults(row)
        out.append(row)

    db.commit()
    return out



def get_policy_source_refresh_snapshot(source: PolicySource) -> dict[str, Any]:
    _sync_registry_defaults(source)
    return {
        "source_id": int(getattr(source, "id", 0) or 0),
        "url": getattr(source, "url", None),
        "registry_status": getattr(source, "registry_status", None),
        "freshness_status": getattr(source, "freshness_status", None),
        "authority_tier": getattr(source, "authority_tier", None),
        "authority_rank": getattr(source, "authority_rank", None),
        "authority_class": getattr(source, "authority_class", None),
        "authority_reason": getattr(source, "authority_reason", None),
        "publication_type": getattr(source, "publication_type", None),
        "domain_name": getattr(source, "domain_name", None),
        "approved_supporting_source": bool(getattr(source, "approved_supporting_source", False)),
        "semi_authoritative": bool(getattr(source, "semi_authoritative", False)),
        "derived_or_inferred": bool(getattr(source, "derived_or_inferred", False)),
        "last_fetched_at": getattr(source, "last_fetched_at", None).isoformat() if getattr(source, "last_fetched_at", None) else None,
        "next_refresh_due_at": getattr(source, "next_refresh_due_at", None).isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "current_fingerprint": getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
        "last_changed_at": getattr(source, "last_changed_at", None).isoformat() if getattr(source, "last_changed_at", None) else None,
        "last_http_status": getattr(source, "last_http_status", None),
        "refresh_interval_days": int(getattr(source, "refresh_interval_days", 0) or 0),
        "refresh_state": getattr(source, "refresh_state", None),
        "refresh_status_reason": getattr(source, "refresh_status_reason", None),
        "refresh_blocked_reason": getattr(source, "refresh_blocked_reason", None),
        "revalidation_required": bool(getattr(source, "revalidation_required", False)),
        "validation_due_at": getattr(source, "validation_due_at", None).isoformat() if getattr(source, "validation_due_at", None) else None,
        "last_refresh_attempt_at": getattr(source, "last_refresh_attempt_at", None).isoformat() if getattr(source, "last_refresh_attempt_at", None) else None,
        "last_refresh_completed_at": getattr(source, "last_refresh_completed_at", None).isoformat() if getattr(source, "last_refresh_completed_at", None) else None,
        "current_refresh_run_id": getattr(source, "current_refresh_run_id", None),
    }



def fetch_policy_source(
    db: Session,
    *,
    source: PolicySource,
    force: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    now = _utcnow()
    _sync_registry_defaults(source)
    pre_refresh = get_policy_source_refresh_snapshot(source)
    run_id = _refresh_run_id(source_id=int(getattr(source, "id", 0) or 0))

    if not policy_source_needs_refresh(source, force=force, now=now):
        return {
            "ok": True,
            "source_id": int(source.id),
            "skipped": True,
            "reason": "fresh_enough",
            "changed": False,
            "change_detected": False,
            "change_summary": build_source_change_summary(
                previous_fingerprint=getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
                current_fingerprint=getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
                authoritative=bool(int(getattr(source, "authority_rank", 0) or 0) >= 85),
            ),
            "current_fingerprint": getattr(source, "current_fingerprint", None),
            "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
            "refresh_state": getattr(source, "refresh_state", None),
            "status_reason": getattr(source, "refresh_status_reason", None),
            "next_step": "monitor",
            "revalidation_required": bool(getattr(source, "revalidation_required", False)),
            "pre_refresh": pre_refresh,
            "post_refresh": pre_refresh,
        }

    source.current_refresh_run_id = run_id
    source.last_refresh_attempt_at = now
    source.last_state_transition_at = now
    source.refresh_state = "crawling"
    source.refresh_status_reason = "fetch_started"
    source.refresh_blocked_reason = None
    db.add(source)
    db.commit()
    db.refresh(source)

    url = (source.url or "").strip()
    if not url:
        blocked_reason = "missing_url"
        source.registry_status = "warning"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = blocked_reason
        source.freshness_checked_at = now
        source.last_fetch_error = blocked_reason
        source.next_refresh_due_at = compute_next_retry_due(retry_count=int(getattr(source, "refresh_retry_count", 0) or 0), base_dt=now)
        source.refresh_state = "blocked"
        source.refresh_status_reason = blocked_reason
        source.refresh_blocked_reason = blocked_reason
        source.last_refresh_completed_at = now
        source.refresh_retry_count = int(getattr(source, "refresh_retry_count", 0) or 0) + 1
        source.last_refresh_outcome_json = _json_dumps({"ok": False, "reason": blocked_reason, "refresh_state": "blocked"})
        db.add(source)
        db.commit()
        post_refresh = get_policy_source_refresh_snapshot(source)
        crawl_sync = sync_crawl_result_to_inventory(
            db,
            source=source,
            fetch_result={
                "ok": False,
                "source_id": int(source.id),
                "fetch_error": blocked_reason,
                "changed": False,
                "change_detected": False,
                "refresh_state": "blocked",
                "status_reason": blocked_reason,
                "next_step": "manual_unblock",
                "revalidation_required": False,
            },
        )
        db.commit()
        return {
            "ok": False,
            "source_id": int(source.id),
            "skipped": False,
            "reason": blocked_reason,
            "changed": False,
            "change_detected": False,
            "fetch_error": blocked_reason,
            "refresh_state": "blocked",
            "status_reason": blocked_reason,
            "next_step": "manual_unblock",
            "revalidation_required": False,
            "pre_refresh": pre_refresh,
            "post_refresh": post_refresh,
            "crawl_sync": crawl_sync,
        }

    http_status: int | None = None
    content_type: str | None = None
    extracted_text = ""
    fetch_error: str | None = None

    previous_current_version = db.scalar(
        select(PolicySourceVersion).where(
            PolicySourceVersion.source_id == int(source.id),
            PolicySourceVersion.is_current.is_(True),
        ).order_by(PolicySourceVersion.retrieved_at.desc(), PolicySourceVersion.id.desc())
    )
    previous_version_id = int(previous_current_version.id) if previous_current_version is not None else None
    previous_fingerprint = getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None)

    try:
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
            resp = client.get(url)
            http_status = int(resp.status_code)
            content_type = resp.headers.get("content-type")
            extracted_text = _safe_text_from_http_response(resp)
            if http_status < 200 or http_status >= 400:
                fetch_error = f"http_status_{http_status}"
    except Exception as exc:
        fetch_error = f"{type(exc).__name__}: {exc}"

    fingerprint = _fingerprint_for_text(extracted_text or "")
    authoritative = bool(int(getattr(source, "authority_rank", 0) or 0) >= 85)

    version = PolicySourceVersion(
        source_id=int(source.id),
        retrieved_at=now,
        http_status=http_status,
        content_sha256=fingerprint[:64] if fingerprint else None,
        raw_path=getattr(source, "raw_path", None),
        content_type=content_type,
        fetch_error=fetch_error,
        extracted_text=extracted_text,
        is_current=True,
    )
    db.add(version)
    db.flush()

    prior_versions = list(
        db.scalars(
            select(PolicySourceVersion).where(
                PolicySourceVersion.source_id == int(source.id),
                PolicySourceVersion.id != int(version.id),
                PolicySourceVersion.is_current.is_(True),
            )
        ).all()
    )
    for row in prior_versions:
        row.is_current = False
        db.add(row)

    change_summary = build_source_change_summary(
        previous_fingerprint=previous_fingerprint,
        current_fingerprint=fingerprint or None,
        previous_version_id=previous_version_id,
        current_version_id=int(version.id),
        http_status=http_status,
        fetch_error=fetch_error,
        authoritative=authoritative,
        previous_last_changed_at=getattr(source, "last_changed_at", None),
    )
    state_payload = determine_source_refresh_state(
        fetch_ok=(fetch_error is None),
        change_summary=change_summary,
    )

    source.http_status = http_status
    source.last_http_status = http_status
    source.content_type = content_type
    source.retrieved_at = now
    source.last_fetched_at = now
    source.extracted_text = extracted_text
    source.content_sha256 = fingerprint[:64] if fingerprint else None
    source.current_fingerprint = fingerprint or None
    source.freshness_checked_at = now
    source.last_fetch_error = fetch_error
    source.next_refresh_due_at = (
        _compute_next_refresh_due_at(source, from_dt=now)
        if fetch_error is None
        else compute_next_retry_due(retry_count=int(getattr(source, "refresh_retry_count", 0) or 0), base_dt=now)
    )
    source.refresh_state = state_payload["refresh_state"]
    source.refresh_status_reason = state_payload["status_reason"]
    source.refresh_blocked_reason = state_payload.get("blocked_reason")
    source.last_refresh_completed_at = now
    source.last_state_transition_at = now
    source.revalidation_required = bool(state_payload.get("revalidation_required", False))
    source.validation_due_at = now if source.revalidation_required else None
    source.refresh_retry_count = 0 if fetch_error is None else int(getattr(source, "refresh_retry_count", 0) or 0) + 1

    if fetch_error is None:
        source.registry_status = "active"
        source.freshness_status = "fresh"
        source.freshness_reason = None
        if change_summary.get("changed"):
            source.last_changed_at = now
        else:
            source.last_seen_same_fingerprint_at = now
    else:
        source.registry_status = "warning"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = fetch_error

    source.last_change_summary_json = _json_dumps(change_summary)
    source.last_refresh_outcome_json = _json_dumps(
        {
            "ok": fetch_error is None,
            "source_version_id": int(version.id),
            "refresh_state": state_payload["refresh_state"],
            "status_reason": state_payload["status_reason"],
            "next_step": state_payload["next_step"],
            "http_status": http_status,
            "fetch_error": fetch_error,
            "change_summary": change_summary,
        }
    )

    db.add(source)
    db.commit()
    db.refresh(source)
    db.refresh(version)

    fetch_payload = {
        "ok": fetch_error is None,
        "source_id": int(source.id),
        "source_version_id": int(version.id),
        "fetch_error": fetch_error,
        "changed": bool(change_summary.get("changed")),
        "change_detected": bool(change_summary.get("change_detected")),
        "change_summary": change_summary,
        "current_fingerprint": fingerprint or None,
        "http_status": http_status,
        "refresh_state": state_payload["refresh_state"],
        "status_reason": state_payload["status_reason"],
        "next_step": state_payload["next_step"],
        "revalidation_required": bool(state_payload.get("revalidation_required", False)),
    }
    crawl_sync = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_payload)
    db.commit()
    db.refresh(source)
    db.refresh(version)
    post_refresh = get_policy_source_refresh_snapshot(source)

    return {
        "ok": fetch_error is None,
        "source_id": int(source.id),
        "source_version_id": int(version.id),
        "skipped": False,
        "reason": None if fetch_error is None else fetch_error,
        "fetch_error": fetch_error,
        "changed": bool(change_summary.get("changed")),
        "change_detected": bool(change_summary.get("change_detected")),
        "change_summary": change_summary,
        "previous_fingerprint": previous_fingerprint,
        "current_fingerprint": fingerprint or None,
        "http_status": http_status,
        "content_type": content_type,
        "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "refresh_state": state_payload["refresh_state"],
        "status_reason": state_payload["status_reason"],
        "next_step": state_payload["next_step"],
        "revalidation_required": bool(state_payload.get("revalidation_required", False)),
        "pre_refresh": pre_refresh,
        "post_refresh": post_refresh,
        "crawl_sync": crawl_sync,
    }


def refresh_policy_source_and_detect_changes(
    db: Session,
    *,
    source: PolicySource,
    force: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    result = fetch_policy_source(
        db,
        source=source,
        force=force,
        timeout_seconds=timeout_seconds,
    )
    result["refresh_due"] = policy_source_needs_refresh(source, force=False, now=_utcnow())
    result["self_maintaining"] = bool(result.get("ok")) and ("post_refresh" in result)
    return result

def inventory_summary_for_market(

    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
) -> dict[str, Any]:
    return summarize_inventory_for_scope(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        program_type=_norm_text(program_type),
    )
