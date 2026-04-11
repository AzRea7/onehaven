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


def _fingerprint_for_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


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
) -> dict[str, Any]:
    parsed = urlparse(url)
    host = parsed.netloc.strip().lower()
    publisher_text = (publisher or "").strip().lower()
    title_text = (title or "").strip().lower()

    authority_kind = "unknown"
    authority_score = 0.35

    if host.endswith(".gov") or ".gov." in host:
        authority_kind = "official_government"
        authority_score = 0.98
    elif host.endswith(".mi.us") or ".us" in host:
        authority_kind = "governmental_local"
        authority_score = 0.90
    elif "hud.gov" in host or "ecfr.gov" in host or "legislature.mi.gov" in host or "michigan.gov" in host:
        authority_kind = "official_government"
        authority_score = 0.99
    elif "housing" in publisher_text or "authority" in publisher_text or "housing" in title_text:
        authority_kind = "program_authority"
        authority_score = 0.82
    elif host.endswith(".org"):
        authority_kind = "organizational"
        authority_score = 0.72
    elif host:
        authority_kind = "private_site"
        authority_score = 0.50

    return {
        "authority_kind": authority_kind,
        "authority_score": float(round(authority_score, 3)),
        "domain": host,
        "is_official": authority_score >= 0.85,
    }


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
    if status not in {"active", "candidate", "warning"}:
        return False

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
                    }
                }
            ),
            last_verified_by_user_id=None,
        )
        _sync_registry_defaults(source)
        db.add(source)
        db.flush()
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
                authority = classify_discovery_authority(url=url, publisher=publisher, title=category_label(category))
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
            authority = classify_discovery_authority(url=url, publisher=pha_name, title="Section 8 overlay")
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
    for item in items:
        rows.append(
            ensure_policy_source_from_catalog_entry(
                db,
                entry=item,
                org_id=org_id,
                focus=focus,
            )
        )
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
        "last_fetched_at": getattr(source, "last_fetched_at", None).isoformat() if getattr(source, "last_fetched_at", None) else None,
        "next_refresh_due_at": getattr(source, "next_refresh_due_at", None).isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "current_fingerprint": getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
        "last_changed_at": getattr(source, "last_changed_at", None).isoformat() if getattr(source, "last_changed_at", None) else None,
        "last_http_status": getattr(source, "last_http_status", None),
        "refresh_interval_days": int(getattr(source, "refresh_interval_days", 0) or 0),
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

    if not policy_source_needs_refresh(source, force=force, now=now):
        return {
            "ok": True,
            "source_id": int(source.id),
            "skipped": True,
            "reason": "fresh_enough",
            "changed": False,
            "change_detected": False,
            "current_fingerprint": getattr(source, "current_fingerprint", None),
            "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
            "pre_refresh": pre_refresh,
            "post_refresh": pre_refresh,
        }

    url = (source.url or "").strip()
    if not url:
        source.registry_status = "warning"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = "missing_url"
        source.freshness_checked_at = now
        source.last_fetch_error = "missing_url"
        source.next_refresh_due_at = now + timedelta(days=1)
        db.add(source)
        db.commit()
        post_refresh = get_policy_source_refresh_snapshot(source)
        return {
            "ok": False,
            "source_id": int(source.id),
            "skipped": False,
            "reason": "missing_url",
            "changed": False,
            "change_detected": False,
            "fetch_error": "missing_url",
            "pre_refresh": pre_refresh,
            "post_refresh": post_refresh,
        }

    http_status: int | None = None
    content_type: str | None = None
    extracted_text = ""
    fetch_error: str | None = None

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
    previous_fingerprint = getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None)
    changed = bool(fingerprint) and fingerprint != previous_fingerprint

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
    source.next_refresh_due_at = _compute_next_refresh_due_at(source, from_dt=now)

    if fetch_error is None:
        source.registry_status = "active"
        source.freshness_status = "fresh"
        source.freshness_reason = None
        if changed:
            source.last_changed_at = now
        else:
            source.last_seen_same_fingerprint_at = now
    else:
        source.registry_status = "warning"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = fetch_error

    db.add(source)
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
        "changed": bool(changed),
        "change_detected": bool(changed),
        "previous_fingerprint": previous_fingerprint,
        "current_fingerprint": fingerprint or None,
        "http_status": http_status,
        "content_type": content_type,
        "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "pre_refresh": pre_refresh,
        "post_refresh": post_refresh,
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