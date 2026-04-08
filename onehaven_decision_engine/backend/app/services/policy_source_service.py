# backend/app/services/policy_source_service.py
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import PolicyCatalogEntry, PolicySource, PolicySourceVersion


DEFAULT_TIMEOUT_SECONDS = 20.0


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
    if source_type == "federal":
        return "federal"
    if source_type == "state":
        return state.lower()
    if source_type == "county" and county:
        return f"{state.lower()}-{county.lower()}"
    if source_type == "city" and city:
        if county:
            return f"{state.lower()}-{county.lower()}-{city.lower()}"
        return f"{state.lower()}-{city.lower()}"
    if source_type == "program":
        base = pha_name or program_type or "program"
        return f"{state.lower()}-{base.strip().lower().replace(' ', '-')}"
    if city:
        if county:
            return f"{state.lower()}-{county.lower()}-{city.lower()}"
        return f"{state.lower()}-{city.lower()}"
    if county:
        return f"{state.lower()}-{county.lower()}"
    return state.lower()


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
    if "federal" in kind or "pha" in kind:
        return 14
    if "municipal" in kind or "inspection" in kind or "registration" in kind:
        return 21
    if "state" in kind:
        return 30
    return 30


def _safe_text_from_http_response(resp: httpx.Response) -> str:
    content_type = (resp.headers.get("content-type") or "").lower()
    if "text" in content_type or "json" in content_type or "html" in content_type or "xml" in content_type:
        return resp.text or ""
    try:
        return resp.text or ""
    except Exception:
        return ""


def policy_source_needs_refresh(
    source: PolicySource,
    *,
    force: bool = False,
    now: Optional[datetime] = None,
) -> bool:
    if force:
        return True

    now = now or datetime.utcnow()
    status = (getattr(source, "registry_status", None) or "active").strip().lower()
    if status not in {"active", "candidate"}:
        return False

    if getattr(source, "last_fetched_at", None) is None:
        return True

    refresh_days = int(getattr(source, "refresh_interval_days", 30) or 30)
    cutoff = now - timedelta(days=max(1, refresh_days))
    if source.last_fetched_at < cutoff:
        return True

    freshness_status = (getattr(source, "freshness_status", None) or "").strip().lower()
    if freshness_status in {"stale", "fetch_failed", "unknown"}:
        return True

    return False


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
) -> PolicySource:
    state = _norm_state(entry.state)
    county = _norm_lower(entry.county)
    city = _norm_lower(entry.city)
    pha_name = _norm_text(entry.pha_name)
    program_type = _norm_text(entry.program_type)

    existing = db.scalar(
        select(PolicySource).where(
            PolicySource.url == entry.url,
            or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None) if org_id is None else PolicySource.org_id == org_id),
        )
    )

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
            retrieved_at=datetime.utcnow(),
            content_sha256=None,
            raw_path=None,
            extracted_text=None,
            notes=entry.notes,
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
            fetch_config_json=_json_dumps({"focus": focus if isinstance(focus, str) else "se_mi_extended"}),
            registry_meta_json=_json_dumps(
                {
                    "catalog_entry_id": entry.id,
                    "baseline_url": entry.baseline_url,
                    "source_kind": entry.source_kind,
                    "priority": entry.priority,
                }
            ),
            fingerprint_algo="sha256",
            current_fingerprint=None,
            last_changed_at=None,
        )
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
    existing.notes = entry.notes
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
        }
    )
    existing.registry_meta_json = _json_dumps(meta)
    db.flush()
    return existing


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
        rows.append(ensure_policy_source_from_catalog_entry(db, entry=item, org_id=org_id))
    db.commit()
    return rows


def fetch_policy_source(
    db: Session,
    *,
    source: PolicySource,
    force: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    now = datetime.utcnow()
    if not policy_source_needs_refresh(source, force=force, now=now):
        return {
            "ok": True,
            "source_id": int(source.id),
            "skipped": True,
            "reason": "fresh_enough",
            "changed": False,
            "current_fingerprint": getattr(source, "current_fingerprint", None),
        }

    url = (source.url or "").strip()
    if not url:
        source.registry_status = "error"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = "missing_url"
        source.freshness_checked_at = now
        db.add(source)
        db.commit()
        return {
            "ok": False,
            "source_id": int(source.id),
            "skipped": False,
            "reason": "missing_url",
            "changed": False,
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
    previous_fingerprint = getattr(source, "current_fingerprint", None)
    changed = bool(fingerprint) and fingerprint != previous_fingerprint

    version = PolicySourceVersion(
        source_id=int(source.id),
        retrieved_at=now,
        http_status=http_status,
        content_sha256=fingerprint or None,
        raw_path=getattr(source, "raw_path", None),
        content_type=content_type,
        fetch_error=fetch_error,
        extracted_text=extracted_text,
        is_current=True,
    )
    db.add(version)

    prior_versions = list(
        db.scalars(
            select(PolicySourceVersion)
            .where(
                PolicySourceVersion.source_id == int(source.id),
                PolicySourceVersion.id != getattr(version, "id", -1),
                PolicySourceVersion.is_current.is_(True),
            )
        ).all()
    )
    for row in prior_versions:
        row.is_current = False
        db.add(row)

    source.http_status = http_status
    source.content_type = content_type
    source.retrieved_at = now
    source.last_fetched_at = now
    source.extracted_text = extracted_text
    source.content_sha256 = fingerprint or None
    source.current_fingerprint = fingerprint or None
    source.freshness_checked_at = now
    source.registry_status = "active" if fetch_error is None else "error"

    if fetch_error is None:
        source.freshness_status = "fresh"
        source.freshness_reason = "fetched_successfully"
        if changed:
            source.last_changed_at = now
    else:
        source.freshness_status = "fetch_failed"
        source.freshness_reason = fetch_error

    db.add(source)
    db.commit()
    db.refresh(source)

    return {
        "ok": fetch_error is None,
        "source_id": int(source.id),
        "skipped": False,
        "http_status": http_status,
        "content_type": content_type,
        "changed": changed,
        "fingerprint": fingerprint,
        "previous_fingerprint": previous_fingerprint,
        "fetch_error": fetch_error,
        "version_id": int(version.id),
    }


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

    rows = list(db.scalars(stmt).all())
    out: list[PolicySource] = []
    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue
        out.append(row)
    out.sort(key=lambda r: ((r.source_type or ""), (r.title or ""), int(r.id or 0)))
    return out