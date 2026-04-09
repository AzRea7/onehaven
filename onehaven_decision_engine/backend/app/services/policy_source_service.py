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
    if cty or cty:
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
            fetch_config_json=_json_dumps({"focus": focus}),
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
            next_refresh_due_at=None,
            last_fetch_error=None,
            last_http_status=None,
            last_seen_same_fingerprint_at=None,
            source_metadata_json="{}",
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
    _sync_registry_defaults(existing)
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


def fetch_policy_source(
    db: Session,
    *,
    source: PolicySource,
    force: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    now = _utcnow()
    _sync_registry_defaults(source)

    if not policy_source_needs_refresh(source, force=force, now=now):
        return {
            "ok": True,
            "source_id": int(source.id),
            "skipped": True,
            "reason": "fresh_enough",
            "changed": False,
            "current_fingerprint": getattr(source, "current_fingerprint", None),
            "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
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
        return {
            "ok": False,
            "source_id": int(source.id),
            "skipped": False,
            "reason": "missing_url",
            "changed": False,
            "fetch_error": "missing_url",
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

    return {
        "ok": fetch_error is None,
        "source_id": int(source.id),
        "source_version_id": int(version.id),
        "skipped": False,
        "reason": None if fetch_error is None else fetch_error,
        "fetch_error": fetch_error,
        "changed": bool(changed),
        "previous_fingerprint": previous_fingerprint,
        "current_fingerprint": fingerprint or None,
        "http_status": http_status,
        "content_type": content_type,
        "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
    }