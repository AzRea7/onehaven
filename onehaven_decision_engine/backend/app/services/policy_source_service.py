from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource, PolicySourceVersion
from app.services.policy_catalog import (
    PolicyCatalogItem,
    catalog_for_market,
    catalog_mi_authoritative,
    catalog_municipalities,
)


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v if v else None


def _norm_state(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().upper()
    return v or None


def _norm_city(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_county(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _safe_text_from_html(html: str, max_len: int = 20_000) -> str:
    html = re.sub(r"(?is)<script.*?>.*?</script>", "", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", "", html)
    text = re.sub(r"(?is)<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def _ssl_verify_setting() -> bool:
    raw = os.getenv("POLICY_FETCH_VERIFY_SSL", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _invalidate_verified_assertions_for_source(db: Session, source_id: int) -> None:
    rows = (
        db.query(PolicyAssertion)
        .filter(PolicyAssertion.source_id == source_id)
        .filter(PolicyAssertion.review_status == "verified")
        .all()
    )
    now = datetime.utcnow()
    for a in rows:
        a.review_status = "needs_recheck"
        a.review_notes = ((a.review_notes or "") + " | source_changed=content_sha256_changed").strip()
        a.stale_after = now
        a.reviewed_at = now


@dataclass
class CollectResult:
    source: PolicySource
    changed: bool
    fetch_ok: bool = True
    fetch_error: Optional[str] = None


def collect_url(
    db: Session,
    *,
    org_id: Optional[int],
    url: str,
    state: Optional[str] = "MI",
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
    publisher: Optional[str] = None,
    title: Optional[str] = None,
    notes: Optional[str] = None,
    timeout_s: float = 20.0,
) -> CollectResult:
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)
    url = url.strip()

    verify_ssl = _ssl_verify_setting()

    content_type: Optional[str] = None
    status: Optional[int] = None
    body: bytes = b""
    resp_text: Optional[str] = None
    fetch_error: Optional[str] = None

    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=timeout_s,
            verify=verify_ssl,
            headers={"User-Agent": "OneHavenPolicyCollector/1.0"},
        ) as client:
            resp = client.get(url)
            status = int(resp.status_code)
            content_type = (resp.headers.get("content-type") or "").split(";")[0].strip() or None
            body = resp.content or b""
            try:
                resp_text = resp.text
            except Exception:
                resp_text = None
    except Exception as e:
        fetch_error = f"{type(e).__name__}: {e}"

    digest = _sha256(body) if body else None

    existing = (
        db.query(PolicySource)
        .filter(PolicySource.url == url)
        .filter(PolicySource.org_id.is_(None) if org_id is None else PolicySource.org_id == org_id)
        .first()
    )

    changed = False
    now = datetime.utcnow()

    if existing is None:
        row = PolicySource(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=_norm(pha_name),
            program_type=_norm(program_type),
            publisher=_norm(publisher),
            title=_norm(title),
            url=url,
            content_type=content_type,
            http_status=status,
            retrieved_at=now,
            content_sha256=digest,
            notes=_norm(notes if not fetch_error else f"{notes or ''} | fetch_error={fetch_error}".strip(" |")),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
    else:
        row = existing
        changed = (row.content_sha256 != digest) and (digest is not None)
        row.state = st
        row.county = cnty
        row.city = cty
        row.pha_name = _norm(pha_name)
        row.program_type = _norm(program_type)
        row.publisher = _norm(publisher) or row.publisher
        row.title = _norm(title) or row.title
        row.content_type = content_type
        row.http_status = status
        row.retrieved_at = now
        row.content_sha256 = digest
        if fetch_error:
            row.notes = _norm(f"{row.notes or ''} | fetch_error={fetch_error}".strip(" |"))
        else:
            row.notes = _norm(notes) or row.notes

        if changed:
            _invalidate_verified_assertions_for_source(db, row.id)

        db.commit()
        db.refresh(row)

    raw_path: Optional[str] = None
    extracted_text: Optional[str] = None

    if not fetch_error:
        day = now.strftime("%Y-%m-%d")
        base_dir = f"/app/policy_raw/{day}"
        os.makedirs(base_dir, exist_ok=True)

        ext = "bin"
        if content_type:
            ct = content_type.lower()
            if "pdf" in ct:
                ext = "pdf"
            elif "html" in ct:
                ext = "html"
            elif "json" in ct:
                ext = "json"
            elif "text" in ct:
                ext = "txt"

        raw_path = f"{base_dir}/{row.id}.{ext}"
        try:
            with open(raw_path, "wb") as f:
                f.write(body)
            row.raw_path = raw_path
        except Exception:
            row.raw_path = None
            raw_path = None

        try:
            if content_type and ("html" in content_type.lower()) and resp_text:
                extracted_text = _safe_text_from_html(resp_text)
            elif content_type and content_type.lower().startswith("text/") and resp_text:
                extracted_text = resp_text[:20_000]
        except Exception:
            extracted_text = None

        row.extracted_text = extracted_text
        db.commit()
        db.refresh(row)

    db.query(PolicySourceVersion).filter(
        PolicySourceVersion.source_id == row.id
    ).update({"is_current": False}, synchronize_session=False)

    db.add(
        PolicySourceVersion(
            source_id=row.id,
            retrieved_at=now,
            http_status=status,
            content_sha256=digest,
            raw_path=raw_path,
            content_type=content_type,
            fetch_error=fetch_error,
            extracted_text=extracted_text,
            is_current=True,
        )
    )
    db.commit()

    return CollectResult(
        source=row,
        changed=changed,
        fetch_ok=fetch_error is None,
        fetch_error=fetch_error,
    )


def collect_catalog_item(
    db: Session,
    *,
    org_id: Optional[int],
    item: PolicyCatalogItem,
    timeout_s: float = 20.0,
) -> CollectResult:
    return collect_url(
        db,
        org_id=org_id,
        url=item.url,
        state=item.state,
        county=item.county,
        city=item.city,
        pha_name=item.pha_name,
        program_type=item.program_type,
        publisher=item.publisher,
        title=item.title,
        notes=item.notes,
        timeout_s=timeout_s,
    )


def collect_catalog_for_market(
    db,
    *,
    org_id: int | None,
    state: str = "MI",
    county: str | None = None,
    city: str | None = None,
    focus: str = "se_mi_extended",
):
    from app.services.policy_catalog_admin_service import merged_catalog_for_market

    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=None,
        focus=focus,
    )

    results = []
    for item in items:
        res = collect_url(
            db,
            org_id=org_id,
            url=item.url,
            state=item.state or "MI",
            county=item.county,
            city=item.city,
            pha_name=item.pha_name,
            program_type=item.program_type,
            publisher=item.publisher,
            title=item.title,
            notes=item.notes,
        )
        results.append(res)

    return results


def collect_catalog_for_focus(
    db: Session,
    *,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
    timeout_s: float = 20.0,
) -> list[CollectResult]:
    items = catalog_mi_authoritative(focus=focus)
    out: list[CollectResult] = []
    for item in items:
        out.append(
            collect_catalog_item(
                db,
                org_id=org_id,
                item=item,
                timeout_s=timeout_s,
            )
        )
    return out


def collect_catalog_all_municipalities(
    db: Session,
    *,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
    timeout_s: float = 20.0,
) -> dict:
    items = catalog_mi_authoritative(focus=focus)
    municipalities = catalog_municipalities(items)

    results: list[dict] = []
    total_sources = 0
    ok_count = 0
    failed_count = 0

    for market in municipalities:
        market_results = collect_catalog_for_market(
            db,
            org_id=org_id,
            state=market["state"] or "MI",
            county=market["county"],
            city=market["city"],
            focus=focus,
            timeout_s=timeout_s,
        )

        total_sources += len(market_results)
        ok_count += sum(1 for r in market_results if r.fetch_ok)
        failed_count += sum(1 for r in market_results if not r.fetch_ok)

        results.append(
            {
                "state": market["state"],
                "county": market["county"],
                "city": market["city"],
                "source_count": len(market_results),
                "ok_count": sum(1 for r in market_results if r.fetch_ok),
                "failed_count": sum(1 for r in market_results if not r.fetch_ok),
                "source_ids": [r.source.id for r in market_results],
            }
        )

    return {
        "focus": focus,
        "municipality_count": len(municipalities),
        "total_sources": total_sources,
        "ok_count": ok_count,
        "failed_count": failed_count,
        "markets": results,
    }
