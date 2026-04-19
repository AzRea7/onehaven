from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

DEFAULT_TIMEOUT_SECONDS = 20.0
FETCH_CACHE_DIR = os.getenv("POLICY_FETCH_CACHE_DIR", "/tmp/policy_fetch_cache")
PLAYWRIGHT_ENABLED = os.getenv("POLICY_PLAYWRIGHT_ENABLED", "1").strip() not in {"0", "false", "False"}
PLAYWRIGHT_TIMEOUT_MS = int(os.getenv("POLICY_PLAYWRIGHT_TIMEOUT_MS", "25000"))
PLAYWRIGHT_WAIT_UNTIL = os.getenv("POLICY_PLAYWRIGHT_WAIT_UNTIL", "domcontentloaded").strip() or "domcontentloaded"

BROWSER_FALLBACK_HOSTS = {
    "www.michigan.gov",
    "michigan.gov",
    "www.dearborn.gov",
    "dearborn.gov",
    "cityofdearborn.org",
    "www.cityofdearborn.org",
    "www.waynecounty.com",
    "waynecounty.com",
    "www.waynecountymi.gov",
    "waynecountymi.gov",
    "www.hud.gov",
    "hud.gov",
}


def _host_from_url(url: str) -> str:
    host = urlparse(str(url or "").strip()).netloc.strip().lower()
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    return host


def _default_fetch_headers(url: str) -> dict[str, str]:
    host = _host_from_url(url)
    referer = f"https://{host}/" if host else "https://www.google.com/"
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Connection": "keep-alive",
    }


def _ensure_fetch_cache_dir() -> Path:
    path = Path(FETCH_CACHE_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _cache_key_for_url(url: str) -> str:
    return hashlib.sha256(str(url or "").strip().lower().encode("utf-8")).hexdigest()


def _cache_path_for_url(url: str) -> Path:
    return _ensure_fetch_cache_dir() / f"{_cache_key_for_url(url)}.json"


def _read_fetch_cache(url: str, *, max_age_seconds: int = 3600) -> dict[str, Any] | None:
    path = _cache_path_for_url(url)
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        fetched_at = raw.get("cached_at_epoch")
        if fetched_at is None:
            return None
        age = time.time() - float(fetched_at)
        if age > max_age_seconds:
            return None
        return raw if isinstance(raw, dict) else None
    except Exception:
        return None


def _write_fetch_cache(url: str, payload: dict[str, Any]) -> None:
    path = _cache_path_for_url(url)
    try:
        wrapped = {**dict(payload or {}), "cached_at_epoch": time.time()}
        path.write_text(json.dumps(wrapped, ensure_ascii=False, sort_keys=True, default=str), encoding="utf-8")
    except Exception:
        pass


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        text = soup.get_text("\n", strip=True)
        return re.sub(r"\n{2,}", "\n\n", text).strip()
    except Exception:
        return html.strip()


def _extract_title(html: str) -> str | None:
    raw = html or ""
    match = re.search(r"<title>\s*(.*?)\s*</title>", raw, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return title or None


def _browser_fallback_allowed(url: str) -> bool:
    host = _host_from_url(url)
    if not host:
        return False
    if host in BROWSER_FALLBACK_HOSTS:
        return True
    if host.endswith(".gov") or host.endswith(".mi.us"):
        return True
    return False


def _fetch_with_httpx(*, url: str, timeout_seconds: float) -> dict[str, Any]:
    headers = _default_fetch_headers(url)
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
        resp = client.get(url)
        body_text = resp.text or ""
        return {
            "ok": 200 <= int(resp.status_code) < 400,
            "method": "httpx",
            "url": str(resp.url),
            "http_status": int(resp.status_code),
            "content_type": resp.headers.get("content-type"),
            "html": body_text,
            "extracted_text": _html_to_text(body_text),
            "title": _extract_title(body_text),
            "fetch_error": None if 200 <= int(resp.status_code) < 400 else f"http_status_{int(resp.status_code)}",
        }


def _fetch_with_playwright(*, url: str, timeout_ms: int) -> dict[str, Any]:
    headers = _default_fetch_headers(url)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                user_agent=headers["User-Agent"],
                locale="en-US",
                extra_http_headers={
                    "Accept": headers["Accept"],
                    "Accept-Language": headers["Accept-Language"],
                    "Cache-Control": headers["Cache-Control"],
                    "Pragma": headers["Pragma"],
                    "Referer": headers["Referer"],
                },
            )
            page = context.new_page()
            response = page.goto(url, wait_until=PLAYWRIGHT_WAIT_UNTIL, timeout=timeout_ms)
            page.wait_for_timeout(1200)
            html = page.content() or ""
            http_status = None
            content_type = None
            final_url = page.url
            if response is not None:
                try:
                    http_status = int(response.status)
                except Exception:
                    http_status = None
                try:
                    content_type = response.headers.get("content-type")
                except Exception:
                    content_type = None
            ok = http_status is None or (200 <= int(http_status) < 400)
            return {
                "ok": ok,
                "method": "playwright",
                "url": final_url,
                "http_status": http_status,
                "content_type": content_type or "text/html",
                "html": html,
                "extracted_text": _html_to_text(html),
                "title": _extract_title(html),
                "fetch_error": None if ok else f"http_status_{int(http_status)}",
            }
        finally:
            try:
                browser.close()
            except Exception:
                pass


def should_browser_fallback_on_result(fetch_result: dict[str, Any]) -> bool:
    http_status = fetch_result.get("http_status")
    fetch_error = str(fetch_result.get("fetch_error") or "").lower()
    if http_status in {401, 403, 405, 406, 407, 429, 451}:
        return True
    if "timeout" in fetch_error or "forbidden" in fetch_error or "captcha" in fetch_error or "anti-bot" in fetch_error:
        return True
    return False


def fetch_official_source_with_fallback(
    *,
    url: str,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    cache_ttl_seconds: int = 3600,
) -> dict[str, Any]:
    cached = _read_fetch_cache(url, max_age_seconds=cache_ttl_seconds)
    if cached is not None:
        return {**cached, "from_cache": True}

    first_result: dict[str, Any] | None = None
    httpx_error: str | None = None

    try:
        first_result = _fetch_with_httpx(url=url, timeout_seconds=timeout_seconds)
        if first_result.get("ok"):
            _write_fetch_cache(url, first_result)
            return {**first_result, "from_cache": False}
        if not should_browser_fallback_on_result(first_result):
            _write_fetch_cache(url, first_result)
            return {**first_result, "from_cache": False}
    except Exception as exc:
        httpx_error = f"{type(exc).__name__}: {exc}"

    if not PLAYWRIGHT_ENABLED or not _browser_fallback_allowed(url):
        if first_result is not None:
            return {**first_result, "from_cache": False}
        return {
            "ok": False,
            "method": "httpx",
            "url": url,
            "http_status": None,
            "content_type": None,
            "html": "",
            "extracted_text": "",
            "title": None,
            "fetch_error": httpx_error or "fetch_failed",
            "from_cache": False,
        }

    try:
        browser_result = _fetch_with_playwright(url=url, timeout_ms=PLAYWRIGHT_TIMEOUT_MS)
        browser_result["httpx_error"] = httpx_error
        _write_fetch_cache(url, browser_result)
        return {**browser_result, "from_cache": False}
    except PlaywrightTimeoutError as exc:
        return {
            "ok": False,
            "method": "playwright",
            "url": url,
            "http_status": None,
            "content_type": None,
            "html": "",
            "extracted_text": "",
            "title": None,
            "fetch_error": f"PlaywrightTimeoutError: {exc}",
            "from_cache": False,
            "httpx_error": httpx_error,
        }
    except Exception as exc:
        if first_result is not None:
            return {
                **first_result,
                "from_cache": False,
                "playwright_error": f"{type(exc).__name__}: {exc}",
                "httpx_error": httpx_error,
            }
        return {
            "ok": False,
            "method": "playwright",
            "url": url,
            "http_status": None,
            "content_type": None,
            "html": "",
            "extracted_text": "",
            "title": None,
            "fetch_error": f"{type(exc).__name__}: {exc}",
            "from_cache": False,
            "httpx_error": httpx_error,
        }


def build_fetch_metadata_payload(*, fetch_meta: dict[str, Any], fetched_at: datetime) -> dict[str, Any]:
    return {
        "fetch_method": fetch_meta.get("method"),
        "from_cache": bool(fetch_meta.get("from_cache")),
        "final_url": fetch_meta.get("url"),
        "http_status": fetch_meta.get("http_status"),
        "content_type": fetch_meta.get("content_type"),
        "title": fetch_meta.get("title"),
        "httpx_error": fetch_meta.get("httpx_error"),
        "playwright_error": fetch_meta.get("playwright_error"),
        "fetch_error": fetch_meta.get("fetch_error"),
        "fetched_at": fetched_at.isoformat(),
    }


PDF_CONTENT_TYPES = {"application/pdf", "application/x-pdf"}
BLOCKED_HTTP_STATUSES = {401, 403, 405, 406, 407, 429, 451}
MANUAL_ONLY_FETCH_MODES = {"manual-required", "manual_required", "manual"}


def _json_bytes_to_text(content: bytes) -> str:
    try:
        return content.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _detect_content_fetch_mode(*, url: str, content_type: str | None, explicit_mode: str | None = None) -> str:
    mode = str(explicit_mode or "").strip().lower()
    if mode in {"api", "html", "pdf", "manual-required"}:
        return mode
    lower_url = str(url or "").strip().lower()
    ctype = str(content_type or "").strip().lower()
    if lower_url.endswith('.pdf') or any(x in ctype for x in PDF_CONTENT_TYPES):
        return "pdf"
    if "/json" in ctype or ctype.endswith("+json"):
        return "api"
    return "html"


def _summarize_error(fetch_error: str | None, http_status: int | None = None) -> tuple[str, str]:
    err = str(fetch_error or "").strip().lower()
    if http_status in BLOCKED_HTTP_STATUSES or any(token in err for token in ["captcha", "forbidden", "anti-bot", "blocked"]):
        return "blocked", "manual_review"
    if err in {"missing_url", "manual_required", "no_source", "not_configured"}:
        return "manual_required" if err == "manual_required" else "unresolved", "manual_review" if err == "manual_required" else "resolve_source"
    return "failed", "retry_fetch"


def fetch_pdf_source(*, url: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    headers = _default_fetch_headers(url)
    try:
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
            resp = client.get(url)
            content = bytes(resp.content or b"")
            ok = 200 <= int(resp.status_code) < 400
            text = _json_bytes_to_text(content[:10000]) if not ok else ""
            return {
                "ok": ok,
                "method": "httpx_pdf",
                "url": str(resp.url),
                "http_status": int(resp.status_code),
                "content_type": resp.headers.get("content-type") or "application/pdf",
                "title": Path(urlparse(str(resp.url)).path).name or None,
                "raw_bytes": content,
                "raw_text_preview": text,
                "fetch_error": None if ok else f"http_status_{int(resp.status_code)}",
            }
    except Exception as exc:
        return {
            "ok": False,
            "method": "httpx_pdf",
            "url": url,
            "http_status": None,
            "content_type": "application/pdf",
            "title": Path(urlparse(url).path).name or None,
            "raw_bytes": b"",
            "raw_text_preview": "",
            "fetch_error": f"{type(exc).__name__}: {exc}",
        }


def fetch_api_source(*, url: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    headers = _default_fetch_headers(url)
    headers["Accept"] = "application/json,text/plain;q=0.9,*/*;q=0.8"
    try:
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
            resp = client.get(url)
            body = resp.text or ""
            ok = 200 <= int(resp.status_code) < 400
            parsed_json = None
            if ok:
                try:
                    parsed_json = resp.json()
                except Exception:
                    parsed_json = None
            return {
                "ok": ok,
                "method": "httpx_api",
                "url": str(resp.url),
                "http_status": int(resp.status_code),
                "content_type": resp.headers.get("content-type") or "application/json",
                "title": None,
                "json": parsed_json,
                "extracted_text": body[:20000],
                "fetch_error": None if ok else f"http_status_{int(resp.status_code)}",
            }
    except Exception as exc:
        return {
            "ok": False,
            "method": "httpx_api",
            "url": url,
            "http_status": None,
            "content_type": "application/json",
            "title": None,
            "json": None,
            "extracted_text": "",
            "fetch_error": f"{type(exc).__name__}: {exc}",
        }


def fetch_policy_source_candidate(candidate: dict[str, Any], *, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    url = str(candidate.get("url") or "").strip()
    fetch_mode = str(candidate.get("fetch_mode") or "").strip().lower()
    if fetch_mode in MANUAL_ONLY_FETCH_MODES:
        return {
            "ok": False,
            "resolution": "manual_required",
            "next_step": "manual_review",
            "fetch_mode": fetch_mode or "manual-required",
            "url": url or None,
            "http_status": None,
            "content_type": None,
            "title": candidate.get("title"),
            "fetch_error": "manual_required",
            "reason": "source family explicitly requires manual handling",
        }
    if not url:
        return {
            "ok": False,
            "resolution": "unresolved",
            "next_step": "resolve_source",
            "fetch_mode": fetch_mode or "unknown",
            "url": None,
            "http_status": None,
            "content_type": None,
            "title": candidate.get("title"),
            "fetch_error": "missing_url",
            "reason": "no fetchable source URL is mapped for this source family",
        }

    if fetch_mode == "api":
        result = fetch_api_source(url=url, timeout_seconds=timeout_seconds)
    elif fetch_mode == "pdf":
        result = fetch_pdf_source(url=url, timeout_seconds=timeout_seconds)
    else:
        result = fetch_official_source_with_fallback(url=url, timeout_seconds=timeout_seconds)

    detected_mode = _detect_content_fetch_mode(url=url, content_type=result.get("content_type"), explicit_mode=fetch_mode)
    if result.get("ok"):
        return {
            **result,
            "resolution": "fetched",
            "next_step": "extract",
            "fetch_mode": detected_mode,
            "reason": "fetch succeeded",
            "fetched_at": datetime.utcnow().isoformat(),
        }

    resolution, next_step = _summarize_error(result.get("fetch_error"), result.get("http_status"))
    return {
        **result,
        "resolution": resolution,
        "next_step": next_step,
        "fetch_mode": detected_mode,
        "reason": "fetch blocked" if resolution == "blocked" else ("manual review required" if resolution == "manual_required" else "fetch failed" if resolution == "failed" else "source unresolved"),
        "fetched_at": datetime.utcnow().isoformat(),
    }
