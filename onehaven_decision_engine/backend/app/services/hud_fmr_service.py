# backend/app/services/hud_fmr_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..policy_models import HudFmrRecord
from ..clients.hud_user import HudUserClient


@dataclass(frozen=True)
class FmrResult:
    ok: bool
    fmr: Optional[float]
    reason: str
    source: str


def _now() -> datetime:
    return datetime.utcnow()


def _stale(fetched_at: datetime, max_age_days: int = 45) -> bool:
    return fetched_at < (_now() - timedelta(days=max_age_days))


def get_cached_fmr(
    db: Session,
    *,
    state: str,
    area_name: str,
    year: int,
    bedrooms: int,
) -> FmrResult:
    row = db.scalar(
        select(HudFmrRecord)
        .where(HudFmrRecord.state == state)
        .where(HudFmrRecord.area_name == area_name)
        .where(HudFmrRecord.year == year)
        .where(HudFmrRecord.bedrooms == bedrooms)
    )
    if row is None:
        return FmrResult(ok=False, fmr=None, reason="missing_cache", source="cache")

    if _stale(row.fetched_at):
        return FmrResult(ok=True, fmr=float(row.fmr), reason="stale_cache", source="cache")

    return FmrResult(ok=True, fmr=float(row.fmr), reason="fresh_cache", source="cache")


def refresh_fmr_from_hud_user(
    db: Session,
    *,
    state: str,
    area_name: str,
    year: int,
    bedrooms: int,
) -> FmrResult:
    """
    Optional online refresh via HUD USER API client.
    If token missing, we do NOT pretend. We return missing_token.
    """
    if not settings.hud_user_token:
        return FmrResult(ok=False, fmr=None, reason="missing_hud_user_token", source="hud_user_api")

    cli = HudUserClient()
    # NOTE: implement client method if missing. If you can’t fetch area_name exactly,
    # store policy "area_name" to match what HUD uses for your target metros.
    try:
        payload = cli.fetch_fmr(state=state, area_name=area_name, year=year, bedrooms=bedrooms)
        # payload should include "fmr" numeric; keep raw_json for audit.
        fmr_val = float(payload["fmr"])
    except Exception as e:
        return FmrResult(ok=False, fmr=None, reason=f"hud_fetch_failed:{type(e).__name__}", source="hud_user_api")

    row = db.scalar(
        select(HudFmrRecord)
        .where(HudFmrRecord.state == state)
        .where(HudFmrRecord.area_name == area_name)
        .where(HudFmrRecord.year == year)
        .where(HudFmrRecord.bedrooms == bedrooms)
    )
    if row is None:
        row = HudFmrRecord(
            state=state,
            area_name=area_name,
            year=year,
            bedrooms=bedrooms,
            fmr=fmr_val,
            source="hud_user_api",
            fetched_at=_now(),
            raw_json=json.dumps(payload, ensure_ascii=False),
        )
        db.add(row)
    else:
        row.fmr = fmr_val
        row.source = "hud_user_api"
        row.fetched_at = _now()
        row.raw_json = json.dumps(payload, ensure_ascii=False)

    db.commit()
    return FmrResult(ok=True, fmr=fmr_val, reason="refreshed", source="hud_user_api")