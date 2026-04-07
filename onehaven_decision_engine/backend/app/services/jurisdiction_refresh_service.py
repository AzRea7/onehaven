from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile, PolicySource
from app.services.jurisdiction_completeness_service import recompute_profile_and_coverage
from app.services.policy_pipeline_service import repair_market


DEFAULT_JURISDICTION_STALE_DAYS = 90


@dataclass(frozen=True)
class JurisdictionRefreshTarget:
    jurisdiction_profile_id: int
    org_id: int | None
    state: str
    county: str | None
    city: str | None
    pha_name: str | None
    stale_reason: str | None
    last_refresh_success_at: datetime | None
    last_verified_at: datetime | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "jurisdiction_profile_id": self.jurisdiction_profile_id,
            "org_id": self.org_id,
            "state": self.state,
            "county": self.county,
            "city": self.city,
            "pha_name": self.pha_name,
            "stale_reason": self.stale_reason,
            "last_refresh_success_at": self.last_refresh_success_at.isoformat()
            if self.last_refresh_success_at
            else None,
            "last_verified_at": self.last_verified_at.isoformat()
            if self.last_verified_at
            else None,
        }


def _utcnow() -> datetime:
    return datetime.utcnow()


def _stale_cutoff(stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS) -> datetime:
    return _utcnow() - timedelta(days=int(stale_days))


def list_jurisdictions_needing_refresh(
    db: Session,
    *,
    org_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> list[JurisdictionRefreshTarget]:
    cutoff = _stale_cutoff(stale_days)

    stmt = select(JurisdictionProfile).where(
        or_(
            JurisdictionProfile.is_stale.is_(True),
            JurisdictionProfile.last_verified_at.is_(None),
            JurisdictionProfile.last_refresh_success_at.is_(None),
            JurisdictionProfile.last_refresh_success_at < cutoff,
            JurisdictionProfile.last_verified_at < cutoff,
        )
    ).order_by(JurisdictionProfile.id.asc())

    if org_id is not None:
        stmt = stmt.where(
            or_(
                JurisdictionProfile.org_id == int(org_id),
                JurisdictionProfile.org_id.is_(None),
            )
        )

    if batch_size is not None:
        stmt = stmt.limit(max(1, int(batch_size)))

    rows = list(db.scalars(stmt).all())
    out: list[JurisdictionRefreshTarget] = []

    for row in rows:
        out.append(
            JurisdictionRefreshTarget(
                jurisdiction_profile_id=int(row.id),
                org_id=getattr(row, "org_id", None),
                state=(getattr(row, "state", None) or "MI").strip().upper(),
                county=getattr(row, "county", None),
                city=getattr(row, "city", None),
                pha_name=getattr(row, "pha_name", None),
                stale_reason=getattr(row, "stale_reason", None),
                last_refresh_success_at=getattr(row, "last_refresh_success_at", None),
                last_verified_at=getattr(row, "last_verified_at", None),
            )
        )

    return out


def build_jurisdiction_refresh_payload(
    *,
    org_id: int | None,
    jurisdiction_profile_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    reason: str | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> dict[str, Any]:
    return {
        "trigger_type": "jurisdiction_refresh",
        "org_id": org_id,
        "jurisdiction_profile_id": jurisdiction_profile_id,
        "state": (state or "MI").strip().upper(),
        "county": (county or "").strip().lower() or None,
        "city": (city or "").strip().lower() or None,
        "pha_name": (pha_name or "").strip() or None,
        "reason": reason,
        "force": bool(force),
        "stale_days": int(stale_days),
    }


def _needs_refresh(
    profile: JurisdictionProfile,
    *,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> bool:
    if force:
        return True

    if bool(getattr(profile, "is_stale", False)):
        return True

    cutoff = _stale_cutoff(stale_days)
    last_success = getattr(profile, "last_refresh_success_at", None)
    last_verified = getattr(profile, "last_verified_at", None)

    if last_success is None:
        return True
    if last_success < cutoff:
        return True
    if last_verified is None:
        return True
    if last_verified < cutoff:
        return True

    return False


def refresh_jurisdiction_profile(
    db: Session,
    *,
    jurisdiction_profile_id: int,
    reviewer_user_id: int | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        return {
            "ok": False,
            "error": "jurisdiction_profile_not_found",
            "jurisdiction_profile_id": int(jurisdiction_profile_id),
        }

    if not _needs_refresh(profile, force=force, stale_days=stale_days):
        profile, coverage = recompute_profile_and_coverage(
            db,
            profile,
            stale_days=stale_days,
            commit=True,
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "not_stale",
            "jurisdiction_profile_id": int(profile.id),
            "profile": {
                "id": int(profile.id),
                "completeness_status": profile.completeness_status,
                "completeness_score": float(profile.completeness_score or 0.0),
                "is_stale": bool(profile.is_stale),
                "stale_reason": profile.stale_reason,
            },
            "coverage": {
                "id": int(coverage.id),
                "coverage_status": coverage.coverage_status,
                "production_readiness": coverage.production_readiness,
            },
        }

    result = repair_market(
        db,
        org_id=getattr(profile, "org_id", None),
        reviewer_user_id=reviewer_user_id,
        state=getattr(profile, "state", None) or "MI",
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
        focus=focus,
        archive_extracted_duplicates=True,
    )

    refreshed_profile = db.get(JurisdictionProfile, int(profile.id))
    refreshed_profile, coverage = recompute_profile_and_coverage(
        db,
        refreshed_profile,
        stale_days=stale_days,
        commit=True,
    )

    return {
        "ok": True,
        "skipped": False,
        "jurisdiction_profile_id": int(refreshed_profile.id),
        "repair_result": result,
        "profile": {
            "id": int(refreshed_profile.id),
            "org_id": refreshed_profile.org_id,
            "state": refreshed_profile.state,
            "county": refreshed_profile.county,
            "city": refreshed_profile.city,
            "pha_name": refreshed_profile.pha_name,
            "completeness_status": refreshed_profile.completeness_status,
            "completeness_score": float(refreshed_profile.completeness_score or 0.0),
            "is_stale": bool(refreshed_profile.is_stale),
            "stale_reason": refreshed_profile.stale_reason,
            "last_refresh_success_at": refreshed_profile.last_refresh_success_at.isoformat()
            if refreshed_profile.last_refresh_success_at
            else None,
        },
        "coverage": {
            "id": int(coverage.id),
            "coverage_status": coverage.coverage_status,
            "production_readiness": coverage.production_readiness,
            "completeness_status": coverage.completeness_status,
            "is_stale": bool(coverage.is_stale),
        },
    }


def refresh_stale_jurisdictions(
    db: Session,
    *,
    org_id: int | None = None,
    reviewer_user_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    targets = list_jurisdictions_needing_refresh(
        db,
        org_id=org_id,
        batch_size=batch_size,
        stale_days=stale_days,
    )

    refreshed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    for target in targets:
        try:
            result = refresh_jurisdiction_profile(
                db,
                jurisdiction_profile_id=target.jurisdiction_profile_id,
                reviewer_user_id=reviewer_user_id,
                force=False,
                stale_days=stale_days,
                focus=focus,
            )
            if result.get("ok"):
                refreshed.append(
                    {
                        "jurisdiction_profile_id": target.jurisdiction_profile_id,
                        "state": target.state,
                        "county": target.county,
                        "city": target.city,
                        "pha_name": target.pha_name,
                        "skipped": bool(result.get("skipped", False)),
                    }
                )
            else:
                failed.append(
                    {
                        "jurisdiction_profile_id": target.jurisdiction_profile_id,
                        "state": target.state,
                        "county": target.county,
                        "city": target.city,
                        "pha_name": target.pha_name,
                        "error": result.get("error", "refresh_failed"),
                    }
                )
        except Exception as exc:
            failed.append(
                {
                    "jurisdiction_profile_id": target.jurisdiction_profile_id,
                    "state": target.state,
                    "county": target.county,
                    "city": target.city,
                    "pha_name": target.pha_name,
                    "error": str(exc),
                }
            )

    return {
        "ok": len(failed) == 0,
        "org_id": org_id,
        "target_count": len(targets),
        "refreshed_count": len(refreshed),
        "failed_count": len(failed),
        "refreshed": refreshed,
        "failed": failed,
    }


def list_stale_policy_sources(
    db: Session,
    *,
    org_id: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    batch_size: int | None = None,
) -> list[PolicySource]:
    cutoff = _stale_cutoff(stale_days)

    stmt = select(PolicySource).where(
        or_(
            PolicySource.freshness_status == "stale",
            PolicySource.freshness_status == "fetch_failed",
            PolicySource.last_verified_at.is_(None),
            PolicySource.retrieved_at.is_(None),
            PolicySource.retrieved_at < cutoff,
        )
    ).order_by(PolicySource.id.asc())

    if org_id is not None:
        stmt = stmt.where(
            or_(
                PolicySource.org_id == int(org_id),
                PolicySource.org_id.is_(None),
            )
        )

    if batch_size is not None:
        stmt = stmt.limit(max(1, int(batch_size)))

    return list(db.scalars(stmt).all())


# ---- Chunk 5 refresh enrichments ----
_base_refresh_jurisdiction_profile = refresh_jurisdiction_profile


def refresh_jurisdiction_profile(
    db: Session,
    *,
    jurisdiction_profile_id: int,
    reviewer_user_id: int | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = 'se_mi_extended',
) -> dict[str, Any]:
    result = _base_refresh_jurisdiction_profile(
        db,
        jurisdiction_profile_id=jurisdiction_profile_id,
        reviewer_user_id=reviewer_user_id,
        force=force,
        stale_days=stale_days,
        focus=focus,
    )
    profile_payload = result.get('profile') or {}
    if profile_payload:
        confidence_score = 0.0
        if profile_payload.get('completeness_score') is not None:
            confidence_score += 0.5 * float(profile_payload.get('completeness_score') or 0.0)
        if not profile_payload.get('is_stale'):
            confidence_score += 0.25
        if profile_payload.get('last_refresh_success_at'):
            confidence_score += 0.25
        confidence_score = max(0.0, min(1.0, round(confidence_score, 3)))
        result['refresh_confidence'] = {
            'coverage_confidence': 'high' if confidence_score >= 0.75 else ('medium' if confidence_score >= 0.45 else 'low'),
            'confidence_score': confidence_score,
        }
    result['refresh_payload'] = build_jurisdiction_refresh_payload(
        org_id=(profile_payload.get('org_id') if profile_payload else None),
        jurisdiction_profile_id=jurisdiction_profile_id,
        state=profile_payload.get('state') if profile_payload else None,
        county=profile_payload.get('county') if profile_payload else None,
        city=profile_payload.get('city') if profile_payload else None,
        pha_name=profile_payload.get('pha_name') if profile_payload else None,
        reason='chunk5_refresh',
        force=force,
        stale_days=stale_days,
    )
    return result
