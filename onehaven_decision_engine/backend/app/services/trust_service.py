# backend/app/services/trust_service.py
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from ..models import TrustSignal, TrustScore


@dataclass(frozen=True)
class TrustSnapshot:
    org_id: int
    entity_type: str
    entity_id: str
    score_0_100: float
    confidence_0_1: float
    components: dict[str, Any]
    updated_at: datetime


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def record_signal(
    db: Session,
    *,
    org_id: int,
    entity_type: str,
    entity_id: str,
    signal_key: str,
    value: float,
    weight: float = 1.0,
    meta: Optional[dict[str, Any]] = None,
    created_at: Optional[datetime] = None,
) -> TrustSignal:
    """
    Append-only trust signal stream.
    value clamped to [0..1] for safety.
    """
    v = _clamp(float(value), 0.0, 1.0)
    w = max(0.0, float(weight))
    row = TrustSignal(
        org_id=int(org_id),
        entity_type=str(entity_type),
        entity_id=str(entity_id),
        signal_key=str(signal_key),
        value=v,
        weight=w,
        meta_json=_dumps(meta) if meta else None,
        created_at=created_at or datetime.utcnow(),
    )
    db.add(row)
    return row


def clear_entity_signals(db: Session, *, org_id: int, entity_type: str, entity_id: str) -> int:
    res = db.execute(
        delete(TrustSignal).where(
            TrustSignal.org_id == int(org_id),
            TrustSignal.entity_type == str(entity_type),
            TrustSignal.entity_id == str(entity_id),
        )
    )
    return int(res.rowcount or 0)


def _confidence_from_evidence(*, total_weight: float, n_signals: int, newest_at: Optional[datetime]) -> float:
    evidence = 1.0 - math.exp(-max(0.0, total_weight) / 6.0)
    count = 1.0 - math.exp(-max(0, n_signals) / 12.0)

    recency = 0.5
    if newest_at:
        age = datetime.utcnow() - newest_at
        recency = math.exp(-age.total_seconds() / (7.0 * 24 * 3600.0))

    return _clamp(0.55 * evidence + 0.30 * count + 0.15 * recency, 0.0, 1.0)


def _compute_components(signals: list[TrustSignal]) -> dict[str, Any]:
    contribs: list[dict[str, Any]] = []
    for s in signals:
        v = float(getattr(s, "value", 0.0) or 0.0)
        w = float(getattr(s, "weight", 1.0) or 1.0)
        contribs.append(
            {
                "signal_key": s.signal_key,
                "value": v,
                "weight": w,
                "created_at": s.created_at.isoformat() if getattr(s, "created_at", None) else None,
                "meta": _loads(getattr(s, "meta_json", None), None),
                "contribution": v * w,
            }
        )

    positives = sorted(contribs, key=lambda x: x["contribution"], reverse=True)
    negatives = sorted(contribs, key=lambda x: (x["value"] - 1.0) * x["weight"])

    return {"top_positive": positives[:3], "top_negative": negatives[:3], "signal_count": len(signals)}


def recompute_score(
    db: Session,
    *,
    org_id: int,
    entity_type: str,
    entity_id: str,
    lookback_days: int = 90,
) -> TrustSnapshot:
    cutoff = datetime.utcnow() - timedelta(days=int(lookback_days))

    rows = list(
        db.scalars(
            select(TrustSignal)
            .where(TrustSignal.org_id == int(org_id))
            .where(TrustSignal.entity_type == str(entity_type))
            .where(TrustSignal.entity_id == str(entity_id))
            .where(TrustSignal.created_at >= cutoff)
            .order_by(desc(TrustSignal.created_at), desc(TrustSignal.id))
        ).all()
    )

    total_weight = 0.0
    weighted_sum = 0.0
    newest_at: Optional[datetime] = None

    for s in rows:
        w = float(getattr(s, "weight", 1.0) or 1.0)
        v = _clamp(float(getattr(s, "value", 0.0) or 0.0), 0.0, 1.0)
        if w <= 0:
            continue
        total_weight += w
        weighted_sum += (v * w)
        if newest_at is None:
            newest_at = getattr(s, "created_at", None)

    mean_0_1 = (weighted_sum / total_weight) if total_weight > 0 else 0.0
    score_0_100 = _clamp(mean_0_1 * 100.0, 0.0, 100.0)

    confidence = _confidence_from_evidence(total_weight=total_weight, n_signals=len(rows), newest_at=newest_at)
    components = _compute_components(rows)

    return TrustSnapshot(
        org_id=int(org_id),
        entity_type=str(entity_type),
        entity_id=str(entity_id),
        score_0_100=score_0_100,
        confidence_0_1=confidence,
        components=components,
        updated_at=datetime.utcnow(),
    )


def recompute_and_persist(
    db: Session,
    *,
    org_id: int,
    entity_type: str,
    entity_id: str,
    lookback_days: int = 90,
) -> TrustScore:
    snap = recompute_score(db, org_id=org_id, entity_type=entity_type, entity_id=entity_id, lookback_days=lookback_days)

    row = db.scalar(
        select(TrustScore)
        .where(TrustScore.org_id == int(snap.org_id))
        .where(TrustScore.entity_type == str(snap.entity_type))
        .where(TrustScore.entity_id == str(snap.entity_id))
    )

    if row is None:
        row = TrustScore(
            org_id=snap.org_id,
            entity_type=snap.entity_type,
            entity_id=snap.entity_id,
            score=float(snap.score_0_100),
            confidence=float(snap.confidence_0_1),
            components_json=_dumps(snap.components),
            updated_at=snap.updated_at,
        )
        db.add(row)
    else:
        row.score = float(snap.score_0_100)
        row.confidence = float(snap.confidence_0_1)
        row.components_json = _dumps(snap.components)
        row.updated_at = snap.updated_at
        db.add(row)

    return row


def get_trust_score(
    db: Session,
    *,
    org_id: int,
    entity_type: str,
    entity_id: str,
    recompute: bool = False,
) -> TrustScore:
    row = db.scalar(
        select(TrustScore)
        .where(TrustScore.org_id == int(org_id))
        .where(TrustScore.entity_type == str(entity_type))
        .where(TrustScore.entity_id == str(entity_id))
    )

    if row is None or recompute:
        row = recompute_and_persist(db, org_id=int(org_id), entity_type=str(entity_type), entity_id=str(entity_id))
        db.commit()
        db.refresh(row)

    return row
