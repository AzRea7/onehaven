from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import select, delete, desc
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
    value is generally in [0..1], but we clamp to [0..1] for safety.
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
    """
    Confidence is separate from score.
    You can have a high score with low confidence if evidence is sparse/old.
    """
    # Evidence amount curve: saturates as weight increases
    evidence = 1.0 - math.exp(-max(0.0, total_weight) / 6.0)  # ~0.63 at w=6, ~0.86 at w=12
    # Count curve: saturates with number of signals
    count = 1.0 - math.exp(-max(0, n_signals) / 12.0)

    recency = 0.5
    if newest_at:
        age = datetime.utcnow() - newest_at
        # 0 days -> 1.0, 7 days -> ~0.5, 30 days -> low
        recency = math.exp(-age.total_seconds() / (7.0 * 24 * 3600.0))

    # Blend and clamp
    return _clamp(0.55 * evidence + 0.30 * count + 0.15 * recency, 0.0, 1.0)


def _compute_components(signals: list[TrustSignal]) -> dict[str, Any]:
    """
    Build explanation payload:
    - top positive / negative contributors
    - raw aggregates
    """
    contribs: list[dict[str, Any]] = []
    for s in signals:
        contribs.append(
            {
                "signal_key": s.signal_key,
                "value": float(getattr(s, "value", 0.0) or 0.0),
                "weight": float(getattr(s, "weight", 1.0) or 1.0),
                "created_at": s.created_at.isoformat() if getattr(s, "created_at", None) else None,
                "meta": _loads(getattr(s, "meta_json", None), None),
                "contribution": float(getattr(s, "value", 0.0) or 0.0) * float(getattr(s, "weight", 1.0) or 1.0),
            }
        )

    # Sort by contribution for positives, and by (value-1)*weight for negatives.
    positives = sorted(contribs, key=lambda x: x["contribution"], reverse=True)
    negatives = sorted(contribs, key=lambda x: (x["value"] - 1.0) * x["weight"])

    return {
        "top_positive": positives[:3],
        "top_negative": negatives[:3],
        "signal_count": len(signals),
    }


def recompute_score(
    db: Session,
    *,
    org_id: int,
    entity_type: str,
    entity_id: str,
    lookback_days: int = 90,
) -> TrustSnapshot:
    """
    Trust score = weighted mean of signal values in [0..1] -> mapped to [0..100].
    Confidence computed from evidence amount, count, and recency.

    lookback_days prevents ancient signals dominating forever.
    """
    org_id = int(org_id)
    entity_type = str(entity_type)
    entity_id = str(entity_id)

    cutoff = datetime.utcnow() - timedelta(days=int(lookback_days))
    rows = db.scalars(
        select(TrustSignal)
        .where(TrustSignal.org_id == org_id)
        .where(TrustSignal.entity_type == entity_type)
        .where(TrustSignal.entity_id == entity_id)
        .where(TrustSignal.created_at >= cutoff)
        .order_by(desc(TrustSignal.created_at))
    ).all()

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
        org_id=org_id,
        entity_type=entity_type,
        entity_id=entity_id,
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

    existing = db.scalar(
        select(TrustScore)
        .where(TrustScore.org_id == snap.org_id)
        .where(TrustScore.entity_type == snap.entity_type)
        .where(TrustScore.entity_id == snap.entity_id)
    )

    if existing is None:
        existing = TrustScore(
            org_id=snap.org_id,
            entity_type=snap.entity_type,
            entity_id=snap.entity_id,
            score=float(snap.score_0_100),
            confidence=float(snap.confidence_0_1),
            components_json=_dumps(snap.components),
            updated_at=snap.updated_at,
        )
        db.add(existing)
    else:
        existing.score = float(snap.score_0_100)
        existing.confidence = float(snap.confidence_0_1)
        existing.components_json = _dumps(snap.components)
        existing.updated_at = snap.updated_at
        db.add(existing)

    return existing


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
