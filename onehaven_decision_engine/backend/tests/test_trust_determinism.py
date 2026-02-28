# backend/tests/test_trust_determinism.py
from __future__ import annotations

import json
from datetime import datetime, timedelta

from app.db import SessionLocal
from app.services.trust_service import record_signal, recompute_and_persist


def test_trust_recompute_is_deterministic():
    db = SessionLocal()
    try:
        org_id = 999
        entity_type = "property"
        entity_id = "777"

        t0 = datetime.utcnow() - timedelta(days=1)

        record_signal(
            db,
            org_id=org_id,
            entity_type=entity_type,
            entity_id=entity_id,
            signal_key="a",
            value=1.0,
            weight=2.0,
            meta={"x": 1},
            created_at=t0,
        )
        record_signal(
            db,
            org_id=org_id,
            entity_type=entity_type,
            entity_id=entity_id,
            signal_key="b",
            value=0.0,
            weight=1.0,
            meta={"y": 2},
            created_at=t0,
        )
        db.commit()

        s1 = recompute_and_persist(db, org_id=org_id, entity_type=entity_type, entity_id=entity_id)
        db.commit()
        db.refresh(s1)

        score1 = float(s1.score)
        conf1 = float(s1.confidence)
        comp1 = json.loads(s1.components_json)

        s2 = recompute_and_persist(db, org_id=org_id, entity_type=entity_type, entity_id=entity_id)
        db.commit()
        db.refresh(s2)

        score2 = float(s2.score)
        conf2 = float(s2.confidence)
        comp2 = json.loads(s2.components_json)

        assert score1 == score2
        assert conf1 == conf2
        assert comp1 == comp2
    finally:
        db.close()