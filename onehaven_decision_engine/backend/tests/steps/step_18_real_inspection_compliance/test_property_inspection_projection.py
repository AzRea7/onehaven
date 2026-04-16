from __future__ import annotations

from datetime import datetime

from app.models import (
    Deal,
    InspectionItem,
    PropertyChecklistItem,
    RehabTask,
    UnderwritingResult,
)
from app.services.compliance_service import build_property_inspection_readiness
from app.services.property_state_machine import derive_stage_and_constraints, sync_property_state


def _seed_deal_and_uw(db_session, *, org_id: int, property_id: int):
    deal = Deal(
        org_id=org_id,
        property_id=property_id,
        source="manual",
        asking_price=85000,
        rehab_estimate=12000,
        strategy="section8",
        financing_type="dscr",
        interest_rate=0.07,
        term_years=30,
        down_payment_pct=0.2,
        decision="buy",
        source_fingerprint=f"fp-{property_id}",
    )
    db_session.add(deal)
    db_session.commit()
    db_session.refresh(deal)

    uw = UnderwritingResult(
        org_id=org_id,
        deal_id=deal.id,
        decision="PASS",
        score=88,
        reasons_json="[]",
        gross_rent_used=1600,
        mortgage_payment=650,
        operating_expenses=400,
        noi=1200,
        cash_flow=550,
        dscr=1.4,
        cash_on_cash=0.18,
        break_even_rent=1200,
        min_rent_for_target_roi=1400,
        decision_version="test",
        created_at=datetime.utcnow(),
    )
    db_session.add(uw)
    db_session.commit()
    return deal, uw


def test_property_projection_blocks_stage_when_real_failures_exist(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    _seed_deal_and_uw(db_session, org_id=org.id, property_id=prop.id)

    db_session.add(
        RehabTask(
            org_id=org.id,
            property_id=prop.id,
            title="Finish paint touchups",
            category="rehab",
            inspection_relevant=True,
            status="done",
            created_at=datetime.utcnow(),
        )
    )
    db_session.add(
        PropertyChecklistItem(
            org_id=org.id,
            property_id=prop.id,
            item_code="GFCI_KITCHEN",
            category="electrical",
            description="Kitchen GFCI",
            severity=3,
            common_fail=True,
            status="failed",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
    )
    db_session.add(
        InspectionItem(
            inspection_id=insp.id,
            code="GFCI_KITCHEN",
            failed=True,
            severity=4,
            details="missing",
            location="kitchen",
        )
    )
    db_session.commit()

    readiness = build_property_inspection_readiness(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )
    assert readiness["overall_status"] == "blocked"
    assert readiness["readiness"]["hqs_ready"] is False

    state = derive_stage_and_constraints(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )
    assert state["current_stage"] in {"rehab", "compliance"}
    assert "inspection_open_failures" in state["outstanding_tasks"]["blockers"] or "checklist_blockers" in state["outstanding_tasks"]["blockers"]


def test_property_projection_can_advance_when_compliance_is_clean(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    _seed_deal_and_uw(db_session, org_id=org.id, property_id=prop.id)

    db_session.add(
        RehabTask(
            org_id=org.id,
            property_id=prop.id,
            title="Finish rehab",
            category="rehab",
            inspection_relevant=True,
            status="done",
            created_at=datetime.utcnow(),
        )
    )
    db_session.add_all(
        [
            PropertyChecklistItem(
                org_id=org.id,
                property_id=prop.id,
                item_code="SMOKE_DETECTORS",
                category="safety",
                description="Smoke detectors",
                severity=3,
                common_fail=True,
                status="done",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
            PropertyChecklistItem(
                org_id=org.id,
                property_id=prop.id,
                item_code="GFCI_KITCHEN",
                category="electrical",
                description="Kitchen GFCI",
                severity=3,
                common_fail=True,
                status="done",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
        ]
    )
    insp.passed = True
    db_session.add(
        InspectionItem(
            inspection_id=insp.id,
            code="SMOKE_DETECTORS",
            failed=False,
            severity=3,
            details="ok",
        )
    )
    db_session.commit()

    sync_property_state(db_session, org_id=org.id, property_id=prop.id)
    db_session.commit()

    state = derive_stage_and_constraints(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert state["constraints"]["inspection"]["exists"] is True
    assert state["constraints"]["inspection"]["passed"] is True
    assert state["constraints"]["checklist"]["failed"] == 0
    assert state["constraints"]["checklist"]["blocked"] == 0



def test_property_projection_exposes_proof_obligations(real_inspection_seed, db_session):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    from app.services.policy_projection_service import build_property_projection_snapshot
    snapshot = build_property_projection_snapshot(db_session, org_id=org.id, property_id=prop.id)
    assert "proof_obligations" in snapshot or "proof_obligations" in (snapshot.get("projection") or {})
