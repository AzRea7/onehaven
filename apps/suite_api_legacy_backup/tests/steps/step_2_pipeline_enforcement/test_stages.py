from __future__ import annotations

from app.domain.workflow.stages import (
    STAGES,
    clamp_stage,
    gate_for_next_stage,
    next_stage,
    prev_stage,
    stage_catalog,
    stage_label,
    stage_rank,
)


def test_stage_catalog_uses_only_the_six_canonical_stages():
    assert STAGES == ["deal", "rehab", "compliance", "tenant", "cash", "equity"]

    catalog = stage_catalog()
    assert [row["key"] for row in catalog] == STAGES
    assert [row["rank"] for row in catalog] == [0, 1, 2, 3, 4, 5]


def test_clamp_stage_normalizes_legacy_aliases_into_new_workflow():
    assert clamp_stage("deal") == "deal"
    assert clamp_stage("intake") == "deal"
    assert clamp_stage("inspection") == "compliance"
    assert clamp_stage("lease") == "tenant"
    assert clamp_stage("management") == "cash"
    assert clamp_stage("portfolio") == "equity"
    assert clamp_stage("unknown-random-value") == "deal"


def test_stage_rank_and_navigation_are_consistent():
    assert stage_rank("deal") == 0
    assert stage_rank("rehab") == 1
    assert stage_rank("compliance") == 2
    assert stage_rank("tenant") == 3
    assert stage_rank("cash") == 4
    assert stage_rank("equity") == 5

    assert next_stage("deal") == "rehab"
    assert next_stage("cash") == "equity"
    assert next_stage("equity") is None

    assert prev_stage("equity") == "cash"
    assert prev_stage("rehab") == "deal"
    assert prev_stage("deal") is None


def test_stage_labels_are_investor_facing():
    assert stage_label("deal") == "Deal"
    assert stage_label("rehab") == "Rehab"
    assert stage_label("compliance") == "Compliance"
    assert stage_label("tenant") == "Tenant"
    assert stage_label("cash") == "Cash"
    assert stage_label("equity") == "Equity"


def test_gate_for_next_stage_blocks_rejected_deals():
    gate = gate_for_next_stage(
        current_stage="deal",
        decision_bucket="REJECT",
        deal_complete=False,
        rehab_complete=False,
        compliance_complete=False,
        tenant_complete=False,
        cash_complete=False,
        equity_complete=False,
    )

    assert gate.ok is False
    assert gate.allowed_next_stage is None
    assert "decision_reject" in gate.blockers
    assert "Rejected deals" in str(gate.blocked_reason)


def test_gate_for_next_stage_opens_from_good_deal_to_rehab():
    gate = gate_for_next_stage(
        current_stage="deal",
        decision_bucket="GOOD",
        deal_complete=True,
        rehab_complete=False,
        compliance_complete=False,
        tenant_complete=False,
        cash_complete=False,
        equity_complete=False,
    )

    assert gate.ok is True
    assert gate.allowed_next_stage == "rehab"
    assert gate.blockers == []


def test_gate_for_next_stage_blocks_when_rehab_not_complete():
    gate = gate_for_next_stage(
        current_stage="rehab",
        decision_bucket="GOOD",
        deal_complete=True,
        rehab_complete=False,
        compliance_complete=False,
        tenant_complete=False,
        cash_complete=False,
        equity_complete=False,
    )

    assert gate.ok is False
    assert gate.allowed_next_stage == "compliance"
    assert "rehab_incomplete" in gate.blockers


def test_gate_for_next_stage_blocks_when_compliance_not_complete():
    gate = gate_for_next_stage(
        current_stage="compliance",
        decision_bucket="GOOD",
        deal_complete=True,
        rehab_complete=True,
        compliance_complete=False,
        tenant_complete=False,
        cash_complete=False,
        equity_complete=False,
    )

    assert gate.ok is False
    assert gate.allowed_next_stage == "tenant"
    assert "compliance_incomplete" in gate.blockers


def test_gate_for_next_stage_blocks_when_tenant_not_complete():
    gate = gate_for_next_stage(
        current_stage="tenant",
        decision_bucket="GOOD",
        deal_complete=True,
        rehab_complete=True,
        compliance_complete=True,
        tenant_complete=False,
        cash_complete=False,
        equity_complete=False,
    )

    assert gate.ok is False
    assert gate.allowed_next_stage == "cash"
    assert "tenant_incomplete" in gate.blockers


def test_gate_for_next_stage_blocks_when_cash_not_complete():
    gate = gate_for_next_stage(
        current_stage="cash",
        decision_bucket="GOOD",
        deal_complete=True,
        rehab_complete=True,
        compliance_complete=True,
        tenant_complete=True,
        cash_complete=False,
        equity_complete=False,
    )

    assert gate.ok is False
    assert gate.allowed_next_stage == "equity"
    assert "cash_incomplete" in gate.blockers