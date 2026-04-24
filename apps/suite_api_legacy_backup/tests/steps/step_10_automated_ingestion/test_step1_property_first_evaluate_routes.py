from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.routers.evaluate as evaluate_mod
from app.auth import get_principal
from app.db import get_db
from app.routers.evaluate import router as evaluate_router


class DummyUnderwritingResultOut:
    @classmethod
    def model_validate(cls, payload):
        return payload


class FakeDB:
    def __init__(self, scalar_values=None, execute_rows=None):
        self.scalar_values = list(scalar_values or [])
        self.execute_rows = list(execute_rows or [])
        self.rollbacks = 0

    def scalar(self, _query):
        if self.scalar_values:
            return self.scalar_values.pop(0)
        return None

    def execute(self, _query):
        class _Result:
            def __init__(self, rows):
                self._rows = rows

            def all(self):
                return self._rows

        return _Result(self.execute_rows)

    def rollback(self):
        self.rollbacks += 1


def _build_app(db) -> FastAPI:
    app = FastAPI()
    app.include_router(evaluate_router)
    app.dependency_overrides[get_principal] = lambda: SimpleNamespace(
        org_id=1,
        user_id=999,
    )
    app.dependency_overrides[get_db] = lambda: db
    return app


def test_evaluate_property_uses_property_first_core(monkeypatch):
    fake_db = FakeDB(
        scalar_values=[
            SimpleNamespace(  # latest Deal lookup in /evaluate/property/{property_id}
                id=555,
                property_id=101,
                org_id=1,
                strategy="section8",
            )
        ]
    )
    app = _build_app(fake_db)
    client = TestClient(app)

    explain_calls = []
    core_calls = []

    monkeypatch.setattr(evaluate_mod, "UnderwritingResultOut", DummyUnderwritingResultOut)

    def fake_explain_rent(*, property_id, strategy, payment_standard_pct, persist, db, p):
        explain_calls.append(
            {
                "property_id": property_id,
                "strategy": strategy,
                "payment_standard_pct": payment_standard_pct,
                "persist": persist,
            }
        )
        return {"ok": True}

    def fake_evaluate_property_core(
        db,
        *,
        org_id,
        property_id,
        strategy=None,
        payment_standard_pct=None,
        actor_user_id=None,
        emit_events=True,
        commit=True,
    ):
        core_calls.append(
            {
                "org_id": org_id,
                "property_id": property_id,
                "strategy": strategy,
                "payment_standard_pct": payment_standard_pct,
                "actor_user_id": actor_user_id,
                "emit_events": emit_events,
                "commit": commit,
            }
        )
        return {
            "ok": True,
            "property_id": property_id,
            "deal_id": 555,
            "decision": "GOOD",
            "score": 91,
            "fallback_used": False,
            "computed_ceiling": 1650.0,
            "cap_reason": "fmr",
            "fmr_adjusted": 1705.0,
            "created": True,
            "result": {
                "id": 1,
                "deal_id": 555,
                "org_id": 1,
                "decision": "GOOD",
                "score": 91,
                "dscr": 1.42,
                "cash_flow": 515.0,
                "gross_rent_used": 1650.0,
                "mortgage_payment": 720.0,
                "operating_expenses": 310.0,
                "noi": 1340.0,
                "cash_on_cash": 0.14,
                "break_even_rent": 1180.0,
                "min_rent_for_target_roi": 1400.0,
                "decision_version": "test",
                "payment_standard_pct_used": 1.1,
                "jurisdiction_multiplier": 1.0,
                "jurisdiction_reasons_json": "[]",
                "rent_cap_reason": "fmr",
                "fmr_adjusted": 1705.0,
                "reasons_json": '["ok"]',
                "bedrooms": 3,
                "bathrooms": 1.0,
                "rent_explain_run_id": None,
            },
        }

    monkeypatch.setattr(evaluate_mod, "explain_rent", fake_explain_rent)
    monkeypatch.setattr(evaluate_mod, "evaluate_property_core", fake_evaluate_property_core)

    res = client.post("/evaluate/property/101")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["ok"] is True
    assert body["property_id"] == 101
    assert body["deal_id"] == 555
    assert body["decision"] == "GOOD"
    assert body["score"] == 91

    assert len(explain_calls) == 1
    assert explain_calls[0]["property_id"] == 101
    assert explain_calls[0]["persist"] is True

    assert len(core_calls) == 1
    assert core_calls[0]["org_id"] == 1
    assert core_calls[0]["property_id"] == 101
    assert core_calls[0]["actor_user_id"] == 999
    assert core_calls[0]["emit_events"] is True
    assert core_calls[0]["commit"] is True


def test_evaluate_properties_batch_is_property_first(monkeypatch):
    fake_db = FakeDB(
        scalar_values=[
            SimpleNamespace(id=11, property_id=201, org_id=1, strategy="section8"),
            SimpleNamespace(id=12, property_id=202, org_id=1, strategy="market"),
        ]
    )
    app = _build_app(fake_db)
    client = TestClient(app)

    monkeypatch.setattr(evaluate_mod, "UnderwritingResultOut", DummyUnderwritingResultOut)

    explain_calls = []
    core_calls = []

    def fake_explain_rent(*, property_id, strategy, payment_standard_pct, persist, db, p):
        explain_calls.append((property_id, strategy, payment_standard_pct, persist))
        return {"ok": True}

    def fake_evaluate_property_core(
        db,
        *,
        org_id,
        property_id,
        strategy=None,
        payment_standard_pct=None,
        actor_user_id=None,
        emit_events=True,
        commit=True,
    ):
        core_calls.append(
            {
                "org_id": org_id,
                "property_id": property_id,
                "strategy": strategy,
                "payment_standard_pct": payment_standard_pct,
            }
        )
        decision = "GOOD" if property_id == 201 else "REVIEW"
        score = 88 if property_id == 201 else 73
        return {
            "ok": True,
            "property_id": property_id,
            "deal_id": 9000 + property_id,
            "decision": decision,
            "score": score,
            "fallback_used": False,
            "computed_ceiling": 1600.0,
            "cap_reason": "fmr",
            "fmr_adjusted": 1700.0,
            "created": True,
            "result": {
                "id": property_id,
                "deal_id": 9000 + property_id,
                "org_id": 1,
                "decision": decision,
                "score": score,
                "dscr": 1.3,
                "cash_flow": 400.0,
                "gross_rent_used": 1600.0,
                "mortgage_payment": 700.0,
                "operating_expenses": 300.0,
                "noi": 1300.0,
                "cash_on_cash": 0.12,
                "break_even_rent": 1200.0,
                "min_rent_for_target_roi": 1350.0,
                "decision_version": "test",
                "payment_standard_pct_used": 1.1,
                "jurisdiction_multiplier": 1.0,
                "jurisdiction_reasons_json": "[]",
                "rent_cap_reason": "fmr",
                "fmr_adjusted": 1700.0,
                "reasons_json": "[]",
                "bedrooms": 3,
                "bathrooms": 1.0,
                "rent_explain_run_id": None,
            },
        }

    monkeypatch.setattr(evaluate_mod, "explain_rent", fake_explain_rent)
    monkeypatch.setattr(evaluate_mod, "evaluate_property_core", fake_evaluate_property_core)

    res = client.post(
        "/evaluate/properties",
        json={
            "property_ids": [201, 202, 201],  # duplicate included intentionally
            "strategy": None,
            "payment_standard_pct": 1.1,
            "explain_rent_first": True,
        },
    )
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["ok"] is True
    assert body["attempted"] == 2
    assert body["evaluated"] == 2
    assert body["property_ids"] == [201, 202]
    assert body["good_count"] == 1
    assert body["review_count"] == 1
    assert body["reject_count"] == 0
    assert len(body["results"]) == 2
    assert body["errors"] == []

    assert len(explain_calls) == 2
    assert {x[0] for x in explain_calls} == {201, 202}

    assert len(core_calls) == 2
    assert {x["property_id"] for x in core_calls} == {201, 202}


def test_evaluate_results_is_property_first_and_ignores_unknown_query_params(monkeypatch):
    prop = SimpleNamespace(id=301, org_id=1, bedrooms=3, bathrooms=1.0)
    deal = SimpleNamespace(id=401, property_id=301, org_id=1)
    uw = SimpleNamespace(
        id=501,
        deal_id=401,
        org_id=1,
        decision="GOOD",
        score=95,
        dscr=1.5,
        cash_flow=600.0,
        gross_rent_used=1750.0,
        mortgage_payment=700.0,
        operating_expenses=320.0,
        noi=1430.0,
        cash_on_cash=0.16,
        break_even_rent=1180.0,
        min_rent_for_target_roi=1410.0,
        decision_version="test",
        payment_standard_pct_used=1.1,
        jurisdiction_multiplier=1.0,
        jurisdiction_reasons_json="[]",
        rent_cap_reason="fmr",
        fmr_adjusted=1800.0,
        reasons_json="[]",
        rent_explain_run_id=None,
    )
    fake_db = FakeDB(execute_rows=[(uw, deal, prop)])
    app = _build_app(fake_db)
    client = TestClient(app)

    monkeypatch.setattr(evaluate_mod, "UnderwritingResultOut", DummyUnderwritingResultOut)

    res = client.get(
        "/evaluate/results",
        params={
            "decision": "GOOD",
            "property_ids": "301",
            "snapshot_id": "9999",  # unknown param now; should not break the route
        },
    )
    assert res.status_code == 200, res.text

    body = res.json()
    assert isinstance(body, list)
    assert len(body) == 1
    assert body[0]["deal_id"] == 401
    assert body[0]["org_id"] == 1
    assert body[0]["decision"] == "GOOD"
    assert body[0]["score"] == 95