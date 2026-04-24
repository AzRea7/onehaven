from __future__ import annotations

from types import SimpleNamespace

from app.routers import imports as imports_router


class _FakeImportSnapshot:
    _seq = 100

    def __init__(self, org_id: int, source: str, notes: str | None, created_at):
        type(self)._seq += 1
        self.id = type(self)._seq
        self.org_id = org_id
        self.source = source
        self.notes = notes
        self.created_at = created_at


class _FakeDeal:
    _seq = 200

    def __init__(self, **kwargs):
        type(self)._seq += 1
        self.id = type(self)._seq
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeDB:
    def __init__(self):
        self.added = []
        self.scalar_calls = 0

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        return None

    def refresh(self, obj):
        return None

    def rollback(self):
        return None

    def scalar(self, query):
        # _import_rows only needs this to check for an existing deal fingerprint in this test.
        self.scalar_calls += 1
        return None


def test_import_rows_runs_property_first_pipeline_for_each_imported_property(monkeypatch):
    db = _FakeDB()

    prop_1 = SimpleNamespace(id=11, address="123 Main St", zip="48201")
    prop_2 = SimpleNamespace(id=22, address="456 Oak Ave", zip="48202")

    normalized_rows = [
        SimpleNamespace(
            address="123 Main St",
            city="Detroit",
            state="MI",
            zip="48201",
            bedrooms=3,
            bathrooms=1.0,
            square_feet=1200,
            year_built=1950,
            has_garage=False,
            asking_price=85000,
            estimated_purchase_price=83000,
            rehab_estimate=12000,
            market_rent_estimate=1600,
            section8_fmr=1500,
            approved_rent_ceiling=1500,
            rent_reasonableness_comp=1480,
            inventory_count=5,
            starbucks_minutes=None,
            raw={"id": "row-1"},
            strategy="section8",
        ),
        SimpleNamespace(
            address="456 Oak Ave",
            city="Detroit",
            state="MI",
            zip="48202",
            bedrooms=4,
            bathrooms=1.5,
            square_feet=1400,
            year_built=1948,
            has_garage=True,
            asking_price=92000,
            estimated_purchase_price=90000,
            rehab_estimate=15000,
            market_rent_estimate=1750,
            section8_fmr=1650,
            approved_rent_ceiling=1650,
            rent_reasonableness_comp=1600,
            inventory_count=6,
            starbucks_minutes=None,
            raw={"id": "row-2"},
            strategy="section8",
        ),
    ]

    prop_queue = [prop_1, prop_2]
    pipeline_calls: list[int] = []

    monkeypatch.setattr(imports_router, "ImportSnapshot", _FakeImportSnapshot)
    monkeypatch.setattr(imports_router, "Deal", _FakeDeal)
    monkeypatch.setattr(imports_router, "normalize_zillow", lambda row: normalized_rows.pop(0))
    monkeypatch.setattr(imports_router, "fingerprint", lambda source, address, zip_code, asking_price: f"{address}|{zip_code}|{asking_price}")
    monkeypatch.setattr(imports_router, "_get_or_create_property", lambda db, org_id, n: prop_queue.pop(0))
    monkeypatch.setattr(imports_router, "_upsert_rent", lambda db, org_id, property_id, n: None)
    monkeypatch.setattr(imports_router, "_backfill_inventory_counts_from_snapshot", lambda db, org_id, snapshot_id: None)
    monkeypatch.setattr(imports_router, "execute_post_ingestion_pipeline", lambda db, org_id, property_id, actor_user_id=None, emit_events=False: pipeline_calls.append(property_id))

    result = imports_router._import_rows(
        db=db,
        source="zillow",
        rows=[{"row": 1}, {"row": 2}],
        notes="test import",
        org_id=1,
    )

    assert result.imported == 2
    assert result.skipped_duplicates == 0
    assert result.errors == []
    assert pipeline_calls == [11, 22]
    