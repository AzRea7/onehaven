from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Deal, IngestionRecordLink, IngestionSource, Property, RentAssumption
from app.services.ingestion_run_execute import execute_source_sync


def _make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(bind=engine)
    session = TestingSessionLocal()
    return engine, session


def test_execute_source_sync_imports_property_first_records(monkeypatch) -> None:
    engine, db = _make_session()
    try:
        source = IngestionSource(
            org_id=1,
            provider="rentcast",
            slug="rentcast-manual",
            display_name="RentCast Manual",
            status="connected",
            is_enabled=True,
            config_json={
                "sample_rows": [
                    {
                        "external_record_id": "ext-1",
                        "address": "123 Main St",
                        "city": "Detroit",
                        "state": "MI",
                        "zip": "48226",
                        "bedrooms": 3,
                        "bathrooms": 1.0,
                        "square_feet": 1200,
                        "year_built": 1950,
                        "property_type": "single_family",
                        "asking_price": 85000,
                        "market_rent_estimate": 1550,
                        "photos": [{"url": "https://example.com/front.jpg", "kind": "exterior"}],
                    }
                ]
            },
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        def fake_pipeline(db_session, *, org_id: int, property_id: int, actor_user_id=None, emit_events=False):
            prop = db_session.get(Property, property_id)
            prop.normalized_address = "123 Main St, Detroit, MI 48226"
            prop.lat = 42.3314
            prop.lng = -83.0458
            prop.geocode_source = "google"
            prop.geocode_confidence = 0.99
            db_session.add(prop)
            db_session.flush()
            return {
                "geo_ok": True,
                "risk_ok": True,
                "rent_ok": True,
                "evaluate_ok": True,
                "state_ok": True,
                "workflow_ok": True,
                "next_actions_ok": True,
                "partial": False,
                "errors": [],
            }

        monkeypatch.setattr(
            "app.services.ingestion_run_execute.execute_post_ingestion_pipeline",
            fake_pipeline,
        )

        run = execute_source_sync(
            db,
            org_id=1,
            source=source,
            trigger_type="manual",
            runtime_config={"limit": 10},
        )

        prop = db.scalar(select(Property).where(Property.org_id == 1))
        deal = db.scalar(select(Deal).where(Deal.org_id == 1))
        rent = db.scalar(select(RentAssumption).where(RentAssumption.org_id == 1))
        link = db.scalar(select(IngestionRecordLink).where(IngestionRecordLink.org_id == 1))

        assert run.status == "success"
        assert prop is not None
        assert deal is not None
        assert rent is not None
        assert link is not None

        assert prop.address == "123 Main St"
        assert prop.city == "Detroit"
        assert prop.state == "MI"
        assert prop.zip == "48226"
        assert prop.normalized_address == "123 Main St, Detroit, MI 48226"
        assert prop.lat == 42.3314
        assert prop.lng == -83.0458

        summary = dict(run.summary_json or {})
        assert summary["records_imported"] == 1
        assert summary["properties_created"] == 1
        assert summary["deals_created"] == 1
        assert summary["rent_rows_upserted"] == 1
        assert summary["photos_upserted"] == 1
        assert summary["post_import_pipeline_attempted"] == 1
        assert summary["geo_enriched"] == 1
        assert summary["risk_scored"] == 1
        assert summary["evaluated"] == 1
        assert summary["workflow_synced"] == 1
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


def test_execute_source_sync_updates_existing_property_via_record_link(monkeypatch) -> None:
    engine, db = _make_session()
    try:
        source = IngestionSource(
            org_id=1,
            provider="rentcast",
            slug="rentcast-manual",
            display_name="RentCast Manual",
            status="connected",
            is_enabled=True,
            config_json={
                "sample_rows": [
                    {
                        "external_record_id": "ext-1",
                        "address": "123 Main St",
                        "city": "Detroit",
                        "state": "MI",
                        "zip": "48226",
                        "bedrooms": 4,
                        "bathrooms": 2.0,
                        "asking_price": 93000,
                    }
                ]
            },
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        initial_run = execute_source_sync(
            db,
            org_id=1,
            source=source,
            trigger_type="manual",
            runtime_config={"limit": 10},
        )
        assert initial_run.status == "success"

        def fake_pipeline(*args, **kwargs):
            return {
                "geo_ok": True,
                "risk_ok": True,
                "rent_ok": True,
                "evaluate_ok": True,
                "state_ok": True,
                "workflow_ok": True,
                "next_actions_ok": True,
                "partial": False,
                "errors": [],
            }

        monkeypatch.setattr(
            "app.services.ingestion_run_execute.execute_post_ingestion_pipeline",
            fake_pipeline,
        )

        second_run = execute_source_sync(
            db,
            org_id=1,
            source=source,
            trigger_type="manual",
            runtime_config={"limit": 10},
        )

        props = db.scalars(select(Property).where(Property.org_id == 1)).all()
        deals = db.scalars(select(Deal).where(Deal.org_id == 1)).all()

        assert second_run.status == "success"
        assert len(props) == 1
        assert len(deals) == 1
        assert props[0].bedrooms == 4
        assert props[0].bathrooms == 2.0

        summary = dict(second_run.summary_json or {})
        assert summary["records_imported"] == 1
        assert summary["properties_updated"] == 1
        assert summary["deals_updated"] == 1
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()