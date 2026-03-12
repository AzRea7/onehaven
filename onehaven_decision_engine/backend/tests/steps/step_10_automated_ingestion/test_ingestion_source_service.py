from app.models import IngestionSource
from app.services.ingestion_source_service import ensure_default_manual_sources


def test_bootstrap_default_sources(db_session):
    rows = ensure_default_manual_sources(db_session, org_id=1)
    assert len(rows) == 1

    persisted = db_session.query(IngestionSource).filter(IngestionSource.org_id == 1).all()
    assert len(persisted) == 1

    row = persisted[0]
    assert row.provider == "rentcast"
    assert row.slug == "rentcast-sale-listings"
    assert row.is_enabled is True
    assert isinstance(row.config_json, dict)
    assert row.config_json.get("state") == "MI"
    assert row.config_json.get("city") == "Detroit"
    assert row.config_json.get("photo_mode") == "placeholder_until_connected"
    assert row.config_json.get("image_backfill_status") == "pending"