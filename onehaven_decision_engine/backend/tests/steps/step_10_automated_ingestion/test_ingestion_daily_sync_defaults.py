from app.models import IngestionSource
from app.services.ingestion_source_service import ensure_default_manual_sources


def test_default_source_is_daily_sync_and_tracks_focus_markets(db_session):
    rows = ensure_default_manual_sources(db_session, org_id=77)
    assert len(rows) == 1

    source = db_session.query(IngestionSource).filter(IngestionSource.org_id == 77).one()
    assert int(source.sync_interval_minutes or 0) == 1440
    cfg = source.config_json or {}
    assert isinstance(cfg.get("daily_sync_markets"), list)
    assert any(m.get("city") == "Detroit" for m in cfg["daily_sync_markets"])
