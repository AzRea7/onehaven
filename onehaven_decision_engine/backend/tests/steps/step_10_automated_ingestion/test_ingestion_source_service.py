from app.models import IngestionSource
from app.services.ingestion_source_service import ensure_default_manual_sources


def test_bootstrap_default_sources(db_session):
    rows = ensure_default_manual_sources(db_session, org_id=1)
    assert len(rows) >= 3

    persisted = db_session.query(IngestionSource).filter(IngestionSource.org_id == 1).all()
    providers = {r.provider for r in persisted}
    assert "zillow" in providers
    assert "investorlift" in providers
    assert "partner_feed" in providers