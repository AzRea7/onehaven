from app.models import IngestionSource
from products.acquire.backend.src.services.ingestion_source_service import ensure_default_manual_sources


def test_bootstrap_default_sources(db_session, auth_context):
    org_id = auth_context["org"].id

    rows = ensure_default_manual_sources(db_session, org_id=org_id)
    assert len(rows) >= 1

    persisted = (
        db_session.query(IngestionSource)
        .filter(IngestionSource.org_id == org_id)
        .all()
    )
    assert len(persisted) >= 1

    target = next(
        (
            row
            for row in persisted
            if isinstance(row.config_json, dict)
            and row.config_json.get("photo_mode") == "placeholder_until_connected"
            and row.config_json.get("image_backfill_status") == "pending"
        ),
        None,
    )

    assert target is not None
    assert target.is_enabled is True
    assert isinstance(target.config_json, dict)
    assert target.config_json.get("state") == "MI"
    assert target.config_json.get("city") == "Detroit"
    assert target.config_json.get("photo_mode") == "placeholder_until_connected"
    assert target.config_json.get("image_backfill_status") == "pending"
    