from app.services.ingestion_scheduler_service import compute_next_daily_sync, list_default_daily_markets


def test_default_daily_market_list_has_multiple_markets():
    markets = list_default_daily_markets()
    assert len(markets) >= 5


def test_next_daily_sync_returns_datetime():
    dt = compute_next_daily_sync()
    assert hasattr(dt, "hour")
