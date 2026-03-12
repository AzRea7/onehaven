from fastapi.testclient import TestClient

from app.main import app


def test_ingestion_overview_requires_auth():
    client = TestClient(app)
    resp = client.get("/api/ingestion/overview")
    assert resp.status_code in {401, 403}


def test_ingestion_sources_list_contract(client_with_auth_headers):
    client, headers = client_with_auth_headers
    resp = client.get("/api/ingestion/sources", headers=headers)
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)