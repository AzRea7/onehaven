import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client_with_auth_headers():
    client = TestClient(app)
    headers = {
        "X-Org-Slug": "test-org",
        "X-User-Email": "owner@test.com",
        "X-User-Role": "owner",
    }
    return client, headers