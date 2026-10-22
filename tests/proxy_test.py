import pytest


def test_af3_proxy_requires_authentication():
    pytest.importorskip("airflow.api_fastapi")
    from fastapi.testclient import TestClient

    from astronomer_starship._af3.starship import starship_app

    res = TestClient(starship_app).get(
        "/proxy",
        params={"url": "https://example.com/api/v1/health"},
        headers={"Starship-Proxy-Token": "anything"},
    )
    assert res.status_code == 401, res.text
