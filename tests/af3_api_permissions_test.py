"""The Starship API must check the permission matching the request method, not only config read."""

import pytest

pytest.importorskip("airflow.api_fastapi")

from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager  # noqa: E402
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser  # noqa: E402
from airflow.api_fastapi.core_api.security import get_user  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from astronomer_starship._af3 import starship_api  # noqa: E402

CONNECTION = {"conn_id": "my_conn", "conn_type": "http"}


class FakeCompat:
    @classmethod
    def connection_attrs(cls):
        return starship_api.StarshipCompatabilityLayer().connection_attrs()

    def set_connection(self, **kwargs):
        return kwargs

    def get_connections(self):
        return []

    def delete_connection(self, **kwargs):
        return None

    @staticmethod
    def get_env_vars():
        return {}


@pytest.fixture
def client_as(monkeypatch):
    monkeypatch.setattr("airflow.api_fastapi.core_api.security.get_auth_manager", SimpleAuthManager)

    def make(role):
        app = starship_api.StarshipApi()
        app.dependency_overrides[get_user] = lambda: SimpleAuthManagerUser(username=role, role=role)
        app.dependency_overrides[starship_api.starship_compat] = FakeCompat
        return TestClient(app)

    return make


@pytest.mark.parametrize(
    "method, path, kwargs",
    [
        ("GET", "/connections", {}),
        ("POST", "/connections", {"json": CONNECTION}),
        ("DELETE", "/connections", {"params": {"conn_id": "my_conn"}}),
        ("POST", "/variables", {"json": {"key": "k", "val": "v"}}),
        ("DELETE", "/pools", {"params": {"name": "p"}}),
        ("DELETE", "/dag_runs", {}),
        ("POST", "/xcom", {"json": {}}),
        ("GET", "/env_vars", {}),
    ],
)
def test_viewer_cannot_read_secrets_or_write(client_as, method, path, kwargs):
    res = client_as("viewer").request(method, path, **kwargs)
    assert res.status_code == 403, res.text


def test_viewer_keeps_baseline_access(client_as):
    assert client_as("viewer").get("/health").status_code == 200


def test_op_can_write_connections(client_as):
    client = client_as("op")
    assert client.get("/connections").status_code == 200
    res = client.post("/connections", json=CONNECTION)
    assert res.status_code == 200, res.text
    assert res.json() == CONNECTION
