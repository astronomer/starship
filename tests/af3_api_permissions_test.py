"""The Starship API must check the permission matching the request method, not only config read."""

import threading

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

    def __getattr__(self, name):
        if name.endswith("_attrs"):
            return dict
        return lambda **kwargs: {}


class DagScopedAuthManager(SimpleAuthManager):
    """Grants DAG access on DAG `a` only, like FAB per-DAG permissions"""

    threads = []

    def is_authorized_dag(self, *, details=None, **kwargs):
        self.threads.append(threading.current_thread().name)
        return bool(details and details.id == "a") and super().is_authorized_dag(details=details, **kwargs)


@pytest.fixture
def client_as(monkeypatch):
    def make(role, auth_manager=SimpleAuthManager):
        monkeypatch.setattr("airflow.api_fastapi.core_api.security.get_auth_manager", auth_manager)
        monkeypatch.setattr(starship_api, "get_auth_manager", auth_manager)
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


@pytest.mark.parametrize("denied", ["is_authorized_connection", "is_authorized_variable"])
def test_env_vars_needs_connection_and_variable_read(client_as, monkeypatch, denied):
    """Environment variables can hold connections and variables, so `/env_vars` needs read access to both"""
    assert client_as("op").get("/env_vars").status_code == 200
    monkeypatch.setattr(SimpleAuthManager, denied, lambda *args, **kwargs: False)
    assert client_as("op").get("/env_vars").status_code == 403


@pytest.mark.parametrize(
    "method, path, kwargs, status",
    [
        ("GET", "/dag_runs", {"params": {"dag_id": "a"}}, 200),
        ("GET", "/dag_runs", {"params": {"dag_id": "b"}}, 403),
        # Without a dag_id, the user needs access to all DAGs
        ("GET", "/dag_runs", {}, 403),
        ("GET", "/dags", {}, 403),
        ("PATCH", "/dags", {"json": {"dag_id": "a", "is_paused": True}}, 200),
        # The handlers prefer the body dag_id over the query string one
        ("PATCH", "/dags", {"params": {"dag_id": "a"}, "json": {"dag_id": "b", "is_paused": True}}, 403),
        ("DELETE", "/dag_runs", {"params": {"dag_id": "a"}, "json": {"dag_id": "b"}}, 403),
        # Bulk bodies name a DAG per record
        ("POST", "/dag_runs", {"json": {"dag_runs": [{"dag_id": "a"}]}}, 200),
        ("POST", "/dag_runs", {"json": {"dag_runs": [{"dag_id": "a"}, {"dag_id": "b"}]}}, 403),
        ("POST", "/task_instances", {"params": {"dag_id": "a"}, "json": {"task_instances": [{"dag_id": "b"}]}}, 403),
        ("POST", "/task_instance_history", {"json": {"task_instances": [{"dag_id": "a"}]}}, 200),
        ("POST", "/xcom", {"json": {"dag_id": "b"}}, 403),
        ("DELETE", "/task_log", {"params": {"dag_id": "a"}}, 200),
    ],
)
def test_dag_access_covers_every_dag_in_the_request(client_as, method, path, kwargs, status):
    res = client_as("op", DagScopedAuthManager).request(method, path, **kwargs)
    assert res.status_code == status, res.text


@pytest.mark.parametrize("kwargs", [{"json": {"dag_id": ["a"]}}, {"content": b"{not json"}])
def test_malformed_dag_ids_are_rejected(client_as, kwargs):
    res = client_as("op", DagScopedAuthManager).request("PATCH", "/dags", **kwargs)
    assert res.status_code == 400, res.text


def test_dag_access_check_runs_in_threadpool(client_as):
    DagScopedAuthManager.threads.clear()
    client_as("op", DagScopedAuthManager).get("/dag_runs", params={"dag_id": "a"})
    assert DagScopedAuthManager.threads == ["AnyIO worker thread"]


def test_unregistered_method_is_not_allowed(client_as):
    assert client_as("admin").put("/pools").status_code == 405


def test_dag_access_passes_team_with_multi_team(client_as, monkeypatch):
    from airflow.models import DagModel

    if not hasattr(DagModel, "get_team_name"):
        pytest.skip("Airflow 3.1+ only")
    seen = []

    class TeamAuthManager(SimpleAuthManager):
        def is_authorized_dag(self, *, details=None, **kwargs):
            seen.append(details)
            return True

    monkeypatch.setenv("AIRFLOW__CORE__MULTI_TEAM", "True")
    monkeypatch.setattr(DagModel, "get_team_name", staticmethod(lambda dag_id: f"team_{dag_id}"))
    assert client_as("op", TeamAuthManager).get("/dag_runs", params={"dag_id": "a"}).status_code == 200
    assert [(d.id, d.team_name) for d in seen] == [("a", "team_a")]
