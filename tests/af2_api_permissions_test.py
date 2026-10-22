"""The Airflow 2 Starship API must check the FAB permission matching the request method, not only config read."""

from types import SimpleNamespace

import pytest

from astronomer_starship.compat import AIRFLOW_V_2

if not AIRFLOW_V_2:
    pytest.skip("Airflow 2 only", allow_module_level=True)

from airflow.providers.fab.auth_manager.security_manager.override import (  # noqa: E402
    FabAirflowSecurityManagerOverride,
)
from airflow.security import permissions  # noqa: E402
from flask import Flask  # noqa: E402

from astronomer_starship._af2 import starship_api  # noqa: E402

SM = FabAirflowSecurityManagerOverride
CONFIG_READ = (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)

# The FAB default roles, plus config read so every role passes the Starship baseline.
VIEWER = {*SM.VIEWER_PERMISSIONS, CONFIG_READ}
OP = {*SM.VIEWER_PERMISSIONS, *SM.USER_PERMISSIONS, *SM.OP_PERMISSIONS, CONFIG_READ}
ADMIN = {*OP, *SM.ADMIN_PERMISSIONS}


class FakeSecurityManager:
    """Answers FAB's `check_authorization` from a fixed set of (action, resource) permissions"""

    check_authorization = SM.check_authorization

    def __init__(self, perms):
        self.perms = perms

    def has_access(self, action, resource):
        return (action, resource) in self.perms

    def can_access_some_dags(self, action, dag_id):
        if dag_id:
            return self.has_access(action, permissions.resource_name_for_dag(dag_id))
        return any(a == action and r.startswith(permissions.RESOURCE_DAG_PREFIX) for a, r in self.perms)


class FakeCompat:
    def __getattr__(self, name):
        if name.endswith("_attrs"):
            return lambda: {}
        return lambda **kwargs: {}


class FakeAuthManager:
    @staticmethod
    def is_logged_in():
        return True

    @staticmethod
    def is_authorized_view(**kwargs):
        return False

    @staticmethod
    def get_url_logout():
        return ""


@pytest.fixture
def client_as(monkeypatch):
    # Denied requests render Airflow's "no roles" page, which needs the full webserver; return a stub instead.
    for module in ("airflow.www.auth", "airflow.providers.fab.auth_manager.decorators.auth"):
        monkeypatch.setattr(f"{module}.render_template", lambda *args, **kwargs: "denied", raising=False)
        monkeypatch.setattr(f"{module}.get_auth_manager", FakeAuthManager, raising=False)
    monkeypatch.setattr(starship_api, "StarshipCompatabilityLayer", FakeCompat)

    def make(perms):
        app = Flask(__name__)
        app.appbuilder = SimpleNamespace(sm=FakeSecurityManager(perms))
        view = starship_api.StarshipApi()
        for name in dir(view):
            route = getattr(view, name)
            for url, methods in getattr(route, "_urls", []):
                app.add_url_rule(url, endpoint=name, view_func=route, methods=methods)
        return app.test_client()

    return make


@pytest.mark.parametrize(
    "method, path, kwargs",
    [
        ("GET", "/connections", {}),
        ("POST", "/connections", {"json": {}}),
        ("DELETE", "/connections", {}),
        ("POST", "/variables", {"json": {}}),
        ("DELETE", "/pools", {}),
        ("PATCH", "/dags", {"json": {"dag_id": "a"}}),
        ("DELETE", "/dag_runs", {}),
        ("POST", "/task_instances", {"json": {}}),
        ("POST", "/task_log", {"json": {}}),
        ("POST", "/xcom", {"json": {}}),
        ("GET", "/env_vars", {}),
    ],
)
def test_viewer_cannot_read_secrets_or_write(client_as, method, path, kwargs):
    res = client_as(VIEWER).open(path, method=method, **kwargs)
    assert res.status_code == 403, res.text


def test_viewer_keeps_baseline_access(client_as):
    client = client_as(VIEWER)
    assert client.get("/health").status_code == 200
    assert client.get("/dags").status_code == 200
    assert client.get("/dag_runs").status_code == 200


def test_no_config_read_is_denied_everywhere(client_as):
    client = client_as(ADMIN - {CONFIG_READ})
    assert client.get("/health").status_code == 403
    assert client.get("/pools").status_code == 403


@pytest.mark.parametrize(
    "method, path",
    [
        ("GET", "/connections"),
        ("POST", "/connections"),
        ("DELETE", "/connections"),
        ("POST", "/variables"),
        ("DELETE", "/pools"),
        ("PATCH", "/dags"),
        ("POST", "/dag_runs"),
        ("DELETE", "/dag_runs"),
        ("POST", "/task_instances"),
        ("POST", "/task_instance_history"),
        ("POST", "/task_log"),
        ("DELETE", "/task_log"),
        ("POST", "/xcom"),
        ("DELETE", "/xcom"),
    ],
)
def test_op_can_write(client_as, method, path):
    res = client_as(OP).open(path, method=method, json={})
    assert res.status_code == 200, res.text


def test_dags_patch_maps_to_dag_edit(client_as):
    """`PATCH /dags` needs can_edit on the DAG, either for all DAGs or for the DAG in the request"""
    read_all = VIEWER | {(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)}
    edit_a = read_all | {(permissions.ACTION_CAN_EDIT, permissions.resource_name_for_dag("a"))}
    assert client_as(read_all).patch("/dags", json={"dag_id": "a"}).status_code == 403
    assert client_as(edit_a).patch("/dags", json={"dag_id": "a"}).status_code == 200
    assert client_as(edit_a).patch("/dags", json={"dag_id": "b"}).status_code == 403


def test_dag_id_in_query_and_body_must_match(client_as):
    """FAB rejects a request that names one DAG in the query string and another in the body"""
    edit_a = VIEWER | {(permissions.ACTION_CAN_EDIT, permissions.resource_name_for_dag("a"))}
    res = client_as(edit_a).patch("/dags?dag_id=a", json={"dag_id": "b"})
    assert res.status_code == 403
