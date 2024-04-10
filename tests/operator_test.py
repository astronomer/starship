import datetime
import os
from airflow import DAG
import pytest
from astronomer_starship.compat.starship_compatability import (
    StarshipAirflow,
    get_test_data,
)
from astronomer_starship.compat.starship_hook import StarshipAPIHook
from astronomer_starship.compat.starship_operator import StarshipOperator
from tests.conftest import manual_tests
from tests.api_integration_test import (
    get_extras,
)


@pytest.fixture
def starship_hook_and_starship(
    url_and_token_and_starship,
) -> tuple[StarshipAPIHook, StarshipAirflow]:
    (url, token, starship) = url_and_token_and_starship
    return StarshipAPIHook(webserver_url=url, **get_extras(url, token)), starship


def get_json_test_data(attrs, method=None):
    test_data = get_test_data(attrs=attrs, method=method)
    json_serializable = {
        k: v if not isinstance(v, datetime.datetime) else v.isoformat()
        for k, v in test_data.items()
    }
    return json_serializable


class TestStarshipApiHook:
    @manual_tests
    def test_get_dags(self, starship_hook_and_starship):
        hook, _ = starship_hook_and_starship
        dags = hook.get_dags()
        assert len(dags) > 0, dags

    @manual_tests
    def test_set_and_get_dag_runs(self, starship_hook_and_starship):
        hook, starship = starship_hook_and_starship
        post_payload = get_json_test_data(method="POST", attrs=starship.dag_run_attrs())
        set_runs = hook.set_dag_runs(dag_runs=[post_payload])
        assert set_runs
        assert (
            "dag_id" in set_runs
            or set_runs["error"] == "Integrity Error (Duplicate Record?)"
        )
        get_runs = hook.get_dag_runs(dag_id=post_payload["dag_id"])
        assert get_runs, get_runs == post_payload

    @manual_tests
    def test_set_and_get_task_instances(self, starship_hook_and_starship):
        hook, starship = starship_hook_and_starship
        post_payload = get_json_test_data(
            method="POST", attrs=starship.task_instance_attrs()
        )
        set_tis = hook.set_task_instances(task_instances=[post_payload])
        assert set_tis
        assert (
            "task_instances" in set_tis
            or set_tis["error"] == "Integrity Error (Duplicate Record?)"
        )
        get_tis = hook.get_task_instances(dag_id=post_payload["dag_id"], limit=1)
        assert "dag_run_count" in get_tis, get_tis
        assert len(get_tis["task_instances"]) == 1

    @manual_tests
    @pytest.mark.parametrize("action", ["unpause", "pause"])
    def test_patch_dag_state(self, starship_hook_and_starship, action):
        hook, _ = starship_hook_and_starship
        example_dag = hook.get_dags()[0]["dag_id"]
        resp = hook.set_dag_state(dag_id=example_dag, action=action)
        assert resp.status_code == 200, "dag_id" in resp.json()

    @manual_tests
    def test_get_latest_dagrun_state(self, starship_hook_and_starship):
        hook, starship = starship_hook_and_starship
        example_dag_run = get_test_data(starship.dag_run_attrs())
        latest_state = hook.get_latest_dagrun_state(dag_id=example_dag_run["dag_id"])
        assert latest_state == example_dag_run["state"]


@manual_tests
def test_starship_migration_operator():
    dag = DAG("test_dag", default_args={})
    starship_operator = StarshipOperator(
        task_id="test_operator",
        dag=dag,
    )
    dagrun_conf = {
        "source_webserver_url": "http://localhost:8080",
        "source_auth": ["admin", "admin"],
        "target_webserver_url": os.getenv("TARGET_WEBSERVER_URL"),
        "target_headers": (
            {"Authorization": f"Bearer {os.getenv('TARGET_TOKEN')}"}
            if os.getenv("TARGET_TOKEN")
            else None
        ),
        "target_auth": os.getenv("TARGET_AUTH"),
    }

    ctx = {"conf": dagrun_conf}
    starship_operator.execute(ctx)
