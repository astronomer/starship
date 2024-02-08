from typing import Dict, Any

import pytest
import requests

from tests.conftest import manual_tests


def example_dag_run(dag_id, run_id):
    return {
        "dag_id": dag_id,
        "queued_at": "1970-01-01T00:00:01+00:00",
        "execution_date": "1970-01-01T00:00:01+00:00",
        "start_date": "1970-01-01T00:00:00+00:00",
        "end_date": "1970-01-01T00:00:01+00:00",
        "state": "SUCCESS",
        "run_id": run_id,
        "creating_job_id": 123,
        "external_trigger": True,
        "run_type": "manual",
        "conf": None,
        "data_interval_start": "1970-01-01T00:00:01+00:00",
        "data_interval_end": "1970-01-01T00:00:01+00:00",
        "last_scheduling_decision": "1970-01-01T00:00:01+00:00",
        "dag_hash": "dag_hash",
    }


def example_task_instance(dag_id, run_id) -> dict:
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": "task_id",
        "start_date": "1970-01-01T00:00:00+00:00",
        "end_date": "1970-01-01T00:00:01+00:00",
        "duration": 1.0,
        "state": "SUCCESS",
        "max_tries": 2,
        "hostname": "hostname",
        "unixname": "unixname",
        "job_id": 3,
        "pool": "pool",
        "pool_slots": 4,
        "queue": "queue",
        "priority_weight": 5,
        "operator": "operator",
        "queued_dttm": "1970-01-01T00:00:01+00:00",
        "queued_by_job_id": 6,
        "pid": 7,
        # executor_config
        "external_executor_id": "external_executor_id",
        "trigger_id": None,
        "trigger_timeout": "1970-01-01T00:00:01+00:00",
        # "next_method": "next_method",
        # "next_kwargs": {},
    }


def get_extras(deployment_url: str, token: str) -> Dict[str, Any]:
    """Use Bearer auth if astro else admin:admin if local"""
    return (
        {
            "headers": {
                # "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }
        }
        if "localhost" not in deployment_url
        else {
            # "headers": {"Content-Type": "application/json"},
            "auth": ("admin", "admin"),
        }
    )


def set_and_get(route, test_input, token, url):
    actual = requests.post(f"{url}/{route}", json=test_input, **get_extras(url, token))
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert (
            actual.json()["error"] == "Integrity Error (Duplicate Record?)"
        ), actual.text
    else:
        assert actual.json() == test_input, actual.text
    # Get One
    actual = requests.get(f"{url}/{route}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    assert test_input in actual.json(), actual.text


@pytest.fixture(
    params=[
        ("http://localhost:8080", None),
        # ("http://localhost:8080", None)
    ],
    ids=["localhost"],
)
def url_and_token(request):
    (url, token) = request.param
    return url, token


def test_integration_variables(url_and_token):
    [url, token] = url_and_token
    route = "api/starship/variables"
    test_input = {
        "key": "test_key",
        "val": "test_value",
        "description": "test_description",
    }
    set_and_get(route, test_input, token, url)


def test_integration_pools(url_and_token):
    (url, token) = url_and_token
    route = "api/starship/pools"
    test_input = {
        "name": "test_name",
        "slots": 1,
        "description": "test_description",
    }
    set_and_get(route, test_input, token, url)


def test_integration_connections(url_and_token):
    (url, token) = url_and_token
    route = "api/starship/connections"
    test_input = {
        "conn_id": "conn_id",
        "conn_type": "conn_type",
        "host": "host",
        "port": 1234,
        "schema": "schema",
        "login": "login",
        "password": "password",  # pragma: allowlist secret
        "extra": "extra",
        "description": "description",
    }
    set_and_get(route, test_input, token, url)


@manual_tests
def test_integration_dags(url_and_token):
    """This test requires a real DAG that exists already"""
    (url, token) = url_and_token
    route = "api/starship/dags"
    test_input = {
        "dag_id": "dag_id",
        "is_paused": True,
    }
    expected_patch = test_input
    expected_get = {
        "dag_id": "dag_0",
        "schedule_interval": "@once",
        "is_paused": True,
        "fileloc": "/usr/local/airflow/dags/dag0.py",
        "description": None,
        "owners": "baz",
        "tags": ["bar", "foo"],
        "dag_run_count": 0,
        "task_count": 0,
    }

    # Create One
    actual = requests.patch(f"{url}/{route}", json=test_input, **get_extras(url, token))
    # noinspection DuplicatedCode
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert actual.text == '{"error": "Duplicate Record"}', actual.text
    else:
        assert actual.json() == expected_patch, actual.text

    # Get Many
    actual = requests.get(f"{url}/{route}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    for dag in actual.json():
        if dag["dag_id"] == expected_get["dag_id"]:
            assert dag["dag_id"] == expected_get["dag_id"], actual.text
            assert (
                dag["schedule_interval"] == expected_get["schedule_interval"]
            ), actual.text
            assert dag["is_paused"] == expected_get["is_paused"], actual.text
            assert dag["fileloc"] == expected_get["fileloc"], actual.text
            assert dag["description"] == expected_get["description"], actual.text
            assert dag["owners"] == expected_get["owners"], actual.text
            assert sorted(dag["tags"]) == sorted(expected_get["tags"]), actual.text


@manual_tests
def test_get_task_instances():
    from astronomer_starship.starship_api import StarshipCompatabilityLayer

    actual = StarshipCompatabilityLayer().get_task_instances("dag_99")
    expected = {"dag_run_count": {"dag_run_count": 0}, "task_instances": []}
    assert actual == expected, actual


@manual_tests
def test_integration_dag_runs_and_task_instances(url_and_token):
    """This test requires a real DAG with DAG Runs that exists already"""
    (url, token) = url_and_token
    route = "api/starship/dag_runs"
    run_id = "manual__1970-01-01T00:00:00+00:00"
    dag_id = "dag_0"

    # delete dag
    requests.delete(f"{url}/api/v1/dags/{dag_id}", **get_extras(url, token))

    test_input = {
        "dag_runs": [example_dag_run(dag_id, run_id)],
        "dag_id": dag_id,
    }

    # Create Many (one)
    actual = requests.post(f"{url}/{route}", json=test_input, **get_extras(url, token))
    assert actual.status_code in [200, 409], actual.text
    print(actual.json())
    if actual.status_code == 409:
        assert (
            actual.json()["error"] == "Integrity Error (Duplicate Record?)"
        ), actual.text
    else:
        # This key gets deleted
        del test_input["dag_runs"][0]["conf"]
        assert actual.json()["dag_runs"] == test_input["dag_runs"], actual.text

    # Get Many (one)
    actual = requests.get(f"{url}/{route}?dag_id={dag_id}", **get_extras(url, token))
    print("DR:\n", actual.text, "\n")
    assert actual.status_code == 200, actual.text
    actual_dag_runs = [
        dag_run for dag_run in actual.json()["dag_runs"] if dag_run["run_id"] == run_id
    ]
    assert len(actual_dag_runs) == 1, actual.json()
    actual_dag_run = actual_dag_runs[0]
    del actual_dag_run["conf"]
    assert test_input["dag_runs"][0] == actual_dag_run, actual.text

    route = "api/starship/task_instances"
    requests.delete(f"{url}/api/v1/dags/{dag_id}", **get_extras(url, token))

    test_input = {
        "task_instances": [example_task_instance(dag_id, run_id)],
        "dag_id": dag_id,
    }

    # Create Many (one)
    actual = requests.post(f"{url}/{route}", json=test_input, **get_extras(url, token))
    print("TI:\n", actual.text, "\n")
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert (
            actual.json()["error"] == "Integrity Error (Duplicate Record?)"
        ), actual.text
    else:
        # This key gets deleted
        # del test_input['task_instances'][0]['executor_config']
        assert (
            actual.json()["task_instances"] == test_input["task_instances"]
        ), actual.text

    # Get Many (one)
    actual = requests.get(f"{url}/{route}?dag_id={dag_id}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    actual_task_instances = [
        dag_run
        for dag_run in actual.json()["task_instances"]
        if dag_run["run_id"] == test_input["task_instances"][0]["run_id"]
    ]
    print("TI:\n", actual.text, "\n")
    assert len(actual_task_instances) == 1, actual.json()
    actual_task_instance = actual_task_instances[0]
    assert actual_task_instance == test_input["task_instances"][0]
