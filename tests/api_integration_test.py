import json
from typing import Any, Dict

import pytest
import requests

from astronomer_starship.common import get_test_data
from astronomer_starship.compat.starship_compatability import (
    StarshipCompatabilityLayer,
)
from tests.conftest import manual_tests

URLS_AND_TOKENS = {
    "localhost": ("http://localhost:8080", None),
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
    # noinspection DuplicatedCode
    actual = requests.post(f"{url}/{route}", json=test_input, **get_extras(url, token))
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert actual.json()["error"] == "Integrity Error (Duplicate Record?)", actual.text
    else:
        assert actual.json() == test_input, actual.text
    # Get One
    actual = requests.get(f"{url}/{route}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    assert test_input in actual.json(), actual.text


def delete(route, test_input, token, url):
    actual = requests.delete(f"{url}/{route}", params=test_input, **get_extras(url, token))
    assert actual.status_code == 204, actual.text


@pytest.fixture(
    params=list(URLS_AND_TOKENS.values()),
    ids=list(URLS_AND_TOKENS.keys()),
)
def url_and_token_and_starship(request):
    (url, token) = request.param
    version = requests.get(f"{url}/api/starship/airflow_version", **get_extras(url, token)).text
    return url, token, StarshipCompatabilityLayer(airflow_version=version)


@manual_tests
def test_integration_variables(url_and_token_and_starship):
    (url, token, starship) = url_and_token_and_starship
    route = "api/starship/variables"
    test_input = get_test_data(method="POST", attrs=starship.variable_attrs())
    set_and_get(route, test_input, token, url)
    delete(route, test_input, token, url)


@manual_tests
def test_integration_pools(url_and_token_and_starship):
    (url, token, starship) = url_and_token_and_starship
    route = "api/starship/pools"
    test_input = get_test_data(method="POST", attrs=starship.pool_attrs())
    set_and_get(route, test_input, token, url)
    delete(route, test_input, token, url)


@manual_tests
def test_integration_connections(url_and_token_and_starship):
    (url, token, starship) = url_and_token_and_starship
    route = "api/starship/connections"
    test_input = get_test_data(method="POST", attrs=starship.connection_attrs())
    set_and_get(route, test_input, token, url)
    delete(route, test_input, token, url)


@manual_tests
def test_integration_dags(url_and_token_and_starship):
    (url, token, starship) = url_and_token_and_starship
    route = "api/starship/dags"
    expected_patch = get_test_data(method="PATCH", attrs=starship.dag_attrs())

    # Create One
    actual = requests.patch(f"{url}/{route}", json=expected_patch, **get_extras(url, token))
    # noinspection DuplicatedCode
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert actual.text == '{"error": "Duplicate Record"}', actual.text
    else:
        assert actual.json() == expected_patch, actual.text

    # Get Many
    expected_get = get_test_data(attrs=starship.dag_attrs())
    actual = requests.get(f"{url}/{route}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    for dag in actual.json():
        if dag["dag_id"] == expected_get["dag_id"]:
            assert dag["dag_id"] == expected_get["dag_id"], actual.text
            assert dag["schedule_interval"] == expected_get["schedule_interval"], actual.text
            assert dag["is_paused"] == expected_get["is_paused"], actual.text
            assert dag["description"] == expected_get["description"], actual.text
            assert dag["owners"] == expected_get["owners"], actual.text
            assert sorted(dag["tags"]) == sorted(expected_get["tags"]), actual.text


@manual_tests
def test_integration_dag_runs_and_task_instances(url_and_token_and_starship):
    (url, token, starship) = url_and_token_and_starship
    dr_route = "api/starship/dag_runs"
    run_id = "manual__1970-01-01T00:00:00+00:00"
    dag_id = "dag_0"

    # delete dag
    requests.delete(f"{url}/api/v1/dags/{dag_id}", **get_extras(url, token))

    dr_test_input = get_test_data(method="POST", attrs=starship.dag_runs_attrs())
    dr_test_input = json.loads(json.dumps(dr_test_input, default=str))

    # Set DAG Runs
    actual = requests.post(f"{url}/{dr_route}", json=dr_test_input, **get_extras(url, token))
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert actual.json()["error"] == "Integrity Error (Duplicate Record?)", actual.text
    else:
        assert actual.json()["dag_runs"] == dr_test_input["dag_runs"], actual.text

    # Get DAG Runs
    actual = requests.get(f"{url}/{dr_route}?dag_id={dag_id}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    actual_dag_runs = [dag_run for dag_run in actual.json()["dag_runs"] if dag_run["run_id"] == run_id]
    assert len(actual_dag_runs) == 1, actual.json()
    actual_dag_run = actual_dag_runs[0]
    actual_dag_run = {
        k: (v.replace("1970-01-01T00", "1970-01-01 00") if isinstance(v, str) and k != "run_id" else v)
        for k, v in actual_dag_run.items()
    }
    assert dr_test_input["dag_runs"][0] == actual_dag_run, actual_dag_run

    ti_route = "api/starship/task_instances"

    ti_test_input = get_test_data(method="POST", attrs=starship.task_instances_attrs())
    ti_test_input = json.loads(json.dumps(ti_test_input, default=str))

    # Set Task Instances
    actual = requests.post(f"{url}/{ti_route}", json=ti_test_input, **get_extras(url, token))
    assert actual.status_code in [200, 409], actual.text
    if actual.status_code == 409:
        assert actual.json()["error"] == "Integrity Error (Duplicate Record?)", actual.text
    else:
        assert actual.json()["task_instances"] == ti_test_input["task_instances"], actual.text

    # Get Task Instances
    actual = requests.get(f"{url}/{ti_route}?dag_id={dag_id}", **get_extras(url, token))
    assert actual.status_code == 200, actual.text
    actual_task_instances = [
        task_instance
        for task_instance in actual.json()["task_instances"]
        if task_instance["run_id"] == ti_test_input["task_instances"][0]["run_id"]
    ]
    assert len(actual_task_instances) == 1, actual.json()
    actual_task_instance = actual_task_instances[0]
    actual_task_instance = {
        k: (v.replace("1970-01-01T00", "1970-01-01 00") if isinstance(v, str) and k != "run_id" else v)
        for k, v in actual_task_instance.items()
    }
    # gets blanked out
    ti_test_input["task_instances"][0]["executor_config"] = None

    if "trigger_timeout" in actual_task_instance:
        del actual_task_instance["trigger_timeout"]
    if "trigger_timeout" in ti_test_input["task_instances"][0]:
        del ti_test_input["task_instances"][0]["trigger_timeout"]

    assert actual_task_instance == ti_test_input["task_instances"][0], actual_task_instance

    # Delete test
    delete(dr_route, dr_test_input["dag_runs"][0], token, url)
