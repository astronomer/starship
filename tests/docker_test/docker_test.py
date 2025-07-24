"""NOTE: These tests run _inside docker containers_ generated from the validation_test.py file."""

import json
import os
import pytest

from http import HTTPStatus

from astronomer_starship.compat.starship_compatability import (
    StarshipCompatabilityLayer,
    get_test_data,
)

docker_test = pytest.mark.skipif(
    not bool(os.getenv("DOCKER_TEST")), reason="Not inside Docker container under test"
)


@pytest.fixture()
def app():
    from airflow.www.app import create_app

    app = create_app(testing=True)
    yield app


@pytest.fixture(autouse=True)
def app_context(app):
    with app.app_context():
        yield


@pytest.fixture(scope="session")
def starship():
    return StarshipCompatabilityLayer()


@docker_test
def test_airflow_version(starship):
    """Test the Airflow version endpoint."""
    from airflow import __version__

    actual = starship.get_airflow_version()
    assert actual == __version__


@docker_test
def test_info(starship):
    """Test the info endpoint."""
    from airflow import __version__ as airflow_version
    from astronomer_starship import __version__ as starship_version

    actual = starship.get_info()
    assert actual == {
        "airflow_version": airflow_version,
        "starship_version": starship_version,
    }


@docker_test
def test_variables(starship):
    test_input = get_test_data(method="POST", attrs=starship.variable_attrs())
    actual = starship.set_variable(**test_input)
    assert actual == test_input, actual

    actual = starship.get_variables()
    assert test_input in actual, actual

    test_input = get_test_data(method="DELETE", attrs=starship.variable_attrs())
    actual = starship.delete_variable(**test_input)
    assert actual.status_code == HTTPStatus.NO_CONTENT, actual


@docker_test
def test_pools(starship):
    from copy import copy

    test_input = get_test_data(method="POST", attrs=starship.pool_attrs())
    expected = copy(test_input)

    # switch "pool" to "name"
    test_input["pool"] = test_input["name"]
    del test_input["name"]

    actual = starship.set_pool(**test_input)
    assert actual == expected, actual

    actual = starship.get_pools()
    assert expected in actual, actual

    test_input = get_test_data(method="DELETE", attrs=starship.pool_attrs())
    actual = starship.delete_pool(**test_input)
    assert actual.status_code == HTTPStatus.NO_CONTENT, actual


@docker_test
def test_connections(starship):
    test_input = get_test_data(method="POST", attrs=starship.connection_attrs())
    actual = starship.set_connection(**test_input)
    assert actual == test_input, actual

    actual = starship.get_connections()
    assert test_input in actual, actual

    test_input = get_test_data(method="DELETE", attrs=starship.connection_attrs())
    actual = starship.delete_connection(**test_input)
    assert actual.status_code == HTTPStatus.NO_CONTENT, actual


@docker_test
def test_dags(starship):
    test_input = get_test_data(method="PATCH", attrs=starship.dag_attrs())
    actual = starship.set_dag_is_paused(**test_input)
    assert actual == test_input, actual

    test_input = get_test_data(attrs=starship.dag_attrs())
    actual = starship.get_dags()
    actual_dags = [dag for dag in actual if dag["dag_id"] == test_input["dag_id"]]
    assert len(actual_dags) == 1, actual_dags

    # not predictable, so remove it
    del actual_dags[0]["fileloc"]
    del test_input["fileloc"]

    # not predictable (sorting), so remove it
    del actual_dags[0]["tags"]
    del test_input["tags"]

    assert actual_dags[0] == test_input, actual_dags[0]


@docker_test
def test_dag_runs_and_task_instances(starship):
    test_input = get_test_data(method="POST", attrs=starship.dag_runs_attrs())
    dag_id = test_input["dag_runs"][0]["dag_id"]

    # Set Dag Runs
    actual = starship.set_dag_runs(**test_input)
    expected = dict({"dag_run_count": 1}, **test_input)
    assert actual == expected, actual

    # Get Dag Runs
    run_id = test_input["dag_runs"][0]["run_id"]
    actual = starship.get_dag_runs(dag_id)
    actual_dag_runs = [
        dag_run for dag_run in actual["dag_runs"] if dag_run["run_id"] == run_id
    ]
    assert len(actual_dag_runs) == 1, actual
    assert json.dumps(actual_dag_runs[0], default=str) in json.dumps(
        test_input["dag_runs"], default=str
    ), actual_dag_runs

    # Set Task Instances
    test_input = get_test_data(method="POST", attrs=starship.task_instances_attrs())
    actual = starship.set_task_instances(**test_input)
    assert actual == test_input, actual

    # Get Task Instances
    actual = starship.get_task_instances(dag_id)
    actual_task_instances = actual["task_instances"]
    assert len(actual_task_instances) == 1, actual
    test_input["task_instances"][0]["executor_config"] = None
    if "trigger_timeout" in actual_task_instances[0]:
        del actual_task_instances[0]["trigger_timeout"]
    if "trigger_timeout" in test_input["task_instances"][0]:
        del test_input["task_instances"][0]["trigger_timeout"]
    assert json.dumps(actual_task_instances, default=str) in json.dumps(
        test_input["task_instances"], default=str
    ), actual_task_instances

    test_input = get_test_data(method="DELETE", attrs=starship.dag_runs_attrs())
    actual = starship.delete_dag_runs(**test_input)
    assert actual.status_code == HTTPStatus.NO_CONTENT, actual
