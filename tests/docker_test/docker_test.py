"""NOTE: These tests run _inside docker containers_ generated from the validation_test.py file."""

import os

import pytest

from astronomer_starship.common import get_test_data, normalize_for_comparison, normalize_test_data
from astronomer_starship.compat.starship_compatability import (
    StarshipCompatabilityLayer,
)

docker_test = pytest.mark.skipif(not bool(os.getenv("DOCKER_TEST")), reason="Not inside Docker container under test")


@pytest.fixture
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
    assert actual is None, actual


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
    assert actual is None, actual


@docker_test
def test_connections(starship):
    test_input = get_test_data(method="POST", attrs=starship.connection_attrs())
    actual = starship.set_connection(**test_input)
    assert actual == test_input, actual

    actual = starship.get_connections()
    assert test_input in actual, actual

    test_input = get_test_data(method="DELETE", attrs=starship.connection_attrs())
    actual = starship.delete_connection(**test_input)
    assert actual is None, actual


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
    actual_dag_runs = [dag_run for dag_run in actual["dag_runs"] if dag_run["run_id"] == run_id]
    assert len(actual_dag_runs) == 1, actual
    # Normalize and Filter both sides
    test_keys = set(test_input["dag_runs"][0].keys())
    filtered_actual = normalize_for_comparison({k: v for k, v in actual_dag_runs[0].items() if k in test_keys})
    expected = normalize_for_comparison(normalize_test_data(test_input["dag_runs"][0]))
    assert filtered_actual == expected, f"Actual: {filtered_actual}\nExpected: {expected}"

    # Set Task Instances
    test_input = get_test_data(method="POST", attrs=starship.task_instances_attrs())
    actual = starship.set_task_instances(**test_input)
    assert actual == test_input, actual

    # Get Task Instances
    actual = starship.get_task_instances(dag_id)
    actual_task_instances = actual["task_instances"]
    assert len(actual_task_instances) == 1, actual
    # Normalize and Filter both sides
    exclude_keys = {"dag_version_id", "trigger_timeout", "executor_config"}
    test_keys = set(test_input["task_instances"][0].keys()) - exclude_keys
    filtered_actual = normalize_for_comparison({k: v for k, v in actual_task_instances[0].items() if k in test_keys})
    filtered_expected = normalize_for_comparison(
        normalize_test_data({k: v for k, v in test_input["task_instances"][0].items() if k in test_keys})
    )
    assert filtered_actual == filtered_expected, f"Actual: {filtered_actual}\nExpected: {filtered_expected}"

    test_input = get_test_data(method="DELETE", attrs=starship.dag_runs_attrs())
    actual = starship.delete_dag_runs(**test_input)
    assert actual is None, actual


@docker_test
def test_task_instance_history(starship):
    """Test task instance history get/set operations."""
    from airflow import __version__
    from packaging.version import Version

    # Task instance history requires AF 2.6+
    if Version(__version__) < Version("2.6.0"):
        pytest.skip("task_instance_history requires Airflow 2.6+")

    # First create a dag_run and task_instance (prerequisite)
    dr_input = get_test_data(method="POST", attrs=starship.dag_runs_attrs())
    starship.set_dag_runs(**dr_input)

    ti_input = get_test_data(method="POST", attrs=starship.task_instances_attrs())
    starship.set_task_instances(**ti_input)

    # Get task instance history
    dag_id = ti_input["task_instances"][0]["dag_id"]
    actual = starship.get_task_instance_history(dag_id)

    # Verify structure
    assert "task_instances" in actual, f"Expected 'task_instances' key, got: {actual}"
    assert "dag_run_count" in actual, f"Expected 'dag_run_count' key, got: {actual}"

    # Set task instance history (re-post should work)
    if actual["task_instances"]:
        result = starship.set_task_instance_history(task_instances=actual["task_instances"])
        assert "task_instances" in result, f"Expected 'task_instances' in result, got: {result}"


@docker_test
def test_upsert_idempotency(starship):
    """Test that re-posting same data doesn't error (UPSERT / ON CONFLICT DO NOTHING)."""
    from airflow import __version__
    from packaging.version import Version

    # UPSERT only implemented in AF3
    if Version(__version__).major < 3:
        pytest.skip("UPSERT idempotency only implemented in AF3")

    # Create dag_run
    dr_input = get_test_data(method="POST", attrs=starship.dag_runs_attrs())
    first_result = starship.set_dag_runs(**dr_input)
    assert "dag_runs" in first_result, f"First dag_run insert failed: {first_result}"

    # Post same dag_run again - should succeed (not error due to ON CONFLICT DO NOTHING)
    second_result = starship.set_dag_runs(**dr_input)
    assert "dag_runs" in second_result, f"Second dag_run insert should succeed: {second_result}"

    # Same for task_instances
    ti_input = get_test_data(method="POST", attrs=starship.task_instances_attrs())
    first_ti = starship.set_task_instances(**ti_input)
    assert "task_instances" in first_ti, f"First task_instance insert failed: {first_ti}"

    second_ti = starship.set_task_instances(**ti_input)
    assert "task_instances" in second_ti, f"Second task_instance insert should succeed: {second_ti}"


@docker_test
def test_dag_version_id(starship):
    """Test AF3-specific dag_version_id functionality."""
    from airflow import __version__
    from packaging.version import Version

    # Skip for AF2 - dag_version_id only exists in AF3
    if Version(__version__).major < 3:
        pytest.skip("dag_version_id only exists in AF3")

    dag_id = "dag_0"

    # Check if the method exists (AF3 only)
    if not hasattr(starship, "get_latest_dag_version_id"):
        pytest.skip("get_latest_dag_version_id not implemented")

    # Get latest dag_version_id
    version_id = starship.get_latest_dag_version_id(dag_id)
    # May be None if DAG not parsed yet, or a UUID string
    assert version_id is None or isinstance(version_id, str), f"Expected None or str, got: {type(version_id)}"

    # If version exists, test update
    if version_id and hasattr(starship, "update_dag_version_id"):
        result = starship.update_dag_version_id(dag_id, version_id)
        assert result["dag_id"] == dag_id, f"Expected dag_id={dag_id}, got: {result}"
        assert "dag_runs_updated" in result, f"Expected 'dag_runs_updated' in result: {result}"
        assert "task_instances_updated" in result, f"Expected 'task_instances_updated' in result: {result}"
