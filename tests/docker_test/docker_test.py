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
    # Filter to POST-able keys so read-only compat-layer fields (e.g. `team_name` on AF3.2+) don't break equality.
    assert {k: v for k, v in actual.items() if k in test_input} == test_input, actual

    actual = starship.get_variables()
    assert any(test_input.items() <= a.items() for a in actual), actual

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
    # Filter to POST-able keys so read-only compat-layer fields (e.g. `team_name` on AF3.2+) don't break equality.
    assert {k: v for k, v in actual.items() if k in expected} == expected, actual

    actual = starship.get_pools()
    assert any(expected.items() <= a.items() for a in actual), actual

    test_input = get_test_data(method="DELETE", attrs=starship.pool_attrs())
    actual = starship.delete_pool(**test_input)
    assert actual is None, actual


@docker_test
def test_connections(starship):
    test_input = get_test_data(method="POST", attrs=starship.connection_attrs())
    actual = starship.set_connection(**test_input)
    # Filter to POST-able keys so read-only compat-layer fields (e.g. `team_name` on AF3.2+) don't break equality.
    assert {k: v for k, v in actual.items() if k in test_input} == test_input, actual

    actual = starship.get_connections()
    assert any(test_input.items() <= a.items() for a in actual), actual

    test_input = get_test_data(method="DELETE", attrs=starship.connection_attrs())
    actual = starship.delete_connection(**test_input)
    assert actual is None, actual


@docker_test
def test_dags(starship):
    test_input = get_test_data(method="PATCH", attrs=starship.dag_attrs())
    actual = starship.set_dag_is_paused(**test_input)
    assert actual == test_input, actual

    test_input = get_test_data(attrs=starship.dag_attrs())
    result = starship.get_dags()
    # get_dags returns {"dags": [...], "total_dag_count": N} (>=2.11);
    # older releases returned a bare list.
    actual = result["dags"] if isinstance(result, dict) else result
    actual_dags = [dag for dag in actual if dag["dag_id"] == test_input["dag_id"]]
    assert len(actual_dags) == 1, actual_dags

    # not predictable, so remove it
    del actual_dags[0]["fileloc"]

    # not predictable (sorting), so remove it
    del actual_dags[0]["tags"]

    # Row-shape assertion should ignore keys that only appear in test_input as
    # query-param descriptors (limit/offset/search), which are never row fields.
    filtered_input = {k: v for k, v in test_input.items() if k in actual_dags[0]}
    assert actual_dags[0] == filtered_input, actual_dags[0]


@docker_test
def test_dags_pagination_and_search(starship):
    # Response wraps the list plus a total that reflects the filter, not the page window.
    result = starship.get_dags()
    assert isinstance(result, dict), result
    assert "dags" in result and "total_dag_count" in result, result
    all_dags = result["dags"]
    total = result["total_dag_count"]
    assert total == len(all_dags), (total, len(all_dags))

    # Paged fetch returns a slice of the same set.
    if total >= 2:
        page = starship.get_dags(limit=1, offset=0)
        assert len(page["dags"]) == 1, page
        assert page["total_dag_count"] == total, page

        next_page = starship.get_dags(limit=1, offset=1)
        assert len(next_page["dags"]) == 1, next_page
        assert next_page["dags"][0]["dag_id"] != page["dags"][0]["dag_id"], (page, next_page)

    # Search restricts total_dag_count, not just the page window.
    if all_dags:
        target = all_dags[0]["dag_id"]
        hit = starship.get_dags(search=target)
        assert hit["total_dag_count"] >= 1, hit
        assert any(d["dag_id"] == target for d in hit["dags"]), hit

    miss = starship.get_dags(search="__nonexistent_starship_test_dag__")
    assert miss == {"dags": [], "total_dag_count": 0}, miss


@docker_test
def test_dags_search_field(starship):
    # search_field=None|""|"bogus" must fall back to OR-across-all (regression:
    # `field_filters.get(x) or or_(...)` used to raise TypeError on the returned
    # SQLAlchemy clause via __bool__).
    result = starship.get_dags()
    all_dags = result["dags"]
    if not all_dags:
        return  # No DAGs to filter against; test is a no-op.

    sample = all_dags[0]
    dag_id = sample["dag_id"]

    # search_field=dag_id: matches the sample DAG by its id.
    by_id = starship.get_dags(search=dag_id, search_field="dag_id")
    assert by_id["total_dag_count"] >= 1, by_id
    assert any(d["dag_id"] == dag_id for d in by_id["dags"]), by_id

    # search_field=owner: substring-match on owners.
    if sample.get("owners"):
        owner_frag = sample["owners"].split(",")[0].strip()
        if owner_frag:
            by_owner = starship.get_dags(search=owner_frag, search_field="owner")
            assert by_owner["total_dag_count"] >= 1, by_owner

    # search_field=tag: substring-match on any tag.
    if sample.get("tags"):
        tag_frag = sample["tags"][0]
        by_tag = starship.get_dags(search=tag_frag, search_field="tag")
        assert by_tag["total_dag_count"] >= 1, by_tag
        assert any(dag_id == d["dag_id"] for d in by_tag["dags"]), by_tag

    # Unknown search_field values must not error; they degrade to the
    # OR-across-all behaviour of an unset field.
    any_any = starship.get_dags(search=dag_id)
    for field in ("", "bogus", "any"):
        alt = starship.get_dags(search=dag_id, search_field=field)
        assert alt["total_dag_count"] == any_any["total_dag_count"], (field, alt, any_any)


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
