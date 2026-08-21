import pytest

from astronomer_starship._af3.starship_compatability import (
    StarshipAirflow30,
    StarshipAirflow31,
    StarshipAirflow32,
    StarshipAirflow33,
    StarshipCompatabilityLayer,
)


@pytest.mark.parametrize(
    "airflow_version,expected_cls",
    [
        ("3.0.0", StarshipAirflow30),
        ("3.0.6", StarshipAirflow30),
        ("3.1.0", StarshipAirflow31),
        ("3.1.8", StarshipAirflow31),
        ("3.2.0", StarshipAirflow32),
        ("3.2.2", StarshipAirflow32),
        ("3.3.0", StarshipAirflow33),
        ("3.3.1", StarshipAirflow33),
    ],
)
def test_compatability_layer_dispatches_to_correct_subclass(airflow_version, expected_cls):
    instance = StarshipCompatabilityLayer(airflow_version=airflow_version)
    assert isinstance(instance, expected_cls)


@pytest.mark.parametrize("airflow_version", ["2.11.0", "3.4.0", "4.0.0"])
def test_compatability_layer_raises_for_unsupported_versions(airflow_version):
    with pytest.raises(RuntimeError, match="Unsupported Airflow Version"):
        StarshipCompatabilityLayer(airflow_version=airflow_version)


def test_starship_airflow32_exposes_team_name_read_only():
    for attrs_fn in (
        StarshipAirflow32.pool_attrs,
        StarshipAirflow32.variable_attrs,
        StarshipAirflow32.connection_attrs,
    ):
        team_name = attrs_fn()["team_name"]
        # `team_name` is a deployment-local FK to `team.name`; migrating it blindly
        # can fail with FK violations if the target hasn't created the same teams.
        assert team_name["methods"] == []


def test_starship_airflow32_exposes_new_dag_run_columns():
    dag_run_attrs = StarshipAirflow32.dag_run_attrs()
    for col in ("created_at", "partition_key", "partition_date"):
        assert col in dag_run_attrs, f"{col} missing from StarshipAirflow32.dag_run_attrs"


def test_starship_airflow33_exposes_new_task_instance_columns():
    task_instance_attrs = StarshipAirflow33.task_instance_attrs()
    for col in ("retry_delay_override", "retry_reason"):
        assert col in task_instance_attrs, f"{col} missing from StarshipAirflow33.task_instance_attrs"


def test_starship_airflow33_inherits_all_32_additions():
    # A future 3.3-only override that forgets `super()` would silently drop 3.2's schema deltas.
    for attrs_fn in (
        StarshipAirflow33.pool_attrs,
        StarshipAirflow33.variable_attrs,
        StarshipAirflow33.connection_attrs,
    ):
        assert "team_name" in attrs_fn()

    dag_run_attrs = StarshipAirflow33.dag_run_attrs()
    for col in ("created_at", "partition_key", "partition_date"):
        assert col in dag_run_attrs


def test_dag_runs_test_value_payload_carries_new_columns():
    # The list-endpoint payloads drive docker validation -- assert the row shape stays in sync
    # with dag_run_attrs so a stale payload can't silently pass validation.
    row_32 = StarshipAirflow32.dag_runs_attrs()["dag_runs"]["test_value"][0]
    for col in ("created_at", "partition_key", "partition_date"):
        assert col in row_32, f"{col} missing from StarshipAirflow32.dag_runs_attrs payload"

    row_33 = StarshipAirflow33.task_instances_attrs()["task_instances"]["test_value"][0]
    for col in ("retry_delay_override", "retry_reason"):
        assert col in row_33, f"{col} missing from StarshipAirflow33.task_instances_attrs payload"


def test_starship_airflow31_does_not_have_32_or_33_additions():
    # Guards against a future refactor moving a 3.2/3.3 delta into the base class by accident.
    assert "team_name" not in StarshipAirflow31.pool_attrs()
    assert "created_at" not in StarshipAirflow31.dag_run_attrs()
    assert "retry_delay_override" not in StarshipAirflow31.task_instance_attrs()
