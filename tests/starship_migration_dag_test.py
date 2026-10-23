"""Tests for building ``StarshipAirflowMigrationDAG`` with DAG history options."""

import pytest

from astronomer_starship.providers.starship.operators.starship import StarshipAirflowMigrationDAG


@pytest.fixture(autouse=True)
def _connections(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
    monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_DEFAULT", "http://target.example.com/")


@pytest.mark.parametrize(
    "dag_history_kwargs",
    [{"dag_run_limit": 50}, {"unpause_dag_in_target": True}],
)
def test_dag_history_kwargs_are_accepted(dag_history_kwargs):
    StarshipAirflowMigrationDAG(target_http_conn_id="starship_default", **dag_history_kwargs)


def test_dag_history_kwargs_reach_dag_history_operator():
    dag = StarshipAirflowMigrationDAG(
        target_http_conn_id="starship_default",
        dag_run_limit=50,
        unpause_dag_in_target=True,
    )

    partial_kwargs = dag.task_dict["dag_history.migrate_dag_ids"].partial_kwargs
    assert partial_kwargs["dag_run_limit"] == 50
    assert partial_kwargs["unpause_dag_in_target"] is True


def test_dag_history_kwargs_default_to_operator_defaults():
    dag = StarshipAirflowMigrationDAG(target_http_conn_id="starship_default")

    partial_kwargs = dag.task_dict["dag_history.migrate_dag_ids"].partial_kwargs
    assert partial_kwargs["dag_run_limit"] == 10
    assert partial_kwargs["unpause_dag_in_target"] is False
