import json
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests
from airflow.models import DagRun
from pytest_mock import MockerFixture
from requests import HTTPError

from astronomer.starship.services import remote_airflow_client
from astronomer.starship.services.local_airflow_client import (
    get_dag_runs_and_task_instances,
    receive_dag,
)
from mock_alchemy.mocking import UnifiedAlchemyMagicMock

from astronomer.starship.services.remote_airflow_client import get_extras

test_dag_id = "dag_0"
test_dag_run_id = "dag_0_run"
test_task_id = "operator_0"

example_dr = {
    "conf": {},
    "creating_job_id": None,
    "dag_hash": None,
    "dag_id": test_dag_id,
    "data_interval_end": None,
    "data_interval_start": None,
    "end_date": None,
    "execution_date": "1970-01-01 00:00:01",
    "external_trigger": True,
    "id": None,
    "last_scheduling_decision": None,
    "queued_at": None,
    "run_id": test_dag_run_id,
    "run_type": "manual",
    "start_date": None,
    "state": "queued",
    "table": "dag_run",
}
example_ti = {
    "task_id": test_task_id,
    "dag_id": test_dag_id,
    "run_id": test_dag_run_id,
    "table": "task_instance",
    "pool": "default_pool",
    "pool_slots": 1,
    "try_number": 1,
    "max_tries": 0,
    "hostname": "0150c3d6f607",
    "unixname": "astro",
    "job_id": 25,
    "queue": "default",
    "priority_weight": 1,
    "operator": "BashOperator",
    "executor_config": "{}",
    "external_executor_id": None,
    "trigger_id": None,
    "trigger_timeout": None,
    "next_method": None,
    "next_kwargs": None,
}


def example_dag_run(dag_id):
    return json.loads(
        """{
    "table": "dag_run", "id": 1, "dag_id": """
        + '"'
        + dag_id
        + '"'
        + """, "queued_at": "2023-07-07 18:26:34.500485+00:00",
    "execution_date": "2023-07-07 18:26:34.483055+00:00", "start_date": "2023-07-07 18:26:35.078907+00:00",
    "end_date": "2023-07-07 18:26:36.325558+00:00", "state": "success",
    "run_id": "manual__2023-07-07T18:26:34.483055+00:00", "creating_job_id": null, "external_trigger": true,
    "run_type": "manual", "conf": {}, "data_interval_start": "2023-07-07 18:26:34.483055+00:00",
    "data_interval_end": "2023-07-07 18:26:34.483055+00:00",
    "last_scheduling_decision": "2023-07-07 18:26:36.317202+00:00", "dag_hash": "6cb694964de67c6445230dc9da0f1721"
    }"""
    )


def example_task_instance(dag_id):
    return json.loads(
        """{
        "table": "task_instance", "task_id": "operator_0", "dag_id": """
        + '"'
        + dag_id
        + '"'
        + """,
        "run_id": "manual__2023-07-07T18:26:34.483055+00:00", "start_date": "2023-07-07 18:26:35.367559+00:00",
        "end_date": "2023-07-07 18:26:35.557389+00:00", "duration": 0.18983, "state": "success", "try_number": 2,
        "max_tries": 0, "hostname": "0150c3d6f607", "unixname": "astro", "job_id": 25, "pool": "default_pool",
        "pool_slots": 1, "queue": "default", "priority_weight": 1, "operator": "BashOperator",
        "queued_dttm": "2023-07-07 18:26:35.160600+00:00", "queued_by_job_id": 24, "pid": 5217, "executor_config": {},
        "external_executor_id": null, "trigger_id": null, "trigger_timeout": null,
        "next_method": null, "next_kwargs": null
        }"""
    )


@pytest.mark.integration_test
def test_get_dag_runs_and_task_instances(mocker: MockerFixture):
    class MockColumn:
        def __init__(self, name):
            self.name = name

    class MockTable:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def __str__(self):
            return getattr(self, "name")

    mock_dag_run = DagRun(dag_id="dag_id")
    mock_ti = MagicMock()
    mock_ti.task_id = test_task_id
    mock_ti.run_id = test_dag_run_id
    mock_ti.dag_id = test_dag_id
    mock_ti.__table__ = MockTable(
        name="task_instance",
        columns=[MockColumn("task_id"), MockColumn("dag_id"), MockColumn("run_id")],
    )
    mock_dag_run.task_instances = [mock_ti]

    mock_session = UnifiedAlchemyMagicMock(
        data=[
            (
                [
                    mock.call.query(DagRun),
                    mock.call.filter(DagRun.dag_id == test_dag_id),
                ],
                [mock_dag_run],
            )
        ]
    )

    actual = get_dag_runs_and_task_instances(session=mock_session, dag_id=test_dag_id)
    expected = [example_dr, example_ti]
    assert actual == expected


@pytest.mark.skip
def test_receive_dag():
    session = UnifiedAlchemyMagicMock()
    test_data = [example_dr, example_ti]
    receive_dag(session=session, data=test_data)
    # TODO - figure out how to test this correctly
    session.assert_called()


# @manual_tests  # requires a running `astro dev start` with dags
@pytest.mark.integration_test
def test_receive_dag_integration():
    deployment_url = "http://localhost:8080"
    deployment_token = ""
    dag_id = "dag_0"

    # GIVEN
    # DAG History, and a blank target slate
    remote_airflow_client.delete_dag(deployment_url, deployment_token, dag_id)
    keep_going = True
    timeout = datetime.now() + timedelta(minutes=1)
    actual_deleted_dag = None
    while keep_going and datetime.now() <= timeout:
        try:
            actual_deleted_dag = remote_airflow_client.get_dag(
                dag_id, deployment_url, deployment_token, skip_cache=True
            )
            keep_going = False
        except HTTPError:
            pass
    assert (
        actual_deleted_dag is not None
    ), "we deleted the DAG and are starting with a fresh slate"

    # WHEN MIGRATE - Note: this MUST target a locally running webserver
    actual_migrate_dag_result = requests.post(
        f"{deployment_url}/astromigration/dag_history/receive",
        data=json.dumps([example_dr, example_ti], default=str),
        **get_extras(deployment_url, ""),
    )
    print(actual_migrate_dag_result.content)
    assert actual_migrate_dag_result.ok, "We can 'migrate' a DAG to our local instance"

    # THEN
    # WHEN GET DAG RUNS
    actual_dag_runs = remote_airflow_client.get_dag_runs(
        dag_id, deployment_url, deployment_token
    )
    assert actual_dag_runs == {
        "dag_runs": [
            {
                "conf": {},
                "dag_id": "dag_0",
                "dag_run_id": "dag_0_run",
                "end_date": None,
                "execution_date": "1970-01-01T00:00:01+00:00",
                "external_trigger": True,
                "logical_date": "1970-01-01T00:00:01+00:00",
                "start_date": None,
                "state": "queued",
            }
        ],
        "total_entries": 1,
    }

    # WHEN GET TASK INSTANCES
    actual_task_instances = remote_airflow_client.get_task_instances(
        dag_id, test_dag_run_id, deployment_url, deployment_token
    )
    assert actual_task_instances == [
        {
            "dag_id": "dag_0",
            "duration": None,
            "end_date": None,
            "execution_date": "1970-01-01T00:00:01+00:00",
            "executor_config": "{}",
            "hostname": "0150c3d6f607",
            "max_tries": 0,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": None,
            "sla_miss": None,
            "start_date": None,
            "state": None,
            "task_id": "operator_0",
            "try_number": 1,
            "unixname": "astro",
        }
    ]

    # CLEANUP
    remote_airflow_client.delete_dag(deployment_url, deployment_token, dag_id)
