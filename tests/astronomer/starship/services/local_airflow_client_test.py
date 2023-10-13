import json
from datetime import datetime, timedelta
from time import sleep
from unittest import mock
import uuid
import pendulum
import pytest
import requests
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from pytest_mock import MockerFixture
from requests import HTTPError

from astronomer_starship.starship.services import remote_airflow_client
from astronomer_starship.starship.services.local_airflow_client import (
    get_dag_runs_and_task_instances,
    receive_dag,
)
from mock_alchemy.mocking import UnifiedAlchemyMagicMock

from astronomer_starship.starship.services.remote_airflow_client import get_extras
from tests.conftest import manual_tests

test_dag_id = "dag_1"
test_dag_run_id = f"dag_1_run_{uuid.uuid4()}"
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

    mock_dag_run = DagRun(
        dag_id=test_dag_id,
        run_id=test_dag_run_id,
        execution_date=pendulum.parse("1970-01-01 00:00:01"),
        queued_at=pendulum.parse("2023-07-19 04:57:22.569681+00:00"),
        external_trigger=True,
        run_type="manual",
        state=DagRunState.QUEUED,
    )
    # set up the TI from our sample one
    mock_ti = mocker.MagicMock()
    for k, v in example_ti.items():
        setattr(mock_ti, k, v)

    mock_ti.__table__ = MockTable(
        name="task_instance",
        columns=[MockColumn(k) for k in example_ti.keys()],
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
    for i in range(len(actual)):
        for k, v in actual[i].items():
            # queued_at seemed to be datetime.now, not sure why
            # execution date mostly matched, but parsed slightly different
            if k not in ["queued_at", "execution_date"]:
                assert str(v) == str(expected[i][k]), f'Key "{k}"'


@pytest.mark.skip
def test_receive_dag():
    session = UnifiedAlchemyMagicMock()
    test_data = [example_dr, example_ti]
    receive_dag(session=session, data=test_data)
    # TODO - figure out how to test this correctly
    session.assert_called()


@manual_tests  # requires a running `astro dev start` with dags
@pytest.mark.integration_test
def test_receive_dag_integration():
    deployment_url = "http://localhost:8080"
    deployment_token = ""
    dag_id = test_dag_id

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
            sleep(1)
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
                "dag_id": test_dag_id,
                "dag_run_id": test_dag_run_id,
                "data_interval_end": None,
                "data_interval_start": None,
                "end_date": None,
                "execution_date": "1970-01-01T00:00:01+00:00",
                "external_trigger": True,
                "last_scheduling_decision": None,
                "logical_date": "1970-01-01T00:00:01+00:00",
                "note": None,
                "run_type": "manual",
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
            "dag_id": test_dag_id,
            "dag_run_id": test_dag_run_id,
            "duration": None,
            "end_date": None,
            "execution_date": "1970-01-01T00:00:01+00:00",
            "executor_config": "{}",
            "hostname": "0150c3d6f607",
            "map_index": -1,
            "max_tries": 0,
            "note": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": None,
            "rendered_fields": {},
            "sla_miss": None,
            "start_date": None,
            "state": None,
            "task_id": "operator_0",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 1,
            "unixname": "astro",
        }
    ]

    # CLEANUP
    remote_airflow_client.delete_dag(deployment_url, deployment_token, dag_id)
