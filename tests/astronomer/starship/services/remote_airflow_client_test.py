import logging
from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from requests import HTTPError

from astronomer_starship.starship.services import remote_airflow_client
from astronomer_starship.starship.services.remote_airflow_client import (
    create_connection,
    create_pool,
    create_variable,
    delete_connection,
    delete_pool,
    delete_variable,
    do_test_connection,
    get_connections,
    get_dag,
    get_pools,
    get_variables,
    is_pool_migrated,
    is_variable_migrated,
    set_dag_is_paused,
)


@pytest.mark.integration_test
def test_create_and_get_connections(
    e2e_deployment_url, e2e_token_deployment_workspace_org
):
    [e2e_api_token, _, _, _, _] = e2e_token_deployment_workspace_org
    from airflow.models import Connection

    # GIVEN
    test_conn = Connection(
        conn_id="foo",
        conn_type="aws",
        description="e2e test",
    )

    try:
        # Try to delete, just incase we didn't cleanup last time
        delete_connection(e2e_deployment_url, e2e_api_token, test_conn)
    except HTTPError as e:
        logging.exception(e)

    # WHEN CREATE
    actual = create_connection(e2e_deployment_url, e2e_api_token, test_conn)
    # THEN
    assert actual, "we created the connection"

    # WHEN GET
    actual = get_connections(e2e_deployment_url, e2e_api_token)
    expected = 1
    # THEN
    assert len(actual) >= expected

    # WHEN TEST
    actual = do_test_connection(e2e_deployment_url, e2e_api_token, test_conn)
    # THEN
    assert actual, "we tested the connection"

    # CLEANUP
    delete_connection(e2e_deployment_url, e2e_api_token, test_conn)


@pytest.mark.integration_test
def test_create_and_get_pools(e2e_deployment_url, e2e_token_deployment_workspace_org):
    [e2e_api_token, _, _, _, _] = e2e_token_deployment_workspace_org

    # GIVEN
    from airflow.models import Pool

    test_pool = Pool(pool="test", slots=1, description="e2e test")

    try:
        delete_pool(e2e_deployment_url, e2e_api_token, test_pool)
    except HTTPError as e:
        logging.exception(e)

    # WHEN CREATE
    actual = create_pool(e2e_deployment_url, e2e_api_token, test_pool)
    assert actual, "we created the pool"

    # WHEN GET
    actual = get_pools(e2e_deployment_url, e2e_api_token)
    expected = 2
    assert len(actual) >= expected

    # WHEN IS_POOL_MIGRATED
    actual = is_pool_migrated(e2e_deployment_url, e2e_api_token, "test")
    expected = True
    assert actual == expected

    # CLEANUP
    delete_pool(e2e_deployment_url, e2e_api_token, test_pool)


@pytest.mark.integration_test
def test_create_and_get_variables(
    e2e_deployment_url, e2e_token_deployment_workspace_org
):
    [e2e_api_token, _, _, _, _] = e2e_token_deployment_workspace_org

    # GIVEN
    from airflow.models import Variable

    test_variable = Variable(key="foo", val="bar", description="e2e test")
    try:
        delete_variable(e2e_deployment_url, e2e_api_token, test_variable)
    except HTTPError as e:
        logging.exception(e)

    # WHEN CREATE
    actual = create_variable(e2e_deployment_url, e2e_api_token, test_variable)
    assert actual, "we created the variable"

    # WHEN GET
    actual = get_variables(e2e_deployment_url, e2e_api_token)
    expected = 1
    assert len(actual) >= expected

    # WHEN IS_POOL_MIGRATED
    actual = is_variable_migrated(e2e_deployment_url, e2e_api_token, "foo")
    expected = True
    assert actual == expected

    # CLEANUP
    delete_variable(e2e_deployment_url, e2e_api_token, test_variable)


@pytest.mark.skip("Can't pause the monitoring DAG anymore, no other DAGs on e2e")
@pytest.mark.integration_test
def test_set_dag_is_paused(e2e_deployment_url, e2e_token_deployment_workspace_org):
    [e2e_api_token, _, _, _, _] = e2e_token_deployment_workspace_org
    actual = set_dag_is_paused(
        "astronomer_monitoring_dag", True, e2e_deployment_url, e2e_api_token
    )
    assert actual


@pytest.mark.integration_test
def test_get_dag(e2e_deployment_url, e2e_token_deployment_workspace_org):
    [e2e_api_token, _, _, _, _] = e2e_token_deployment_workspace_org
    actual = get_dag("astronomer_monitoring_dag", e2e_deployment_url, e2e_api_token)
    assert actual["dag_id"] == "astronomer_monitoring_dag"


# noinspection PyUnresolvedReferences
def test_get_dag_is_cached_mock(
    mocker: MockerFixture,
):
    mocker.patch(
        "astronomer_starship.starship.services.remote_airflow_client._get_remote_dags"
    )
    mocker.patch(
        "astronomer_starship.starship.services.remote_airflow_client._get_remote_dag"
    )
    mock_dags_response = mocker.MagicMock()
    mock_dags_response.json.return_value = {
        "dags": [{"dag_id": "astronomer_monitoring_dag", "is_paused": False}]
    }
    remote_airflow_client._get_remote_dags.return_value = mock_dags_response

    mock_dag_response = mocker.MagicMock()
    mock_dag_response.json.return_value = {
        "dag_id": "astronomer_monitoring_dag",
        "is_paused": False,
    }
    remote_airflow_client._get_remote_dag.return_value = mock_dag_response

    actual = get_dag("astronomer_monitoring_dag", "", "")
    assert actual["dag_id"] == "astronomer_monitoring_dag"
    assert not actual["is_paused"]
    remote_airflow_client._get_remote_dags.assert_called_once()
    remote_airflow_client._get_remote_dag.assert_not_called()
    mocker.resetall()

    # We don't re-fetch the same DAG
    actual = get_dag("astronomer_monitoring_dag", "", "")
    assert actual is not None
    remote_airflow_client._get_remote_dags.assert_not_called()
    remote_airflow_client._get_remote_dag.assert_not_called()
    mocker.resetall()

    # We still don't re-fetch other DAGs either
    get_dag("other_dag", "", "")
    remote_airflow_client._get_remote_dags.assert_not_called()
    remote_airflow_client._get_remote_dag.assert_not_called()
    mocker.resetall()

    # We can set a different TTL to force the cache to expire
    actual = get_dag(
        "astronomer_monitoring_dag",
        "",
        "",
        ttl=timedelta(seconds=0),
    )
    assert actual["dag_id"] == "astronomer_monitoring_dag"
    remote_airflow_client._get_remote_dags.assert_called_once()
    remote_airflow_client._get_remote_dag.assert_not_called()
    mocker.resetall()

    # We can also directly skip the cache
    # (useful when we _know_ something updated and want the server to give us it's copy)
    actual = get_dag("astronomer_monitoring_dag", "", "", skip_cache=True)
    assert actual["dag_id"] == "astronomer_monitoring_dag"
    remote_airflow_client._get_remote_dags.assert_not_called()
    remote_airflow_client._get_remote_dag.assert_called_once()
    mocker.resetall()
