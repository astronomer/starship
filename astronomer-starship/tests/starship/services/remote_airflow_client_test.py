import logging

import pytest
from airflow.models import Pool, Variable
from requests import HTTPError

from starship.services.remote_airflow_client import get_connections, get_pools, create_connection, get_variables, \
    create_variable, get_config, set_dag_is_paused, get_dag, do_test_connection, create_pool, delete_connection, \
    delete_pool, is_pool_migrated, delete_variable, is_variable_migrated


@pytest.mark.integration
def test_create_and_get_connections(e2e_deployment_url, e2e_deployment_id, e2e_workspace_token, e2e_api_token):
    from airflow.models import Connection

    # GIVEN
    test_conn = Connection(
        conn_id="foo",
        conn_type="aws",
        description="e2e test",
    )

    try:
        delete_connection(
            e2e_deployment_url,
            e2e_api_token,
            test_conn
        )
    except HTTPError as e:
        logging.exception(e)

    # WHEN CREATE
    actual = create_connection(
        e2e_deployment_url,
        e2e_api_token,
        test_conn
    )
    # THEN
    assert actual.ok, 'we created the connection'

    # WHEN GET
    actual = get_connections(e2e_deployment_url, e2e_api_token)
    expected = 1
    # THEN
    assert len(actual) >= expected

    # WHEN TEST
    actual = do_test_connection(e2e_deployment_url, e2e_api_token, test_conn)
    # THEN
    assert actual.ok, 'we tested the connection'

    # CLEANUP
    delete_connection(
        e2e_deployment_url,
        e2e_api_token,
        test_conn
    )


@pytest.mark.integration
def test_create_and_get_pools(e2e_deployment_url, e2e_deployment_id, e2e_workspace_token, e2e_api_token):
    # GIVEN

    test_pool = Pool(
        pool="test",
        slots=1,
        description="e2e test"
    )

    try:
        delete_pool(
            e2e_deployment_url,
            e2e_api_token,
            test_pool
        )
    except HTTPError as e:
        logging.exception(e)

    # WHEN CREATE
    actual = create_pool(e2e_deployment_url, e2e_api_token, test_pool)
    assert actual.ok, 'we created the pool'

    # WHEN GET
    actual = get_pools(e2e_deployment_url, e2e_api_token)
    expected = 2
    assert len(actual) >= expected

    # WHEN IS_POOL_MIGRATED
    actual = is_pool_migrated(e2e_deployment_url, e2e_api_token, "test")
    expected = True
    assert actual == expected

    # CLEANUP
    delete_pool(
        e2e_deployment_url,
        e2e_api_token,
        test_pool
    )


@pytest.mark.integration
def test_create_and_get_variables(e2e_deployment_url, e2e_deployment_id, e2e_workspace_token, e2e_api_token):
    # GIVEN
    test_variable = Variable(key="foo", val="bar", description="e2e test")
    try:
        delete_variable(
            e2e_deployment_url,
            e2e_api_token,
            test_variable
        )
    except HTTPError as e:
        logging.exception(e)

    # WHEN CREATE
    actual = create_variable(e2e_deployment_url, e2e_api_token, test_variable)
    assert actual.ok, "we created the variable"

    # WHEN GET
    actual = get_variables(e2e_deployment_url, e2e_api_token)
    expected = 1
    assert len(actual) >= expected

    # WHEN IS_POOL_MIGRATED
    actual = is_variable_migrated(e2e_deployment_url, e2e_api_token, "foo")
    expected = True
    assert actual == expected

    # CLEANUP
    delete_variable(
        e2e_deployment_url,
        e2e_api_token,
        test_variable
    )


@pytest.mark.integration
def test_get_config(e2e_deployment_url, e2e_deployment_id, e2e_workspace_token, e2e_api_token):
    with pytest.raises(HTTPError):
        get_config(e2e_deployment_url, e2e_api_token)


@pytest.mark.integration
def test_set_dag_is_paused(e2e_deployment_url, e2e_deployment_id, e2e_workspace_token, e2e_api_token):
    actual = set_dag_is_paused("astronomer_monitoring_dag", True, e2e_deployment_url, e2e_api_token)
    assert actual.ok


@pytest.mark.integration
def test_get_dag(e2e_deployment_url, e2e_deployment_id, e2e_workspace_token, e2e_api_token):
    actual = get_dag("astronomer_monitoring_dag", e2e_deployment_url, e2e_api_token)
    assert actual.json()['dag_id'] == "astronomer_monitoring_dag"
    assert not actual.json()['is_paused']
