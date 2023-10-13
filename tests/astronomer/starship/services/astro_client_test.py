import pytest
from cryptography.hazmat.backends.openssl.rsa import _RSAPublicKey

from astronomer_starship.starship.services.astro_client import (
    get_deployment_url,
    get_deployments,
    get_environment_variables,
    get_jwk_key,
    get_organizations,
    get_username,
    set_changed_environment_variables,
)
from tests.conftest import manual_tests


@manual_tests  # requires a real user, who ran `astro login` recently
@pytest.mark.slow_integration_test
def test_get_username(user_token):
    actual = get_username(user_token)
    expected = "@astronomer.io"
    assert expected in actual, "We have an @astronomer.io email"


@pytest.mark.integration_test
def test_get_deployments(e2e_token_deployment_workspace_org_url):
    [
        e2e_workspace_token,
        e2e_deployment_id,
        _,
        _,
        _,
    ] = e2e_token_deployment_workspace_org_url
    actual = get_deployments(e2e_workspace_token)
    expected = e2e_deployment_id
    assert expected in actual, "our e2e deployment is able to be listed"


@pytest.mark.integration_test
def test_get_deployment_url(e2e_token_deployment_workspace_org_url):
    [
        e2e_workspace_token,
        e2e_deployment_id,
        _,
        _,
        expected,
    ] = e2e_token_deployment_workspace_org_url
    actual = get_deployment_url(e2e_deployment_id, e2e_workspace_token)
    assert actual == expected, "we get our deployment url back"


@pytest.mark.integration_test
def test_get_organizations(e2e_token_deployment_workspace_org_url):
    [
        e2e_workspace_token,
        _,
        _,
        expected_org_id,
        _,
    ] = e2e_token_deployment_workspace_org_url
    actual = get_organizations(e2e_workspace_token)
    assert (
        expected_org_id in actual
    ), "We can get the expected org where our e2e deployment/workspace is"


@manual_tests  # requires a real user, who ran `astro login` recently
@pytest.mark.slow_integration_test
def test_get_jwk_key(user_token, e2e_token_deployment_workspace_org_url):
    [
        e2e_workspace_token,
        e2e_deployment_id,
        _,
        _,
        _,
    ] = e2e_token_deployment_workspace_org_url
    actual = get_jwk_key(user_token)
    expected = _RSAPublicKey
    assert type(actual) == expected, "we get a key-ish thing back"


@pytest.mark.integration_test
def test_set_and_get_environment_variables(
    mocker, e2e_token_deployment_workspace_org_url
):
    [
        e2e_workspace_token,
        e2e_deployment_id,
        _,
        _,
        _,
    ] = e2e_token_deployment_workspace_org_url
    test_variables = {"KEY": {"key": "KEY", "value": "value", "isSecret": False}}

    mocker.patch.dict(
        "os.environ", {test_variables["KEY"]["key"]: test_variables["KEY"]["value"]}
    )

    set_changed_environment_variables(
        e2e_deployment_id,
        e2e_workspace_token,
        iter(
            test_variables["KEY"]["key"],
        ),
    )
    assert True, "we can set variables"

    actual = get_environment_variables(e2e_deployment_id, e2e_workspace_token)
    expected = test_variables
    expected["AIRFLOW__CORE__TEST_CONNECTION"] = {
        "isSecret": False,
        "key": "AIRFLOW__CORE__TEST_CONNECTION",
        "value": "Enabled",
    }
    assert actual == expected, "and get them back out"
