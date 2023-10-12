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
def test_get_deployments(e2e_workspace_token, e2e_deployment_id):
    actual = get_deployments(e2e_workspace_token)
    expected = e2e_deployment_id
    assert expected in actual, "our e2e deployment is able to be listed"


@pytest.mark.integration_test
def test_get_deployment_url(e2e_workspace_token, e2e_deployment_id):
    actual = get_deployment_url(e2e_deployment_id, e2e_workspace_token)
    expected = "https://astronomer.astronomer.run/dfjimis4"
    assert actual == expected, "we get our deployment url back"


@pytest.mark.integration_test
def test_get_organizations(e2e_workspace_token):
    actual = get_organizations(e2e_workspace_token)
    expected = "cknaqyipv05731evsry6cj4n0"
    assert (
        expected in actual
    ), "We can get the 'Astronomer' Organization (where our e2e deployment/workspace is)"


@manual_tests  # requires a real user, who ran `astro login` recently
@pytest.mark.slow_integration_test
def test_get_jwk_key(user_token, e2e_workspace_token):
    actual = get_jwk_key(user_token)
    expected = _RSAPublicKey
    assert type(actual) == expected, "we get a key-ish thing back"


@pytest.mark.integration_test
def test_set_and_get_environment_variables(e2e_workspace_token, e2e_deployment_id):
    test_variables = {"key": {"key": "key", "value": "value", "isSecret": False}}

    set_changed_environment_variables(
        e2e_deployment_id, e2e_workspace_token, test_variables
    )
    assert True, "we can set variables"

    # actual = get_environment_variables(
    #     "clfu1tp85433431i0ihihlimyr",
    #     "MANUAL TOKEN TO TEST NEWER API VERSION"
    # )
    # expected = {'FOO': {'isSecret': False, 'key': 'FOO', 'value': 'bar'}}
    # assert actual == expected

    actual = get_environment_variables(e2e_deployment_id, e2e_workspace_token)
    expected = test_variables
    assert actual == expected, "and get them back out"
