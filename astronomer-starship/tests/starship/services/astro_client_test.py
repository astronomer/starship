import os

import pytest
from jwt import PyJWKClientError, PyJWK

from starship.services.astro_client import \
    get_username, get_deployments, get_organizations, get_jwk, \
    get_deployment_url, set_environment_variables


@pytest.mark.skipif(condition=not os.getenv("MANUAL_TESTS", False),
                    reason="requires a real user, who ran `astro login` recently")
def test_get_username(user_token):
    actual = get_username(user_token)
    expected = '@astronomer.io'
    assert expected in actual, \
        "We have an @astronomer.io email"


@pytest.mark.integration_test
def test_get_deployments(e2e_workspace_token, e2e_deployment_id):
    actual = get_deployments(e2e_workspace_token)
    expected = e2e_deployment_id
    assert expected in actual, \
        "our e2e deployment is able to be listed"


@pytest.mark.integration_test
def test_get_organizations(e2e_workspace_token):
    actual = get_organizations(e2e_workspace_token)
    expected = 'cknaqyipv05731evsry6cj4n0'
    assert expected in actual, \
        "We can get the 'Astronomer' Organization (where our e2e deployment/workspace is)"


@pytest.mark.skipif(condition=not os.getenv("MANUAL_TESTS", False),
                    reason="requires a real user, who ran `astro login` recently")
def test_get_jwk(user_token, e2e_workspace_token):
    with pytest.raises(PyJWKClientError, match="Unable to find a signing key"):
        get_jwk(e2e_workspace_token)

    actual = get_jwk(user_token)
    expected = PyJWK
    assert type(actual) == expected, "we get a JWK-ish thing back"


@pytest.mark.integration_test
def test_get_deployment_url(e2e_workspace_token, e2e_deployment_id):
    actual = get_deployment_url(e2e_deployment_id, e2e_workspace_token)
    expected = "https://astronomer.astronomer.run/dfjimis4"
    assert actual == expected, "we get our deployment url back"


@pytest.mark.integration_test
def test_set_environment_variables(e2e_workspace_token, e2e_deployment_id):
    set_environment_variables(
        e2e_deployment_id,
        {"key": {"key": "key", "value": "value", "isSecret": False}},
        e2e_workspace_token
    )
    assert True, \
        "we can set variables"
