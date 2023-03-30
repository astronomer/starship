import os
from pathlib import Path

import os
from pathlib import Path

import pytest
import yaml
from dotenv import load_dotenv
from jwt import PyJWKClientError, PyJWK
from services.astro_client import get_username, get_deployments, get_organizations, get_jwk, get_deployment_url, \
    set_environment_variables

E2E_DEPLOYMENT_ID = "clfvnxzq9812004i1e3fjimis4"


@pytest.fixture
def workspace_token() -> str:
    load_dotenv()
    token = os.getenv("ASTRO_WORKSPACE_TOKEN")
    if not token:
        assert False, "ASTRO_WORKSPACE_TOKEN not found in .env or env"
    return token


@pytest.fixture
def user_token() -> str:
    with open(Path.home() / ".astro/config.yaml") as f:
        _, token = yaml.safe_load(f)['contexts']["astronomer_io"]["token"].split("Bearer ")
    return token


@pytest.mark.skipif(condition=not os.getenv("MANUAL_TESTS", False),
                    reason="requires a real user, who ran `astro login` recently")
def test_get_username(user_token):
    actual = get_username(user_token)
    expected = '@astronomer.io'
    assert expected in actual, \
        "We have an @astronomer.io email"


@pytest.mark.integration_test
def test_get_deployments(workspace_token):
    actual = get_deployments(workspace_token)
    expected = E2E_DEPLOYMENT_ID
    assert expected in actual, \
        "our e2e deployment is able to be listed"


@pytest.mark.integration_test
def test_get_organizations(workspace_token):
    actual = get_organizations(workspace_token)
    expected = 'cknaqyipv05731evsry6cj4n0'
    assert expected in actual, \
        "We can get the 'Astronomer' Organization (where our e2e deployment/workspace is)"


@pytest.mark.skipif(condition=not os.getenv("MANUAL_TESTS", False),
                    reason="requires a real user, who ran `astro login` recently")
def test_get_jwk(user_token, workspace_token):
    with pytest.raises(PyJWKClientError, match="Unable to find a signing key"):
        get_jwk(workspace_token)

    actual = get_jwk(user_token)
    expected = PyJWK
    assert type(actual) == expected


@pytest.mark.integration_test
def test_get_deployment_url(workspace_token):
    actual = get_deployment_url(E2E_DEPLOYMENT_ID, workspace_token)
    expected = "https://astronomer.astronomer.run/dfjimis4"
    assert actual == expected


@pytest.mark.integration_test
def test_set_environment_variables(workspace_token):
    set_environment_variables(
        E2E_DEPLOYMENT_ID,
        {"key": {"key": "key", "value": "value", "isSecret": False}},
        workspace_token
    )
    assert True, \
        "we can set variables"

