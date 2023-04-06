import os
from pathlib import Path

import pytest
import requests
import yaml
from dotenv import load_dotenv

from starship.services.astro_client import ASTRO_AUTH, get_deployment_url


@pytest.fixture
def e2e_deployment_url(e2e_deployment_id, e2e_workspace_token):
    return get_deployment_url(e2e_deployment_id, e2e_workspace_token)


@pytest.fixture
def e2e_deployment_id() -> str:
    """The 'e2e' deployment in the 'e2e' workspace in the 'Astronomer' org"""
    return "clfvnxzq9812004i1e3fjimis4"


@pytest.fixture
def e2e_api_token() -> str:
    load_dotenv()
    astro_id = os.getenv("ASTRONOMER_KEY_ID")
    astro_key = os.getenv("ASTRONOMER_KEY_SECRET")
    r = requests.post(
        f"{ASTRO_AUTH}/oauth/token",
        json={
            "client_id": astro_id,
            "client_secret": astro_key,
            "audience": "astronomer-ee",
            "grant_type": "client_credentials"
        }
    )
    r.raise_for_status()
    return r.json()['access_token']


@pytest.fixture
def e2e_workspace_token() -> str:
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


def pytest_addoption(parser):
    parser.addoption("--package", action="store")


@pytest.fixture(scope="session")
def package(request):
    package_value = request.config.option.package
    if package_value is None:
        pytest.skip()
    return package_value
