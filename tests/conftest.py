import os
from pathlib import Path

import pytest
import yaml

from astronomer_starship.starship.services.astro_client import (
    get_deployment_url,
)

manual_tests = pytest.mark.skipif(
    not bool(os.getenv("MANUAL_TESTS")), reason="requires env setup"
)


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture
def e2e_deployment_url(e2e_token_deployment_workspace_org):
    [
        e2e_workspace_token,
        e2e_deployment_id,
        _,
        _,
        _,
    ] = e2e_token_deployment_workspace_org
    return get_deployment_url(e2e_deployment_id, e2e_workspace_token)


@pytest.fixture(
    params=[
        (
            "HOSTED_WORKSPACE_TOKEN",
            "clnnlkp231365078bcy6h44b7i9u",
            "clnnlio6d000l01nxfj0ezh45",
            "clkvh3b46003m01kbalgwwdcy",
            "https://clkvh3b46003m01kbalgwwdcy.astronomer.run/d44b7i9u",
        ),
        (
            "HYBRID_WORKSPACE_TOKEN",
            "clnnlk3w71321430awzwdxgsumio",  # e2e
            "cl656scdl140281h0j3qvfs4e8",  # customer success engineering
            "cknaqyipv05731evsry6cj4n0",
            "https://astronomer.astronomer.run/dxgsumio",
        ),
    ],
    ids=["hosted", "hybrid"],
)
def e2e_token_deployment_workspace_org(request):
    (
        workspace_token_env_key,
        deployment_id,
        workspace_id,
        organization_id,
        url,
    ) = request.param
    pytest.importorskip("dotenv")
    from dotenv import load_dotenv

    load_dotenv()
    workspace_token = os.getenv(workspace_token_env_key)
    return workspace_token, deployment_id, workspace_id, organization_id, url


@pytest.fixture
def user_token() -> str:
    with open(Path.home() / ".astro/config.yaml") as f:
        _, token = yaml.safe_load(f)["contexts"]["astronomer_io"]["token"].split(
            "Bearer "
        )
    return token
