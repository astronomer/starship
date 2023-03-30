import os
from typing import Optional
from urllib.parse import urlparse

import jwt
import requests
from cachetools.func import ttl_cache
from pydash import at
from python_graphql_client import GraphqlClient
from deprecated import deprecated

ASTRO_AUTH = "https://auth.astronomer.io"
ASTROHUB_API = "https://api.astronomer.io/hub/v1"
ASTRO_ALPHA_API = "https://api.astronomer.io/v1alpha1"


def get_username(token):
    headers = {"Authorization": f"Bearer {token}"}
    client = GraphqlClient(
        endpoint=ASTROHUB_API, headers=headers
    )
    query = "{self {user {username}}}"

    try:
        api_rv = at(client.execute(query), "data.self.user.username")[0]
        return api_rv
    except Exception as e:
        print(e)
        return None


@ttl_cache(ttl=1)
def get_deployments(token):
    if not token:
        return {}
    headers = {"Authorization": f"Bearer {token}"}
    orgs = get_organizations(token)
    short_name = os.getenv("STARSHIP_ORG_SHORTNAME", [_ for _ in orgs.values()][0]['shortName'])

    # FIXME use the first org for now. change to a select in the near term.
    url = f"{ASTRO_ALPHA_API}/organizations/{short_name}/deployments"

    try:
        api_rv = requests.get(url, headers=headers).json()["deployments"]

        return {deploy["id"]: deploy for deploy in (api_rv or [])}
    except Exception as exc:
        print(exc)
        return {}


@ttl_cache(ttl=1)
def get_organizations(token):
    if not token:
        return {}

    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ASTRO_ALPHA_API}/organizations"

    try:
        api_rv = requests.get(url, headers=headers).json()

        return {org["id"]: org for org in (api_rv or [])}
    except Exception as exc:
        print(exc)
        return {}


@ttl_cache(ttl=3600)
def get_jwk(token: str):
    jwks_url = f"{ASTRO_AUTH}/.well-known/jwks.json"
    jwks_client = jwt.PyJWKClient(jwks_url)
    return jwks_client.get_signing_key_from_jwt(token)


def get_deployment_url(deployment, token):
    all_astro_deployments = get_deployments(token)

    if all_astro_deployments and all_astro_deployments.get(deployment):
        url = urlparse(
            all_astro_deployments[deployment]["webServerUrl"]
        )
        return f"https:/{url.netloc}/{url.path}"


def set_environment_variables(deployment, remote_vars, token):
    headers = {"Authorization": f"Bearer {token}"}
    client = GraphqlClient(
        endpoint=ASTROHUB_API, headers=headers
    )
    query = """
            fragment EnvironmentVariable on EnvironmentVariable {
                key
                value
                isSecret
                updatedAt
            }
            mutation deploymentVariablesUpdate($input: EnvironmentVariablesInput!) {
                deploymentVariablesUpdate(input: $input) {
                    ...EnvironmentVariable
                }
            }
            """
    client.execute(
        query,
        {
            "input": {
                "deploymentId": deployment,
                "environmentVariables": list(remote_vars.values()),
            }
        },
    )


@deprecated
class AstroClient:
    def __init__(
            self, token: Optional[str] = None, url: str = ASTROHUB_API
    ):
        self.token = token
        self.url = url

    def list_deployments(self):
        headers = {"Authorization": f"Bearer {self.token}"}
        from python_graphql_client import GraphqlClient

        client = GraphqlClient(endpoint=self.url, headers=headers)
        query = """
                    {
                     deployments
                     {
                         id,
                         label,
                         releaseName,
                         workspace
                         {
                             id,
                             label
                         },
                         deploymentShortId,
                         deploymentSpec
                         {
                             environmentVariables
                             webserver {
                                 ingressHostname,
                                 url
                             }
                         }
                     }
                    }
                    """

        return client.execute(query).get("data")
