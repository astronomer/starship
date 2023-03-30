import os
from typing import Optional
from urllib.parse import urlparse

import jwt
import requests
from cachetools.func import ttl_cache
from flask import session
from pydash import at
from python_graphql_client import GraphqlClient
from deprecated import deprecated


def get_username(token):
    if not token:
        pass

    headers = {"Authorization": f"Bearer {token}"}
    client = GraphqlClient(
        endpoint="https://api.astronomer.io/hub/v1", headers=headers
    )

    query = "{self {user {username}}}"

    try:
        api_rv = at(client.execute(query), "data.self.user.username")[0]

        return api_rv
    except Exception as exc:
        print(exc)
        return None


@ttl_cache(ttl=1)
def get_deployments(token):
    if not token:
        return {}

    headers = {"Authorization": f"Bearer {token}"}

    orgs = get_organizations(token)
    short_name = os.getenv("STARSHIP_ORG_SHORTNAME", [_ for _ in orgs.values()][0]['shortName'])

    # FIXME use the first org for now. change to a select in the near term.
    url = f"https://api.astronomer.io/v1alpha1/organizations/{short_name}/deployments"

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
    url = "https://api.astronomer.io/v1alpha1/organizations"

    try:
        api_rv = requests.get(url, headers=headers).json()

        return {org["id"]: org for org in (api_rv or [])}
    except Exception as exc:
        print(exc)
        return {}


@deprecated
class AstroClient:
    def __init__(
            self, token: Optional[str] = None, url: str = "https://api.astronomer.io/hub/v1"
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


@ttl_cache(ttl=3600)
def get_jwk(token: str):
    jwks_url = "https://auth.astronomer.io/.well-known/jwks.json"
    jwks_client = jwt.PyJWKClient(jwks_url)
    return jwks_client.get_signing_key_from_jwt(token)


def get_deployment_url(deployment):
    all_astro_deployments = get_deployments(session.get("token"))

    if all_astro_deployments and all_astro_deployments.get(deployment):
        url = urlparse(
            all_astro_deployments[deployment]["webServerUrl"]
        )
        return f"https:/{url.netloc}/{url.path}"


def set_environment_variables(deployment, remote_vars):
    headers = {"Authorization": f"Bearer {session.get('token')}"}
    client = GraphqlClient(
        endpoint="https://api.astronomer.io/hub/v1", headers=headers
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
