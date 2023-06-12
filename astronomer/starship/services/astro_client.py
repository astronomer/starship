import logging
import os
from typing import Optional, Iterator, Any, Dict
from urllib.parse import urlparse

import jwt
import requests
from cachetools.func import ttl_cache
from pydash import at
from python_graphql_client import GraphqlClient
from deprecated import deprecated

ASTRO_AUTH = os.environ.get("STARSHIP_ASTRO_AUTH", "https://auth.astronomer.io")
ASTROHUB_API = os.environ.get(
    "STARSHIP_ASTROHUB_API", "https://api.astronomer.io/hub/v1"
)
ASTROHUB_GRAPHQL_API = os.environ.get(
    "STARSHIP_ASTROHUB_GRAPHQL_API", "https://api.astronomer.io/hub/graphql"
)
ASTRO_ALPHA_API = os.environ.get(
    "STARSHIP_ASTRO_ALPHA_API", "https://api.astronomer.io/v1alpha1"
)


def get_username(token):
    headers = {"Authorization": f"Bearer {token}"}
    client = GraphqlClient(endpoint=ASTROHUB_API, headers=headers)
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
    short_name = os.getenv(
        "STARSHIP_ORG_SHORTNAME", [_ for _ in orgs.values()][0]["shortName"]
    )

    # FIXME use the first org for now. change to a select in the near term.
    url = f"{ASTRO_ALPHA_API}/organizations/{short_name}/deployments"

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        deployments = r.json().get("deployments", [])
        if len(deployments):
            return {deployment["id"]: deployment for deployment in deployments}
    except Exception as e:
        logging.exception(e)
    return {}


def get_deployment(deployment_id: str, token) -> dict:
    return get_deployments(token)[deployment_id]


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
        url = urlparse(all_astro_deployments[deployment]["webServerUrl"])
        return f"https:/{url.netloc}/{url.path}"


def set_environment_variables(
    deployment: str, token: str, remote_vars: Dict[str, Dict[str, Any]]
):
    headers = {"Authorization": f"Bearer {token}"}
    client = GraphqlClient(endpoint=ASTROHUB_API, headers=headers)
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


def set_changed_environment_variables(
    deployment: str, token: str, items: Iterator[str]
) -> None:
    """
    :param deployment - deployment id
    :param token - user or deployment or workspace token
    :param items: str {'env-AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE': 'AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE'}
    """
    remote_vars = get_environment_variables(deployment, token)
    for item in items:
        if item not in remote_vars.keys():
            remote_vars.setdefault(
                item,
                {
                    "key": item,
                    "value": os.environ[item],
                    "isSecret": False,
                },
            )
    return set_environment_variables(deployment, token, remote_vars)


def get_environment_variables(deployment: str, token: str):
    try:
        # OLD STYLE? - part of the deployment
        return {
            remote_var["key"]: {
                "key": remote_var["key"],
                "value": remote_var["value"],
                "isSecret": remote_var["isSecret"],
            }
            for remote_var in get_deployment(deployment, token)["environmentVariables"]
        }
    except KeyError as e:
        logging.warning(e)
        logging.info("Getting ENV VARS via newer query")
        # NEW STYLE? - separate object
        headers = {
            "Authorization": f"Bearer {token}",
            "astro-current-org-id": "clacwluxo0u3i0t09epts2tjg",
        }
        client = GraphqlClient(
            #  api.astronomer.io/hub/graphql
            endpoint=ASTROHUB_GRAPHQL_API,
            headers=headers,
        )
        query = """
          fragment EnvironmentVariable on EnvironmentVariable {
            key
            value
            isSecret
            updatedAt
          }

          fragment DeploymentSpec on DeploymentSpec {
            environmentVariablesObjects {
              ...EnvironmentVariable
            }
          }

          query deploymentSpec($id: Id!) {
            deployment(id: $id) {
              id
              deploymentSpec {
                ...DeploymentSpec
              }
            }
          }"""
        r = client.execute(
            query,
            {"id": deployment},
        )
        return (
            r.get("data", {})
            .get("deploymentSpec", {})
            .get("environmentVariablesObjects", {})
        )


def is_environment_variable_migrated(deployment: str, token: str, key: str) -> bool:
    return key in get_environment_variables(deployment, token).keys()


@deprecated
class AstroClient:
    def __init__(self, token: Optional[str] = None, url: str = ASTROHUB_API):
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
