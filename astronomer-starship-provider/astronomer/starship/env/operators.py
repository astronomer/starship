import os

from airflow.models import BaseOperator
from airflow.utils.session import provide_session

from python_graphql_client import GraphqlClient
from sqlalchemy.orm import Session
from typing import Any, Sequence

class AstroEnvMigrationOperator(BaseOperator):
    """
    Sends env vars from Airflow to Astronomer Deployment
    """
    template_fields: Sequence[str] = ('token', 'deployment_url')
    ui_color = "#974bde"

    def __init__(
            self,
            deployment_url,
            token,
            env_include_list,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_url = deployment_url
        self.token = token
        self.env_include_list = env_include_list

    def _find_deployment_id(self):
        deployments = self._astro_deployments()
        for id, deployment in deployments.items():
            url = deployment['deploymentSpec']['webserver']['url']
            split_url = url.split("?", 1)
            base_url = split_url[0]
            if base_url in self.deployment_url:
                return id
                #TODO: Add logic if deployment id is not found

    def _existing_env_vars(self):
        deployment_id = self._find_deployment_id()

        client = GraphqlClient(
            endpoint="https://api.astronomer.io/hub/v1",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )

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

        try:
            env_vars = {}
            deployments = client.execute(query,)["data"]["deployments"]
            for deployment in deployments:
                if deployment['id'] == deployment_id:
                    env_vars = deployment['deploymentSpec']['environmentVariables']

            existing_vars = {}
            for var in env_vars:
                existing_vars[var['key']] = {
                    "key": var['key'],
                    "value": var['value'],
                    "isSecret": var['isSecret']
                }

            return existing_vars
        except Exception as exc:
            print(exc)
            return {}

    def _astro_deployments(self):
        headers = {"Authorization": f"Bearer {self.token}"}
        client = GraphqlClient(
            endpoint="https://api.astronomer.io/hub/v1", headers=headers
        )
        query = """
        {
            deployments
            {
                id,
                deploymentSpec
                {
                    webserver {
                        url
                    }
                }
            }
        }
        """

        try:
            api_rv = client.execute(query)["data"]["deployments"]

            return {deploy["id"]: deploy for deploy in (api_rv or [])}
        except Exception as exc:
            print(exc)
            return {}

    @provide_session
    def execute(self, context: Any, session: Session) -> None:
        client = GraphqlClient(
            endpoint="https://api.astronomer.io/hub/v1",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
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
        deployment = self._find_deployment_id()

        complete_env_list = self._existing_env_vars()

        for key, value in os.environ.items():
            if self.env_include_list:
                if key in self.env_include_list and self.env_include_list:
                    complete_env_list[key] = {
                        "key": key,
                        "value": value,
                        "isSecret": False
                    }

        client.execute(
            query,
            {
                "input": {
                    "deploymentId": deployment,
                    "environmentVariables": list(complete_env_list.values()),
                }
            },
        )
