import os
import requests

from airflow.models import BaseOperator, Connection, Variable, Pool

from airflow.utils.session import provide_session
from python_graphql_client import GraphqlClient
from sqlalchemy.orm import Session
from typing import Any, Sequence

class AstroVariableMigrationOperator(BaseOperator):
    """
    Sends variables from Airflow metadatabase to Astronomer Deployment
    """
    template_fields: Sequence[str] = ('token', 'deployment_url')
    ui_color = "#974bde"

    def __init__(
            self,
            deployment_url,
            token,
            variable_exclude_list=None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_url = deployment_url
        self.token = token
        self.variable_exclude_list = variable_exclude_list

    @provide_session
    def execute(self, context: Any, session: Session) -> None:
        get_variables = session.query(Variable).all()
        local_vars = {var.key: var.val for var in get_variables}

        if self.variable_exclude_list:
            exclude_list = self.variable_exclude_list
        else:
            exclude_list = []

        for key, value in local_vars.items():
            if key not in exclude_list:
                requests.post(
                    url=f"{self.deployment_url}/api/v1/variables",
                    headers={
                        "Authorization": f"Bearer {self.token}"
                    },
                    json={"key": key, "value": value}
                )

class AstroConnectionsMigrationOperator(BaseOperator):
    """
    Sends connections from Airflow metadatabase to Astronomer Deployment
    """
    template_fields: Sequence[str] = ('token', 'deployment_url')
    ui_color = "#974bde"

    def __init__(
            self,
            deployment_url,
            token,
            connection_exclude_list=None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_url = deployment_url
        self.token = token
        self.connection_exclude_list = connection_exclude_list

    @provide_session
    def execute(self, context: Any, session: Session) -> None:
        get_connections = session.query(Connection).all()
        local_connections = {conn.conn_id: conn for conn in get_connections}

        if self.connection_exclude_list:
            exclude_list = self.connection_exclude_list
        else:
            exclude_list = []

        for key, value in local_connections.items():
            if key not in exclude_list:
                requests.post(
                    url=f"{self.deployment_url}/api/v1/connections",
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={
                        "connection_id": key,
                        "conn_type": value.conn_type,
                        "host": value.host,
                        "login": value.login,
                        "schema": value.schema,
                        "port": value.port,
                        "password": value.password or "",
                        "extra": value.extra,
                    }
                )

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

        complete_env_list = {}
        for key, value in os.environ.items():
            if key in self.env_include_list:
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