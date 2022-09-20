import requests

from airflow.models import BaseOperator, Connection, Variable, Pool

from airflow.utils.session import provide_session
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
        for key, value in local_vars.items():
            if key not in self.variable_exclude_list:
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

        for key, value in local_connections.items():
            if key not in self.connection_exclude_list:
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

