import requests

from airflow.models import BaseOperator, Variable

from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from typing import Any, Sequence


class AstroVariableMigrationOperator(BaseOperator):
    """
    Sends variables from Airflow metadatabase to Astronomer Deployment
    """

    template_fields: Sequence[str] = ("token", "deployment_url")
    ui_color = "#974bde"

    def __init__(
        self, deployment_url, token, variable_exclude_list=None, **kwargs
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
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={"key": key, "value": value},
                )
