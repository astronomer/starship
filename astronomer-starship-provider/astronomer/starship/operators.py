from airflow.models import BaseOperator
from airflow.utils.session import provide_session

from astronomer.starship.connections.operators import AstroConnectionsMigrationOperator
from astronomer.starship.variables.operators import AstroVariableMigrationOperator
from astronomer.starship.env.operators import AstroEnvMigrationOperator

from sqlalchemy.orm import Session
from typing import Any, Sequence

class AstroMigrationOperator(BaseOperator):
    """
    Sends connections, variables, and environment variables from a source Airflow to Astronomer Deployment
    """
    template_fields: Sequence[str] = ('token', 'deployment_url')
    ui_color = "#974bde"

    def __init__(
            self,
            deployment_url,
            token,
            variables_exclude_list=None,
            connection_exclude_list=None,
            env_include_list=None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_url = deployment_url
        self.token = token
        self.variables_exclude_list = variables_exclude_list
        self.connection_exclude_list = connection_exclude_list
        self.env_include_list = env_include_list

    @provide_session
    def execute(self, context: Any, session: Session) -> None:
        variables = AstroVariableMigrationOperator(
            task_id='export_variables',
            deployment_url=self.deployment_url,
            token=self.token,
            variable_exclude_list=self.variables_exclude_list
        )

        connections = AstroConnectionsMigrationOperator(
            task_id='export_connections',
            deployment_url=self.deployment_url,
            token=self.token,
            connection_exclude_list=self.connection_exclude_list
        )

        env_vars = AstroEnvMigrationOperator(
            task_id='export_env_vars',
            deployment_url=self.deployment_url,
            token=self.token,
            env_include_list=self.env_include_list
        )

        variables.execute(context=context)
        connections.execute(context=context)
        env_vars.execute(context=context)