"""
Compatability Notes:
- @task() is >=AF2.0
- @task_group is >=AF2.1.0
- Dynamic Task Mapping is >=AF2.3.0
"""
from typing import Any, Union, List

import airflow
from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup

from astronomer_starship.providers.starship.hooks.starship import (
    StarshipLocalHook,
    StarshipHttpHook,
)


class StarshipMigrationOperator(BaseOperator):
    def __init__(self, http_conn_id=None, **kwargs):
        super().__init__(**kwargs)
        self.source_hook = StarshipLocalHook()
        self.target_hook = StarshipHttpHook(http_conn_id=http_conn_id)


class StarshipVariableMigrationOperator(StarshipMigrationOperator):
    def __init__(self, variable_key: Union[List[str], None] = None, **kwargs):
        super().__init__(**kwargs)
        self.variable_key = variable_key

    def execute(self, context: Context) -> Any:
        self.source_hook.get_variables()
        # TODO


def starship_variables_migration(variables: List[str] = None, **kwargs):
    with TaskGroup("variables") as tg:

        @task()
        def variables_task():
            _variables = StarshipLocalHook().get_variables()
            return (
                _variables
                if variables is None
                else {k for k in _variables if k in variables}
            )

        variables_output = variables_task()
        if airflow.__version__ >= "2.3.0":
            (
                StarshipVariableMigrationOperator.partial(**kwargs).expand(
                    task_id="migrate_variables", variable=variables_output
                )
            )
        else:
            for variable in variables_output:
                (
                    variables_output
                    >> StarshipVariableMigrationOperator(
                        task_id=f"migrate_variable_{variable}",
                        variable_key=variable,
                        **kwargs,
                    )
                )
        return tg


class StarshipPoolMigrationOperator(StarshipMigrationOperator):
    def __init__(self, pool_name: Union[List[str], None] = None, **kwargs):
        super().__init__(**kwargs)
        self.pool_name = pool_name

    def execute(self, context: Context) -> Any:
        # TODO
        pass


def starship_pools_migration(pools: List[str] = None, **kwargs):
    with TaskGroup("pools") as tg:

        @task()
        def pools_task():
            _pools = StarshipLocalHook().get_pools()
            return _pools if pools is None else {k for k in _pools if k in pools}

        pools_output = pools_task()
        if airflow.__version__ >= "2.3.0":
            (
                StarshipPoolMigrationOperator.partial(**kwargs).expand(
                    task_id="migrate_pools", variable=pools_output
                )
            )
        else:
            for pool in pools_output:
                (
                    pools_output
                    >> StarshipPoolMigrationOperator(
                        task_id=f"migrate_pool_{pool}", pool_name=pool, **kwargs
                    )
                )
        return tg


class StarshipConnectionMigrationOperator(StarshipMigrationOperator):
    def __init__(self, connection_id: Union[List[str], None] = None, **kwargs):
        super().__init__(**kwargs)
        self.connection_id = connection_id

    def execute(self, context: Context) -> Any:
        # TODO
        pass


def starship_connections_migration(connections: List[str] = None, **kwargs):
    with TaskGroup("connections") as tg:

        @task()
        def connections_task():
            _connections = StarshipLocalHook().get_connections()
            return (
                _connections
                if connections is None
                else {k for k in _connections if k in connections}
            )

        connections_output = connections_task()
        if airflow.__version__ >= "2.3.0":
            (
                StarshipConnectionMigrationOperator.partial(**kwargs).expand(
                    task_id="migrate_connections", variable=connections_output
                )
            )
        else:
            for connection in connections_output:
                (
                    connections_output
                    >> StarshipConnectionMigrationOperator(
                        task_id=f"migrate_connection_{connection}",
                        connection_name=connection,
                        **kwargs,
                    )
                )
        return tg


class StarshipDagHistoryMigrationOperator(StarshipMigrationOperator):
    def __init__(
        self,
        target_dag_id: str,
        unpause_dag_in_target: bool = False,
        dag_run_limit: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_dag_id = target_dag_id
        self.unpause_dag_in_target = unpause_dag_in_target
        self.dag_run_limit = dag_run_limit

    def execute(self, context):
        self.source_hook.set_dag_is_paused(dag_id=self.target_dag_id, is_paused=True)
        # TODO - Poll until all tasks are done

        dag_runs = self.source_hook.get_dag_runs(
            dag_id=self.target_dag_id, limit=self.dag_run_limit
        )
        task_instances = self.source_hook.get_task_instances(
            dag_id=self.target_dag_id, limit=self.dag_run_limit
        )

        self.target_hook.set_dag_runs(dag_runs=dag_runs["dag_runs"])
        self.target_hook.set_task_instances(
            task_instances=task_instances["task_instances"]
        )

        if self.unpause_dag_in_target:
            self.target_hook.set_dag_is_paused(
                dag_id=self.target_dag_id, is_paused=False
            )


def starship_dag_history_migration(dag_ids: List[str] = None, **kwargs):
    with TaskGroup("dag_history") as tg:

        @task()
        def dag_ids_task():
            _dag_ids = StarshipLocalHook().get_dags()
            return (
                [k.dag_id for k in _dag_ids]
                if dag_ids is None
                else [k.dag_id for k in _dag_ids if k in dag_ids]
            )

        dag_ids_output = dag_ids_task()
        if airflow.__version__ >= "2.3.0":
            (
                StarshipDagHistoryMigrationOperator.partial(**kwargs).expand(
                    task_id="migrate_dag_ids", variable=dag_ids_output
                )
            )
        else:
            for dag_id in dag_ids_output:
                (
                    dag_ids_output
                    >> StarshipDagHistoryMigrationOperator(
                        task_id=f"migrate_dag_{dag_id}", target_dag_id=dag_id, **kwargs
                    )
                )
        return tg


def starship_migration(
    variables: List[str] = None,
    pools: List[str] = None,
    connections: List[str] = None,
    dag_ids: List[str] = None,
    **kwargs,
):
    with TaskGroup("migration") as tg:
        starship_variables_migration(variables=variables, **kwargs)
        starship_pools_migration(pools=pools, **kwargs)
        starship_connections_migration(connections=connections, **kwargs)
        starship_dag_history_migration(dag_ids=dag_ids, **kwargs)
        return tg
