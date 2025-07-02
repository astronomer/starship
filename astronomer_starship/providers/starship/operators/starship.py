"""Operators, TaskGroups, and DAGs for interacting with the Starship migrations."""
import logging
from datetime import datetime
from typing import Any, Union, List
from packaging.version import Version

import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.task_group import TaskGroup

from astronomer_starship.providers.starship.hooks.starship import (
    StarshipLocalHook,
    StarshipHttpHook,
)


# Compatability Notes:
# - @task() is >=AF2.0
# - @task_group is >=AF2.1
# - Dynamic Task Mapping is >=AF2.3
# - Dynamic Task Mapping labelling is >=AF2.9


class StarshipMigrationOperator(BaseOperator):
    def __init__(self, http_conn_id=None, **kwargs):
        super().__init__(**kwargs)
        self.source_hook = StarshipLocalHook()
        self.target_hook = StarshipHttpHook(http_conn_id=http_conn_id)


class StarshipVariableMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single Variable from one Airflow instance to another."""

    def __init__(self, variable_key: Union[str, None] = None, **kwargs):
        super().__init__(**kwargs)
        self.variable_key = variable_key

    def execute(self, context) -> Any:
        logging.info("Getting Variable", self.variable_key)
        variables = self.source_hook.get_variables()
        variable: Union[dict, None] = (
            [v for v in variables if v["key"] == self.variable_key] or [None]
        )[0]
        if variable is not None:
            logging.info("Migrating Variable", self.variable_key)
            self.target_hook.set_variable(**variable)
        else:
            raise RuntimeError("Variable not found! " + self.variable_key)


def starship_variables_migration(variables: List[str] = None, **kwargs):
    """TaskGroup to fetch and migrate Variables from one Airflow instance to another."""
    with TaskGroup("variables") as tg:

        @task()
        def get_variables():
            _variables = StarshipLocalHook().get_variables()

            _variables = (
                [k["key"] for k in _variables if k["key"] in variables]
                if variables is not None
                else [k["key"] for k in _variables]
            )

            if not len(_variables):
                raise AirflowSkipException("Nothing to migrate")
            return _variables

        variables_results = get_variables()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipVariableMigrationOperator.partial(
                task_id="migrate_variables", **kwargs
            ).expand(variable_key=variables_results)
        else:
            for variable in variables_results.output:
                variables_results >> StarshipVariableMigrationOperator(
                    task_id="migrate_variable_" + variable,
                    variable_key=variable,
                    **kwargs,
                )
        return tg


class StarshipPoolMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single Pool from one Airflow instance to another."""

    def __init__(self, pool_name: Union[str, None] = None, **kwargs):
        super().__init__(**kwargs)
        self.pool_name = pool_name

    def execute(self, context) -> Any:
        logging.info("Getting Pool", self.pool_name)
        pool: Union[dict, None] = (
            [v for v in self.source_hook.get_pools() if v["name"] == self.pool_name]
            or [None]
        )[0]
        if pool is not None:
            logging.info("Migrating Pool", self.pool_name)
            self.target_hook.set_pool(**pool)
        else:
            raise RuntimeError("Pool not found!")


def starship_pools_migration(pools: List[str] = None, **kwargs):
    """TaskGroup to fetch and migrate Pools from one Airflow instance to another."""
    with TaskGroup("pools") as tg:

        @task()
        def get_pools():
            _pools = StarshipLocalHook().get_pools()
            _pools = (
                [k["name"] for k in _pools if k["name"] in pools]
                if pools is not None
                else [k["name"] for k in _pools]
            )

            if not len(_pools):
                raise AirflowSkipException("Nothing to migrate")
            return _pools

        pools_result = get_pools()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipPoolMigrationOperator.partial(
                task_id="migrate_pools", **kwargs
            ).expand(pool_name=pools_result)
        else:
            for pool in pools_result.output:
                pools_result >> StarshipPoolMigrationOperator(
                    task_id="migrate_pool_" + pool, pool_name=pool, **kwargs
                )
        return tg


class StarshipConnectionMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single Connection from one Airflow instance to another."""

    def __init__(self, connection_id: Union[str, None] = None, **kwargs):
        super().__init__(**kwargs)
        self.connection_id = connection_id

    def execute(self, context) -> Any:
        logging.info("Getting Connection", self.connection_id)
        connection: Union[dict, None] = (
            [
                v
                for v in self.source_hook.get_connections()
                if v["conn_id"] == self.connection_id
            ]
            or [None]
        )[0]
        if connection is not None:
            logging.info("Migrating Connection", self.connection_id)
            self.target_hook.set_connection(**connection)
        else:
            raise RuntimeError("Connection not found!")


def starship_connections_migration(connections: List[str] = None, **kwargs):
    """TaskGroup to fetch and migrate Connections from one Airflow instance to another."""
    with TaskGroup("connections") as tg:

        @task()
        def get_connections():
            _connections = StarshipLocalHook().get_connections()
            _connections = (
                [k["conn_id"] for k in _connections if k["conn_id"] in connections]
                if connections is not None
                else [k["conn_id"] for k in _connections]
            )

            if not len(_connections):
                raise AirflowSkipException("Nothing to migrate")
            return _connections

        connections_result = get_connections()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipConnectionMigrationOperator.partial(
                task_id="migrate_connections", **kwargs
            ).expand(connection_id=connections_result)
        else:
            for connection in connections_result.output:
                connections_result >> StarshipConnectionMigrationOperator(
                    task_id="migrate_connection_" + connection.conn_id,
                    connection_id=connection,
                    **kwargs,
                )
        return tg


class StarshipDagHistoryMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single DAG from one Airflow instance to another, with it's history."""

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
        logging.info("Pausing local DAG for", self.target_dag_id)
        self.source_hook.set_dag_is_paused(dag_id=self.target_dag_id, is_paused=True)
        # TODO - Poll until all tasks are done

        logging.info("Getting local DAG Runs for", self.target_dag_id)
        dag_runs = self.source_hook.get_dag_runs(
            dag_id=self.target_dag_id, limit=self.dag_run_limit
        )
        if len(dag_runs["dag_runs"]) == 0:
            raise AirflowSkipException("No DAG Runs found for " + self.target_dag_id)

        logging.info("Getting local Task Instances for", self.target_dag_id)
        task_instances = self.source_hook.get_task_instances(
            dag_id=self.target_dag_id, limit=self.dag_run_limit
        )
        if len(task_instances["task_instances"]) == 0:
            raise AirflowSkipException(
                "No Task Instances found for " + self.target_dag_id
            )

        logging.info("Setting target DAG Runs for", self.target_dag_id)
        self.target_hook.set_dag_runs(dag_runs=dag_runs["dag_runs"])

        logging.info("Setting target Task Instances for", self.target_dag_id)
        self.target_hook.set_task_instances(
            task_instances=task_instances["task_instances"]
        )

        if self.unpause_dag_in_target:
            logging.info("Unpausing target DAG for", self.target_dag_id)
            self.target_hook.set_dag_is_paused(
                dag_id=self.target_dag_id, is_paused=False
            )


def starship_dag_history_migration(dag_ids: List[str] = None, **kwargs):
    """TaskGroup to fetch and migrate DAGs with their history from one Airflow instance to another."""
    with TaskGroup("dag_history") as tg:

        @task()
        def get_dags():
            _dags = StarshipLocalHook().get_dags()
            _dags = (
                [
                    k["dag_id"]
                    for k in _dags
                    if k["dag_id"] in dag_ids
                    and k["dag_id"] != "StarshipAirflowMigrationDAG"
                ]
                if dag_ids is not None
                else [
                    k["dag_id"]
                    for k in _dags
                    if k["dag_id"] != "StarshipAirflowMigrationDAG"
                ]
            )

            if not len(_dags):
                raise AirflowSkipException("Nothing to migrate")
            return _dags

        dags_result = get_dags()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipDagHistoryMigrationOperator.partial(
                task_id="migrate_dag_ids",
                **(
                    {"map_index_template": "{{ task.target_dag_id }}"}
                    if Version(airflow.__version__) >= Version("2.9.0")
                    else {}
                ),
                **kwargs,
            ).expand(target_dag_id=dags_result)
        else:
            for dag_id in dags_result.output:
                dags_result >> StarshipDagHistoryMigrationOperator(
                    task_id="migrate_dag_" + dag_id, target_dag_id=dag_id, **kwargs
                )
        return tg


# noinspection PyPep8Naming
def StarshipAirflowMigrationDAG(
    http_conn_id: str,
    variables: List[str] = None,
    pools: List[str] = None,
    connections: List[str] = None,
    dag_ids: List[str] = None,
    **kwargs,
):
    """
    DAG to fetch and migrate Variables, Pools, Connections, and DAGs with history from one Airflow instance to another.
    """
    dag = DAG(
        dag_id="starship_airflow_migration_dag",
        schedule="@once",
        start_date=datetime(1970, 1, 1),
        tags=["migration", "starship"],
        default_args={"owner": "Astronomer"},
        doc_md="""
        # Starship Migration DAG
        A DAG to migrate Airflow Variables, Pools, Connections, and DAG History from one Airflow instance to another.

        You can use this DAG to migrate all items, or specific items by providing a list of names.

        You can skip migration by providing an empty list.

        ## Setup:
        Make a connection in Airflow with the following details:
        - **Conn ID**: `starship_default`
        - **Conn Type**: `HTTP`
        - **Host**: the URL of the homepage of Airflow (excluding `/home` on the end of the URL)
          - For example, if your deployment URL is `https://astronomer.astronomer.run/abcdt4ry/home`, you'll use `https://astronomer.astronomer.run/abcdt4ry`
        - **Schema**: `https`
        - **Extras**: `{"Authorization": "Bearer <token>"}`

        ## Usage:
        ```python
        from astronomer_starship.providers.starship.operators.starship import (
            StarshipAirflowMigrationDAG,
        )

        globals()["starship_airflow_migration_dag"] = StarshipAirflowMigrationDAG(
            http_conn_id="starship_default",
            variables=None,  # None to migrate all, or ["var1", "var2"] to migrate specific items, or empty list to skip all
            pools=None,  # None to migrate all, or ["pool1", "pool2"] to migrate specific items, or empty list to skip all
            connections=None,  # None to migrate all, or ["conn1", "conn2"] to migrate specific items, or empty list to skip all
            dag_ids=None,  # None to migrate all, or ["dag1", "dag2"] to migrate specific items, or empty list to skip all
        )
        ```
        """,  # noqa: E501
    )
    with dag:
        starship_variables_migration(
            variables=variables, http_conn_id=http_conn_id, **kwargs
        )
        starship_pools_migration(pools=pools, http_conn_id=http_conn_id, **kwargs)
        starship_connections_migration(
            connections=connections, http_conn_id=http_conn_id, **kwargs
        )
        starship_dag_history_migration(
            dag_ids=dag_ids, http_conn_id=http_conn_id, **kwargs
        )
    return dag
