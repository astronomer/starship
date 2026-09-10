"""Operators, TaskGroups, and DAGs for interacting with the Starship migrations."""

try:
    from airflow.providers.http.hooks.http import HttpHook  # noqa: F401
except ImportError as e:
    raise ImportError(
        "The Starship migration DAG requires the apache-airflow-providers-http provider. "
        "Install it to use the migration DAG."
    ) from e

import logging
from datetime import datetime
from typing import Any, List, Union

import airflow
from airflow.exceptions import AirflowSkipException
from packaging.version import Version

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3
from astronomer_starship.providers.starship.hooks.starship import (
    STARSHIP_SOURCE_CONN_ID,
    StarshipHttpHook,
)

# Dispatch to the version-appropriate source hook. On AF2 direct DB access
# works, so the source hook is a real BaseHook. On AF3 workers cannot touch
# the DB, so it's an HttpHook against the source Starship API.
if AIRFLOW_V_2:
    from astronomer_starship._af2.starship_hook import StarshipLocalHook as SourceHook
elif AIRFLOW_V_3:
    from astronomer_starship._af3.starship_hook import StarshipHttpSourceHook as SourceHook
else:
    raise RuntimeError("Unsupported Airflow version")

if AIRFLOW_V_3:
    from airflow.sdk import DAG, BaseOperator, TaskGroup, task
elif AIRFLOW_V_2:
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models.baseoperator import BaseOperator
    from airflow.utils.task_group import TaskGroup
else:
    raise RuntimeError("Unsupported Airflow version")


# Compatability Notes:
# - @task() is >=AF2.0
# - @task_group is >=AF2.1
# - Dynamic Task Mapping is >=AF2.3
# - Dynamic Task Mapping labelling is >=AF2.9


class StarshipMigrationOperator(BaseOperator):
    def __init__(
        self,
        http_conn_id=None,
        source_http_conn_id=None,
        target_http_conn_id=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # `http_conn_id` remains the legacy target alias; new callers should
        # prefer the explicit source/target kwargs.
        target_conn = target_http_conn_id or http_conn_id
        source_conn = source_http_conn_id or STARSHIP_SOURCE_CONN_ID
        self.source_hook = SourceHook(http_conn_id=source_conn)
        self.target_hook = StarshipHttpHook(http_conn_id=target_conn)


class StarshipVariableMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single Variable from one Airflow instance to another."""

    def __init__(self, variable_key: Union[str, None] = None, **kwargs):
        super().__init__(**kwargs)
        self.variable_key = variable_key

    def execute(self, context) -> Any:
        logging.info("Getting Variable %s", self.variable_key)
        variables = self.source_hook.get_variables()
        variable: Union[dict, None] = ([v for v in variables if v["key"] == self.variable_key] or [None])[0]
        if variable is not None:
            logging.info("Migrating Variable %s", self.variable_key)
            self.target_hook.set_variable(**variable)
        else:
            raise RuntimeError("Variable not found! " + self.variable_key)


def starship_variables_migration(
    variables: List[str] = None,
    source_http_conn_id: str = None,
    **kwargs,
):
    """TaskGroup to fetch and migrate Variables from one Airflow instance to another."""
    with TaskGroup("variables") as tg:

        @task()
        def get_variables():
            _variables = SourceHook(http_conn_id=source_http_conn_id or STARSHIP_SOURCE_CONN_ID).get_variables()

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
                task_id="migrate_variables",
                source_http_conn_id=source_http_conn_id,
                **kwargs,
            ).expand(variable_key=variables_results)
        else:
            for variable in variables_results.output:
                variables_results >> StarshipVariableMigrationOperator(
                    task_id="migrate_variable_" + variable,
                    variable_key=variable,
                    source_http_conn_id=source_http_conn_id,
                    **kwargs,
                )
        return tg


class StarshipPoolMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single Pool from one Airflow instance to another."""

    def __init__(self, pool_name: Union[str, None] = None, **kwargs):
        super().__init__(**kwargs)
        self.pool_name = pool_name

    def execute(self, context) -> Any:
        logging.info("Getting Pool %s", self.pool_name)
        pool: Union[dict, None] = ([v for v in self.source_hook.get_pools() if v["name"] == self.pool_name] or [None])[
            0
        ]
        if pool is not None:
            logging.info("Migrating Pool %s", self.pool_name)
            self.target_hook.set_pool(**pool)
        else:
            raise RuntimeError("Pool not found!")


def starship_pools_migration(
    pools: List[str] = None,
    source_http_conn_id: str = None,
    **kwargs,
):
    """TaskGroup to fetch and migrate Pools from one Airflow instance to another."""
    with TaskGroup("pools") as tg:

        @task()
        def get_pools():
            _pools = SourceHook(http_conn_id=source_http_conn_id or STARSHIP_SOURCE_CONN_ID).get_pools()
            _pools = (
                [k["name"] for k in _pools if k["name"] in pools] if pools is not None else [k["name"] for k in _pools]
            )

            if not len(_pools):
                raise AirflowSkipException("Nothing to migrate")
            return _pools

        pools_result = get_pools()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipPoolMigrationOperator.partial(
                task_id="migrate_pools",
                source_http_conn_id=source_http_conn_id,
                **kwargs,
            ).expand(pool_name=pools_result)
        else:
            for pool in pools_result.output:
                pools_result >> StarshipPoolMigrationOperator(
                    task_id="migrate_pool_" + pool,
                    pool_name=pool,
                    source_http_conn_id=source_http_conn_id,
                    **kwargs,
                )
        return tg


class StarshipConnectionMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single Connection from one Airflow instance to another."""

    def __init__(self, connection_id: Union[str, None] = None, **kwargs):
        super().__init__(**kwargs)
        self.connection_id = connection_id

    def execute(self, context) -> Any:
        logging.info("Getting Connection %s", self.connection_id)
        connection: Union[dict, None] = (
            [v for v in self.source_hook.get_connections() if v["conn_id"] == self.connection_id] or [None]
        )[0]
        if connection is not None:
            logging.info("Migrating Connection %s", self.connection_id)
            self.target_hook.set_connection(**connection)
        else:
            raise RuntimeError("Connection not found!")


def starship_connections_migration(
    connections: List[str] = None,
    source_http_conn_id: str = None,
    **kwargs,
):
    """TaskGroup to fetch and migrate Connections from one Airflow instance to another."""
    with TaskGroup("connections") as tg:

        @task()
        def get_connections():
            _connections = SourceHook(http_conn_id=source_http_conn_id or STARSHIP_SOURCE_CONN_ID).get_connections()
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
                task_id="migrate_connections",
                source_http_conn_id=source_http_conn_id,
                **kwargs,
            ).expand(connection_id=connections_result)
        else:
            for connection in connections_result.output:
                connections_result >> StarshipConnectionMigrationOperator(
                    task_id="migrate_connection_" + connection,
                    connection_id=connection,
                    source_http_conn_id=source_http_conn_id,
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
        logging.info("Pausing local DAG for %s", self.target_dag_id)
        self.source_hook.set_dag_is_paused(dag_id=self.target_dag_id, is_paused=True)
        # TODO - Poll until all tasks are done

        logging.info("Getting local DAG Runs for %s", self.target_dag_id)
        dag_runs = self.source_hook.get_dag_runs(dag_id=self.target_dag_id, limit=self.dag_run_limit)
        if len(dag_runs["dag_runs"]) == 0:
            raise AirflowSkipException("No DAG Runs found for " + self.target_dag_id)

        logging.info("Getting local Task Instances for %s", self.target_dag_id)
        task_instances = self.source_hook.get_task_instances(dag_id=self.target_dag_id, limit=self.dag_run_limit)
        if len(task_instances["task_instances"]) == 0:
            raise AirflowSkipException("No Task Instances found for " + self.target_dag_id)

        logging.info("Setting target DAG Runs for %s", self.target_dag_id)
        self.target_hook.set_dag_runs(dag_runs=dag_runs["dag_runs"])

        logging.info("Setting target Task Instances for %s", self.target_dag_id)
        self.target_hook.set_task_instances(task_instances=task_instances["task_instances"])

        if self.unpause_dag_in_target:
            logging.info("Unpausing target DAG for %s", self.target_dag_id)
            self.target_hook.set_dag_is_paused(dag_id=self.target_dag_id, is_paused=False)


def starship_dag_history_migration(
    dag_ids: List[str] = None,
    source_http_conn_id: str = None,
    **kwargs,
):
    """TaskGroup to fetch and migrate DAGs with their history from one Airflow instance to another."""
    with TaskGroup("dag_history") as tg:

        @task()
        def get_dags():
            _dags = SourceHook(http_conn_id=source_http_conn_id or STARSHIP_SOURCE_CONN_ID).get_dags()
            _dags = (
                [k["dag_id"] for k in _dags if k["dag_id"] in dag_ids and k["dag_id"] != "StarshipAirflowMigrationDAG"]
                if dag_ids is not None
                else [k["dag_id"] for k in _dags if k["dag_id"] != "StarshipAirflowMigrationDAG"]
            )

            if not len(_dags):
                raise AirflowSkipException("Nothing to migrate")
            return _dags

        dags_result = get_dags()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipDagHistoryMigrationOperator.partial(
                task_id="migrate_dag_ids",
                source_http_conn_id=source_http_conn_id,
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
                    task_id="migrate_dag_" + dag_id,
                    target_dag_id=dag_id,
                    source_http_conn_id=source_http_conn_id,
                    **kwargs,
                )
        return tg


# noinspection PyPep8Naming
def StarshipAirflowMigrationDAG(  # noqa: N802
    http_conn_id: str = None,
    variables: List[str] = None,
    pools: List[str] = None,
    connections: List[str] = None,
    dag_ids: List[str] = None,
    source_http_conn_id: str = None,
    target_http_conn_id: str = None,
    **kwargs,
):
    """
    DAG to fetch and migrate Variables, Pools, Connections, and DAGs with history from one Airflow instance to another.
    """
    # `target_http_conn_id` is preferred; `http_conn_id` remains as a legacy
    # alias so existing DAGs keep working. `source_http_conn_id` overrides the
    # default source connection id (``starship_source``) used on Airflow 3.
    target_http_conn_id = target_http_conn_id or http_conn_id
    if not target_http_conn_id:
        raise ValueError(
            "StarshipAirflowMigrationDAG requires a target connection. "
            "Pass target_http_conn_id (preferred) or http_conn_id."
        )
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

        ### Target connection (required)
        Make an Airflow HTTP connection pointing at the **target** Airflow:
        - **Conn ID**: `starship_default` (or any id you pass as `target_http_conn_id` / `http_conn_id`)
        - **Conn Type**: `HTTP`
        - **Host**: the URL of the homepage of the target Airflow (excluding `/home` on the end of the URL)
          - For example, if your deployment URL is `https://astronomer.astronomer.run/abcdt4ry/home`, you'll use `https://astronomer.astronomer.run/abcdt4ry`
        - **Schema**: `https`
        - **Extras**: `{"Authorization": "Bearer <token>"}`

        ### Source connection (Airflow 3 only)
        On Airflow 3, workers cannot access the metadata DB directly, so the DAG also needs an HTTP connection pointing at the **source** Airflow:
        - **Conn ID**: `starship_source` (default) or the id you pass as `source_http_conn_id`
        - **Conn Type**: `HTTP`
        - **Host**, **Schema**, **Extras**: same shape as the target connection, but pointing at the source Airflow and using a source-side API token.

        On Airflow 2 the DAG reads source metadata directly from the local metadata DB, so no source connection is needed.

        ## Usage:
        ```python
        from astronomer_starship.providers.starship.operators.starship import (
            StarshipAirflowMigrationDAG,
        )

        globals()["starship_airflow_migration_dag"] = StarshipAirflowMigrationDAG(
            # Preferred kwargs (Airflow 2 + 3):
            target_http_conn_id="starship_default",
            source_http_conn_id="starship_source",  # Airflow 3 only; ignored on Airflow 2
            # Legacy alias (still supported; acts as target_http_conn_id):
            # http_conn_id="starship_default",
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
            variables=variables,
            http_conn_id=target_http_conn_id,
            source_http_conn_id=source_http_conn_id,
            **kwargs,
        )
        starship_pools_migration(
            pools=pools,
            http_conn_id=target_http_conn_id,
            source_http_conn_id=source_http_conn_id,
            **kwargs,
        )
        starship_connections_migration(
            connections=connections,
            http_conn_id=target_http_conn_id,
            source_http_conn_id=source_http_conn_id,
            **kwargs,
        )
        starship_dag_history_migration(
            dag_ids=dag_ids,
            http_conn_id=target_http_conn_id,
            source_http_conn_id=source_http_conn_id,
            **kwargs,
        )
    return dag
