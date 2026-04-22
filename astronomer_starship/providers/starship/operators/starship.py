"""Operators, TaskGroups, and DAGs for interacting with the Starship migrations."""

import logging
from datetime import datetime
from typing import Any, Callable, List, Optional, Union

import airflow
from airflow.exceptions import AirflowSkipException
from packaging.version import Version

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3
from astronomer_starship.providers.starship.hooks.starship import (
    StarshipHook,
    StarshipHttpHook,
    StarshipLocalHook,
)

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
        logging.info("Getting Variable %s", self.variable_key)
        variables = self.source_hook.get_variables()
        variable: Union[dict, None] = ([v for v in variables if v["key"] == self.variable_key] or [None])[0]
        if variable is not None:
            logging.info("Migrating Variable %s", self.variable_key)
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
            StarshipVariableMigrationOperator.partial(task_id="migrate_variables", **kwargs).expand(
                variable_key=variables_results
            )
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
        logging.info("Getting Pool %s", self.pool_name)
        pool: Union[dict, None] = ([v for v in self.source_hook.get_pools() if v["name"] == self.pool_name] or [None])[
            0
        ]
        if pool is not None:
            logging.info("Migrating Pool %s", self.pool_name)
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
                [k["name"] for k in _pools if k["name"] in pools] if pools is not None else [k["name"] for k in _pools]
            )

            if not len(_pools):
                raise AirflowSkipException("Nothing to migrate")
            return _pools

        pools_result = get_pools()
        if Version(airflow.__version__) >= Version("2.3.0"):
            StarshipPoolMigrationOperator.partial(task_id="migrate_pools", **kwargs).expand(pool_name=pools_result)
        else:
            for pool in pools_result.output:
                pools_result >> StarshipPoolMigrationOperator(task_id="migrate_pool_" + pool, pool_name=pool, **kwargs)
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
            StarshipConnectionMigrationOperator.partial(task_id="migrate_connections", **kwargs).expand(
                connection_id=connections_result
            )
        else:
            for connection in connections_result.output:
                connections_result >> StarshipConnectionMigrationOperator(
                    task_id="migrate_connection_" + connection.conn_id,
                    connection_id=connection,
                    **kwargs,
                )
        return tg


class AbortedError(Exception):
    """Raised by ``migrate_dag_history`` when the caller signals an abort."""


def pause_unpause_dag(
    dag_id: str,
    source_hook: StarshipHook,
    target_hook: StarshipHook,
    *,
    pause_in_source: bool,
    unpause_in_target: bool,
) -> dict:
    """Pause in source and/or unpause in target. Returns what was actually done."""
    result = {"source_paused_by_us": False, "target_unpaused_by_us": False}
    if pause_in_source:
        source_hook.set_dag_is_paused(dag_id=dag_id, is_paused=True)
        result["source_paused_by_us"] = True
        logging.info("Paused DAG %s in source.", dag_id)
    if unpause_in_target:
        target_hook.set_dag_is_paused(dag_id=dag_id, is_paused=False)
        result["target_unpaused_by_us"] = True
        logging.info("Unpaused DAG %s in target.", dag_id)
    return result


def migrate_dag_history(  # noqa: C901
    *,
    source_hook: StarshipHook,
    target_hook: StarshipHook,
    target_dag_id: str,
    dag_run_limit: int = 10,
    pause_dag_in_source: bool = True,
    unpause_dag_in_target: bool = False,
    pre_checks: bool = False,
    migrate_ti_history: bool = False,
    on_step: Optional[Callable[[str], None]] = None,
    check_abort: Optional[Callable[[], None]] = None,
) -> dict:
    """Migrate one DAG's runs + task instances (+ optional TI history) between Airflow instances.

    The caller owns ``source_hook`` / ``target_hook`` direction. For the
    classic push flow, source is local and target is remote. For the cutover
    wave flow, source is remote and target is local.

    Optional cutover-style features are opt-in:
    - ``pre_checks``: verify the target DAG is paused and has no existing runs.
    - ``migrate_ti_history``: also copy ``TaskInstanceHistory`` (AF 2.10+).
    - ``on_step(label)``: progress callback for UI-driven consumers.
    - ``check_abort()``: raise :class:`AbortedError` to stop before a write.

    Returns a dict with counts and ``latest_data_interval_end``.
    """

    def _step(label: str) -> None:
        if on_step:
            on_step(label)

    def _abort_guard() -> None:
        if check_abort:
            check_abort()

    if pause_dag_in_source:
        _abort_guard()
        _step("Pausing source")
        source_hook.set_dag_is_paused(dag_id=target_dag_id, is_paused=True)
        logging.info("[%s] Paused in source.", target_dag_id)

    if pre_checks:
        _abort_guard()
        _step("Pre-checks")
        target_dag = target_hook.get_dag(dag_id=target_dag_id)
        if not target_dag.get("is_paused"):
            raise RuntimeError(f"DAG '{target_dag_id}' is active in target. Pause it before migrating.")
        if target_dag.get("dag_run_count"):
            raise RuntimeError(f"DAG '{target_dag_id}' already has runs in target. Clear them first.")
        logging.info("[%s] Pre-checks passed.", target_dag_id)

    _abort_guard()
    _step("Fetching DAG runs")
    dag_runs_resp = source_hook.get_dag_runs(dag_id=target_dag_id, limit=dag_run_limit)
    dag_runs = dag_runs_resp.get("dag_runs", []) if isinstance(dag_runs_resp, dict) else []
    if not dag_runs:
        raise AirflowSkipException(f"No DAG Runs found for {target_dag_id}")
    logging.info("[%s] Fetched %d DAG runs.", target_dag_id, len(dag_runs))

    # Each DAG run has many task instances -- fetch with a higher cap.
    ti_limit = dag_run_limit * 200
    _abort_guard()
    _step("Fetching task instances")
    ti_resp = source_hook.get_task_instances(dag_id=target_dag_id, limit=ti_limit)
    task_instances = ti_resp.get("task_instances", []) if isinstance(ti_resp, dict) else []
    if not task_instances:
        raise AirflowSkipException(f"No Task Instances found for {target_dag_id}")
    logging.info("[%s] Fetched %d task instances.", target_dag_id, len(task_instances))

    _step("Writing DAG runs")
    target_hook.set_dag_runs(dag_runs=dag_runs)

    intervals = [dr["data_interval_end"] for dr in dag_runs if dr.get("data_interval_end")]
    latest_data_interval_end = max(intervals) if intervals else None

    _step("Writing task instances")
    target_hook.set_task_instances(task_instances=task_instances)

    ti_history_count = 0
    if migrate_ti_history:
        # Writes already happened; don't honour abort here — we'd leave partial state.
        _step("Fetching TI history")
        try:
            history = source_hook.get_task_instance_history(dag_id=target_dag_id, limit=ti_limit)
            history_records = history.get("task_instances", []) if isinstance(history, dict) else []
            if history_records:
                _step("Writing TI history")
                target_hook.set_task_instance_history(task_instances=history_records)
                ti_history_count = len(history_records)
        except Exception:
            logging.info("[%s] TI history not available (pre-AF 2.10). Skipping.", target_dag_id)

    if unpause_dag_in_target:
        _step("Unpausing target")
        target_hook.set_dag_is_paused(dag_id=target_dag_id, is_paused=False)
        logging.info("[%s] Unpaused in target.", target_dag_id)

    return {
        "dag_runs_migrated": len(dag_runs),
        "task_instances_migrated": len(task_instances),
        "task_instance_history_migrated": ti_history_count,
        "latest_data_interval_end": latest_data_interval_end,
    }


class StarshipDagHistoryMigrationOperator(StarshipMigrationOperator):
    """Operator to migrate a single DAG from one Airflow instance to another, with its history.

    Default behaviour preserves the classic push flow: pause locally, fetch
    dag_runs + task_instances, push to target, optionally unpause target.

    Cutover-style options (``pre_checks``, ``migrate_ti_history``) are
    off by default to keep existing DAGs unchanged.
    """

    def __init__(
        self,
        target_dag_id: str,
        unpause_dag_in_target: bool = False,
        dag_run_limit: int = 10,
        pre_checks: bool = False,
        migrate_ti_history: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_dag_id = target_dag_id
        self.unpause_dag_in_target = unpause_dag_in_target
        self.dag_run_limit = dag_run_limit
        self.pre_checks = pre_checks
        self.migrate_ti_history = migrate_ti_history

    def execute(self, context):
        return migrate_dag_history(
            source_hook=self.source_hook,
            target_hook=self.target_hook,
            target_dag_id=self.target_dag_id,
            dag_run_limit=self.dag_run_limit,
            pause_dag_in_source=True,
            unpause_dag_in_target=self.unpause_dag_in_target,
            pre_checks=self.pre_checks,
            migrate_ti_history=self.migrate_ti_history,
        )


class StarshipCutoverMigrationOperator(BaseOperator):
    """Cutover-direction DAG migration: remote source → local target.

    Inverse of :class:`StarshipDagHistoryMigrationOperator`. Uses the
    Starship auth factory to resolve the source connection, so Astro,
    MWAA, GCC and OSS sources all work from the same operator.

    Defaults to cutover behaviour: pre-checks on, TI history on.
    """

    def __init__(
        self,
        target_dag_id: str,
        source_conn_id: str = "starship_source",
        dag_run_limit: int = 500,
        pause_dag_in_source: bool = True,
        unpause_dag_in_target: bool = False,
        pre_checks: bool = True,
        migrate_ti_history: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_dag_id = target_dag_id
        self.source_conn_id = source_conn_id
        self.dag_run_limit = dag_run_limit
        self.pause_dag_in_source = pause_dag_in_source
        self.unpause_dag_in_target = unpause_dag_in_target
        self.pre_checks = pre_checks
        self.migrate_ti_history = migrate_ti_history

    def execute(self, context):
        # Deferred import keeps the operator usable when the auth factory's
        # cloud-SDK branches aren't installed (the factory itself is lazy).
        from astronomer_starship.providers.starship.auth import resolve_source_hook

        source_hook = resolve_source_hook(self.source_conn_id)
        target_hook = StarshipLocalHook()
        return migrate_dag_history(
            source_hook=source_hook,
            target_hook=target_hook,
            target_dag_id=self.target_dag_id,
            dag_run_limit=self.dag_run_limit,
            pause_dag_in_source=self.pause_dag_in_source,
            unpause_dag_in_target=self.unpause_dag_in_target,
            pre_checks=self.pre_checks,
            migrate_ti_history=self.migrate_ti_history,
        )


def starship_dag_history_migration(dag_ids: List[str] = None, **kwargs):
    """TaskGroup to fetch and migrate DAGs with their history from one Airflow instance to another."""
    with TaskGroup("dag_history") as tg:

        @task()
        def get_dags():
            _dags = StarshipLocalHook().get_dags()
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
def StarshipAirflowMigrationDAG(  # noqa: N802
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
        starship_variables_migration(variables=variables, http_conn_id=http_conn_id, **kwargs)
        starship_pools_migration(pools=pools, http_conn_id=http_conn_id, **kwargs)
        starship_connections_migration(connections=connections, http_conn_id=http_conn_id, **kwargs)
        starship_dag_history_migration(dag_ids=dag_ids, http_conn_id=http_conn_id, **kwargs)
    return dag
