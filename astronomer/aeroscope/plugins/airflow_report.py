from typing import Any, List, Optional

import base64
import json
import logging
import re
import sys
from functools import reduce

from sqlalchemy import text, Integer, cast

logging.getLogger("airflow.settings").setLevel(logging.ERROR)
logging.getLogger("google.cloud.bigquery.opentelemetry_tracing").setLevel(logging.ERROR)

try:
    from airflow.utils.session import provide_session
except:
    from typing import Callable, Iterator, TypeVar

    import contextlib
    from functools import wraps
    from inspect import signature

    from airflow import DAG, settings

    RT = TypeVar("RT")


    def find_session_idx(func):
        # type: (Callable[..., RT]) -> int
        """Find session index in function call parameter."""
        func_params = signature(func).parameters
        try:
            # func_params is an ordered dict -- this is the "recommended" way of getting the position
            session_args_idx = tuple(func_params).index("session")
        except ValueError:
            raise ValueError("Function {} has no `session` argument".format(func.__qualname__)) from None

        return session_args_idx


    @contextlib.contextmanager
    def create_session():
        """Contextmanager that will create and teardown a session."""
        session = settings.Session
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


    def provide_session(func):
        # type: (Callable[..., RT]) -> Callable[..., RT]
        """
        Function decorator that provides a session if it isn't provided.
        If you want to reuse a session or run the function as part of a
        database transaction, you pass it to the function, if not this wrapper
        will create one and close it for you.
        """
        session_args_idx = find_session_idx(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # type: () -> RT
            if "session" in kwargs or session_args_idx < len(args):
                return func(*args, **kwargs)
            else:
                with create_session() as session:
                    return func(*args, session=session, **kwargs)

        return wrapper


def r_false():
    return False


try:
    from airflow.utils.log.secrets_masker import should_hide_value_for_key
except ModuleNotFoundError:
    should_hide_value_for_key = r_false  # too old version
except ImportError:
    should_hide_value_for_key = r_false  # too old version


def airflow_version_report():
    # type: () -> str
    from airflow.version import version

    return version


def providers_report():
    # type: () -> Optional[dict]
    """Return dict of providers packages {package name: version}"""
    try:
        from airflow.providers_manager import ProvidersManager

        providers_manager = ProvidersManager()
        try:
            return {
                provider_info["package-name"]: provider_version
                for provider_version, provider_info in providers_manager.providers.values()
            }
        except TypeError:  # "cannot unpack non-iterable ProviderObject
            # assume airflow +2.3 and providers changed?
            try:
                return {
                    provider.provider_info["package-name"]: provider.version
                    for _, provider in providers_manager.providers.items()
                }
            except AttributeError:  # ProviderObject has no attribute provider_info
                return {key: provider.version for key, provider in providers_manager.providers.items()}
    except ImportError:
        return None
    except ModuleNotFoundError:
        # Older version of airflow
        return None


def hostname_report():
    # type: () -> str
    import socket

    return socket.gethostname()


def installed_packages_report():
    # type: () -> dict
    import pkg_resources

    return {pkg.key: pkg.version for pkg in pkg_resources.working_set}


def configuration_report():
    # type: () -> dict
    from airflow.configuration import conf

    running_configuration = {}

    # Additional list because these are currently not hidden by the SecretsMasker but might contain passwords
    additional_hide_list = {
        "AIRFLOW__CELERY__BROKER_URL",
        "AIRFLOW__CELERY__FLOWER_BASIC_AUTH",
        "AIRFLOW__CELERY__RESULT_BACKEND",
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__ELASTICSEARCH__HOST",
        "AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST",
    }

    for section, options in conf.as_dict(display_source=True, display_sensitive=True).items():
        if section not in running_configuration:
            running_configuration[section] = {}

        for option, (value, config_source) in options.items():
            airflow_env_var_key = "AIRFLOW__{}__{}".format(section.upper(), option.upper())
            if should_hide_value_for_key(airflow_env_var_key) or airflow_env_var_key in additional_hide_list:
                running_configuration[section][option] = ("***", config_source)
            else:
                running_configuration[section][option] = (value, config_source)

    return running_configuration


def env_vars_report():
    # type: () -> dict
    import os

    config_options = []
    connections = []
    variables = []
    for env_var in os.environ.keys():
        if env_var.startswith("AIRFLOW__"):
            config_options.append(env_var)
        elif env_var.startswith("AIRFLOW_CONN_"):
            connections.append(env_var)
        elif env_var.startswith("AIRFLOW_VAR_"):
            variables.append(env_var)

    return {"config_options": config_options, "connections": connections, "variables": variables}


def pools_report():
    # type: () -> Any
    from airflow.models import Pool

    try:
        return Pool.slots_stats()
    except AttributeError:
        try:
            from typing import Dict, Iterable, Optional, Tuple

            from airflow.exceptions import AirflowException
            from airflow.models import Pool
            from airflow.utils.state import State
            from sqlalchemy.orm.session import Session

            EXECUTION_STATES = {
                State.RUNNING,
                State.QUEUED,
            }

            @provide_session
            def slots_stats(*, lock_rows=False, session=None):
                # type: (..., bool, Session) -> Dict[str, dict]
                """
                Get Pool stats (Number of Running, Queued, Open & Total tasks)
                If ``lock_rows`` is True, and the database engine in use supports the ``NOWAIT`` syntax, then a
                non-blocking lock will be attempted -- if the lock is not available then SQLAlchemy will throw an
                OperationalError.
                :param lock_rows: Should we attempt to obtain a row-level lock on all the Pool rows returns
                :param session: SQLAlchemy ORM Session
                """
                from airflow.models.taskinstance import TaskInstance  # Avoid circular import

                pools = {}  # type: Dict[str, dict]

                query = session.query(Pool.pool, Pool.slots)

                pool_rows = query.all()  # type: Iterable[Tuple[str, int]]
                for (pool_name, total_slots) in pool_rows:
                    if total_slots == -1:
                        total_slots = float("inf")  # type: ignore
                    pools[pool_name] = dict(total=total_slots, running=0, queued=0, open=0)

                state_count_by_pool = (
                    session.query(TaskInstance.pool, TaskInstance.state).filter(
                        TaskInstance.state.in_(list(EXECUTION_STATES))
                    )
                ).all()

                # calculate queued and running metrics
                for (pool_name, state) in state_count_by_pool:
                    # Some databases return decimal.Decimal here.
                    count = 1

                    stats_dict = pools.get(pool_name)
                    if not stats_dict:
                        continue
                    # TypedDict key must be a string literal, so we use if-statements to set value
                    if state == "running":
                        stats_dict["running"] = count
                    elif state == "queued":
                        stats_dict["queued"] = count
                    else:
                        raise AirflowException("Unexpected state. Expected values: {}.".format(EXECUTION_STATES))

                # calculate open metric
                for pool_name, stats_dict in pools.items():
                    if stats_dict["total"] == -1:
                        # -1 means infinite
                        stats_dict["open"] = -1
                    else:
                        stats_dict["open"] = stats_dict["total"] - stats_dict["running"] - stats_dict["queued"]

                return pools

            return slots_stats()
        except ModuleNotFoundError:
            # probably in airflow 1.10.2?
            from airflow.api.common.experimental.pool import get_pools

            return [p.to_json() for p in get_pools()]


# noinspection SqlNoDataSourceInspection,PyBroadException
@provide_session
def dags_report(session):
    # type: (Any) -> List[dict]
    import ast

    from airflow.models import DagBag, DagModel, TaskInstance
    from sqlalchemy import distinct, func, literal_column

    # try:
    #     from airflow.models import RenderedTaskInstanceFields
    #     has_rtif = True
    # except ImportError:
    #     RenderedTaskInstanceFields = None
    #     has_rtif = False

    # Use string_agg if it's postgresql, use group_concat otherwise (sqlite, mysql, ?mssql?)
    agg_fn = {"postgresql": func.string_agg}.get(session.bind.dialect.name, func.group_concat)
    agg_fn_extra = [] if session.bind.dialect.name != "postgresql" else [literal_column("','")]

    try:
        dag_model_fields = [
            DagModel.dag_id,
            DagModel.schedule_interval,
            DagModel.root_dag_id,
            DagModel.is_paused,
            DagModel.is_active,
            DagModel.is_subdag,
            DagModel.fileloc,
            DagModel.owners,
            DagModel.tags,
            DagModel.max_active_tasks,
            DagModel.max_active_runs,
            DagModel.has_task_concurrency_limits,
            DagModel.has_import_errors,
        ]
    except AttributeError:
        # some fields didn't exist in airflow 1.10.2
        dag_model_fields = [
            DagModel.dag_id,
            DagModel.is_paused,
            DagModel.is_active,
            DagModel.is_subdag,
            DagModel.fileloc,
            DagModel.owners,
        ]

    # additions = []
    # if 'next_method' in TaskInstance:
    #     additions.append(func.max(cast(TaskInstance.next_method != None, Integer)).label("has_deferrable"))
    # TODO - err - argument of type 'DeclarativeMeta' is not iterable
    # if 'executor_config' in TaskInstance:
    #     additions.append(agg_fn([distinct(TaskInstance.executor_config)] + agg_fn_extra))
        # .executor_config.pod_override[stringified].spec.containers[0].resources
        # .executor_config.pod_override[stringified].spec.containers[0].volume_mounts[0].mount_path
        # .executor_config.pod_override[stringified].spec.volumes[0] (k/v if v is not null)
        # executor_config.pod_override  # is_stringified :/
    # if has_rtif:
    #     pass
        # additions.append(
            # how the hell do you query json?
            # RenderedTaskInstanceFields
            # .rendered_fields
            # .k8s_pod_yaml.metadata.labels.kubernetes_executor=True
            # .k8s_pod_yaml.metadata.name, .k8s_pod_yaml.metadata.namespace
            # .k8s_pod_yaml.spec.containers[0].resources.limits.cpu
            # .k8s_pod_yaml.spec.containers[0].resources.requests.memory
            # .k8s_pod_yaml.spec.containers[0].resources.requests.memory
            # .k8s_pod_yaml.spec.containers[0].volumeMounts.mountPath
            # .k8s_pod_yaml.spec.containers[0].volumeMounts.name
            # .k8s_pod_yaml.spec.volumes[0].*
            # .k8s_pod_yaml.spec.nodeSelector
            # .k8s_pod_yaml.spec.serviceAccount
        # )

    q = session.query(
        *dag_model_fields,
        agg_fn(*[distinct(TaskInstance.operator)] + agg_fn_extra).label("operators"),
        func.count(distinct(TaskInstance.task_id)).label("num_tasks"),

        # TODO - e.g. "0:00:00.002910"
        func.max(TaskInstance.duration).label("max_duration"),

        # TODO - e.g. "-1 day, 23:59:59.906472"
        func.max(TaskInstance.queued_dttm - TaskInstance.start_date).label("max_queued_delay"),

        # *additions
    )
    q = q.join(TaskInstance, TaskInstance.dag_id == DagModel.dag_id)
    # if has_rtif:
    #     q = q.join(RenderedTaskInstanceFields, RenderedTaskInstanceFields.dag_id == DagModel.dag_id)
    q = q.group_by(*dag_model_fields)
    dags = [dict(zip([desc["name"] for desc in q.column_descriptions], res)) for res in q.all()]

    try:
        dagbag = DagBag()
        stats = [
            {
                "file": x.file,
                "duration": x.duration,
                # "dag_num": x.dag_num,
                # "task_num": x.task_num,
                # "dags": sorted(ast.literal_eval(x.dags)),
            }
            for x in dagbag.dagbag_stats
        ]
    except:
        stats = []

    for dag in dags:
        if dag["fileloc"]:
            try:
                dag["parsing_duration"] = next(
                    stat['duration'] for stat in stats if stat['file'] in dag['fileloc']
                ).total_seconds()
            except StopIteration:
                dag["parsing_duration"] = -1
            try:
                dag["variables"], dag["connections"] = dag_varconn_usage(dag["fileloc"])
            except:
                dag["variables"] = None
                dag["connections"] = None
            try:
                for dag_complexity_metric_name, dag_complexity_metric_value in dag_complexity_report(
                    dag["fileloc"]
                ).items():
                    if dag_complexity_metric_name and dag_complexity_metric_value:
                        dag[dag_complexity_metric_name] = dag_complexity_metric_value
            except:
                dag["cc_rank"] = None
                dag["mi_rank"] = None
                dag["analysis"] = None
    return dags


var_patterns = [
    re.compile(r"{{[\s]*var.value.([a-zA-Z-_]+)[\s]*}}"),  # "{{ var.value.<> }}"
    re.compile(r"{{[\s]*var.json.([a-zA-Z-_]+)[\s]*}}"),  # "{{ var.json.<> }}"
    re.compile(r"""Variable.get[(]["']([a-zA-Z-_]+)["'][)]"""),  # "Variable.get(<>)"
]

conn_patterns = [
    re.compile(r"""(?=[\w]*[_])conn_id=["']([a-zA-Z-_]+)["']"""),  # "conn_id=<>"
    re.compile(r"[{]{2}[\s]*conn[.]([a-zA-Z-_]+)[.]?"),  # "{{ conn.<> }}"
]


def dag_varconn_usage(dag_path):
    # type: (str) -> tuple
    var_results = set()
    conn_results = set()
    with open(dag_path) as f:
        dag_contents = f.read()
        for (results, patterns) in [(conn_results, conn_patterns), (var_results, var_patterns)]:
            for pattern in patterns:
                search_results = pattern.findall(dag_contents)
                if search_results:
                    for result in search_results:
                        results.add(result)
    return (var_results or None), (conn_results or None)


# noinspection PyProtectedMember,PyUnresolvedReferences
def dag_complexity_report(dag_path):
    # type: (str) -> dict
    from radon.complexity import average_complexity, cc_rank, cc_visit
    from radon.metrics import mi_rank, mi_visit
    from radon.raw import analyze

    with open(dag_path) as f:
        dag_contents = f.read()
        return {
            "cc_rank": cc_rank(average_complexity(cc_visit(dag_contents))),
            "mi_rank": mi_rank(mi_visit(dag_contents, False)),
            "analysis": analyze(dag_contents)._asdict(),
        }


@provide_session
def connections_report(session):
    # type: (Any) -> List[str]
    try:
        from airflow.models.connection import Connection
    except ModuleNotFoundError:
        # airflow 1.10.2 has a different import path
        from airflow.models import Connection

    return [conn_id for (conn_id,) in session.query(Connection.conn_id)]


@provide_session
def variables_report(session):
    # type: (Any) -> List[str]
    try:
        from airflow.models.variable import Variable
    except:
        # airflow 1.10.2 has a different import path
        from airflow.models import Variable
    return [key for (key,) in session.query(Variable.key)]


def days_ago(dialect, days):
    # type: (str, int) -> str
    if dialect == "sqlite":
        return "DATE('now', '-{} days')".format(days)
    elif dialect == "mysql":
        return "DATE_SUB(NOW(), INTERVAL {} day)".format(days)
    else:
        # postgresql
        return "now() - interval '{} days'".format(days)


# noinspection SqlResolve
@provide_session
def usage_stats_report(session):
    # type: (Any) -> Any
    dialect = session.bind.dialect.name
    sql = text(
        """
        SELECT
            dag_id,
            (select count(1) from task_instance as sti where state = 'success' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "1_days_success",
            (select count(1) from task_instance as sti where state = 'failed' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "1_days_failed",
            (select count(1) from task_instance as sti where state = 'success' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "7_days_success",
            (select count(1) from task_instance as sti where state = 'failed' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "7_days_failed",
            (select count(1) from task_instance as sti where state = 'success' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "30_days_success",
            (select count(1) from task_instance as sti where state = 'failed' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "30_days_failed",
            (select count(1) from task_instance as sti where state = 'success' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "365_days_success",
            (select count(1) from task_instance as sti where state = 'failed' AND start_date > {} and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "365_days_failed",
            (select count(1) from task_instance as sti where state = 'success' and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "all_days_success",
            (select count(1) from task_instance as sti where state = 'failed' and sti.dag_id = ti.dag_id and sti.operator != 'EmptyOperator' and sti.operator != 'DummyOperator') as "all_days_failed"
        FROM task_instance as ti
        GROUP BY 1;
    """.format(
            days_ago(dialect, 1),
            days_ago(dialect, 1),
            days_ago(dialect, 7),
            days_ago(dialect, 7),
            days_ago(dialect, 30),
            days_ago(dialect, 30),
            days_ago(dialect, 365),
            days_ago(dialect, 365),
        )
    )
    return [dict(r) for r in session.execute(sql)]


@provide_session
def db_report(session):
    from airflow.models import DagRun
    from airflow.models import XCom
    from airflow.models import Log
    from airflow.models import TaskInstance
    return {t.__tablename__: session.query(t).count() for t in [DagRun, XCom, Log, TaskInstance]}


# noinspection SqlResolve
@provide_session
def user_report(session):
    # type: (Any) -> dict
    from airflow.version import version
    from sqlalchemy.exc import OperationalError

    dialect = session.bind.dialect.name
    if version >= "1.10.5":
        sql = text(
            """
            SELECT
                (SELECT COUNT(id) FROM ab_user WHERE last_login > {}) AS "1_days_active_users",
                (SELECT COUNT(id) FROM ab_user WHERE last_login > {}) AS "7_days_active_users",
                (SELECT COUNT(id) FROM ab_user WHERE last_login > {}) AS "30_days_active_users",
                (SELECT COUNT(id) FROM ab_user WHERE last_login > {}) AS "365_days_active_users",
                (SELECT COUNT(id) FROM ab_user) AS "total_users";
            """.format(
                days_ago(dialect, 1), days_ago(dialect, 7), days_ago(dialect, 30), days_ago(dialect, 365)
            )
        )
    else:
        sql = text("""SELECT COUNT(id) AS "total_users" FROM users;""")
    for r in session.execute(sql):
        return dict(r)
    return {}


reports = [
    airflow_version_report,
    providers_report,
    hostname_report,
    installed_packages_report,
    configuration_report,
    env_vars_report,
    pools_report,
    dags_report,
    usage_stats_report,
    connections_report,
    variables_report,
    user_report,
    db_report
]


def airflow_report():
    def try_reporter(r):
        try:
            return {r.__name__: r()}
        except Exception as e:
            logging.exception("Failed reporting {}".format(r.__name__))
            return {r.__name__: str(e)}

    collected_reports = [try_reporter(report) for report in reports]

    return reduce(lambda x, y: {**x, **y}, collected_reports)
