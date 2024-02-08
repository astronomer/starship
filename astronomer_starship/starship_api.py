from __future__ import annotations

import os
from abc import ABC, abstractmethod
from airflow.plugins_manager import AirflowPlugin
from airflow.version import version
from airflow.www.app import csrf
from flask import Blueprint, request, jsonify
from flask_appbuilder import expose, BaseView
from typing import Any, Callable


def starship_route(
    get=None,
    post=None,
    put=None,
    delete=None,
    patch=None,
    kwargs_fn: Callable[[], dict] = None,
):
    try:
        args = []
        kwargs = kwargs_fn() if kwargs_fn else {}
    except RuntimeError as e:
        return jsonify({"error": e}), 400
    except Exception as e:
        return jsonify({"error": f"Unknown Error in kwargs_fn - {e}"}), 500

    if request.method not in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
        raise RuntimeError(f"Unsupported Method: {request.method}")

    try:
        if request.method == "GET":
            res = get(*args, **kwargs)
        elif request.method == "POST":
            from sqlalchemy.exc import IntegrityError, DataError, StatementError

            try:
                res = post(*args, **kwargs)
            except IntegrityError as e:
                res = jsonify(
                    {
                        "error": "Integrity Error (Duplicate Record?)",
                        "error_message": e,
                        "kwargs": kwargs,
                    }
                )
                res.status_code = 409
            except DataError as e:
                res = jsonify(
                    {"error": "Data Error", "error_message": e, "kwargs": kwargs}
                )
                res.status_code = 400
            except StatementError as e:
                res = jsonify(
                    {"error": "SQL Error", "error_message": e, "kwargs": kwargs}
                )
                res.status_code = 400
        elif request.method == "PUT":
            res = put(*args, **kwargs)
        elif request.method == "DELETE":
            res = delete(*args, **kwargs)
        elif request.method == "PATCH":
            res = patch(*args, **kwargs)
    except Exception as e:
        import traceback

        res = jsonify(
            {
                "error": "Unknown Error",
                "error_type": type(e),
                "error_message": f"{e}\n{traceback.format_exc()}",
                "kwargs": kwargs,
            }
        )
        res.status_code = 500
    # noinspection PyUnboundLocalVariable
    return res


class StarshipCompatabilityLayer:
    """
    - 1.8 https://github.com/apache/airflow/blob/1.8.2/airflow/models.py
    - 1.10 https://github.com/apache/airflow/blob/1.10.15/airflow/models
    - 2.0 https://github.com/apache/airflow/tree/2.0.2/airflow/models
    - 2.1 https://github.com/apache/airflow/tree/2.1.4/airflow/models
    - 2.2 https://github.com/apache/airflow/tree/2.2.5/airflow/models
    - 2.3 https://github.com/apache/airflow/blob/2.3.4/airflow/models
    - 2.4 https://github.com/apache/airflow/blob/2.4.3/airflow/models
    """

    def __new__(cls):
        [major, _, _] = version.split(".", maxsplit=2)
        if major == "2":
            # if minor >= 3:
            #     return StarshipAirflow2_3()
            # else:
            return StarshipAirflow()
        else:
            raise RuntimeError(f"Unsupported Airflow Version: {version}")


def get_required(_request, key, is_json: bool = True) -> Any:
    if is_json and key in request.json:
        return request.json[key]
    elif not is_json and key in request.args:
        return request.args[key]
    else:
        raise RuntimeError(f"Missing required key: {key}")


def get_optional(_request, key, is_json: bool = True) -> Any:
    if is_json and key in request.json:
        return request.json.get(key, None)
    elif not is_json and key in request.args:
        return request.args.get(key, None)
    else:
        return None


class StarshipApi(BaseView):
    route_base = "/api/starship"
    default_view = "health"

    @expose("/health", methods=["GET"])
    @csrf.exempt
    def health(self):
        def ok():
            return "OK"

        return starship_route(get=ok)

    @expose("/airflow_version", methods=["GET"])
    @csrf.exempt
    def airflow_version(self):
        """Get the Airflow Version"""
        return starship_route(get=starship_compat.get_airflow_version)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    @expose("/env_vars", methods=["GET"])
    @csrf.exempt
    def env_vars(self):
        """Get the Environment Variables"""
        return starship_route(get=starship_compat.get_env_vars)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
    @expose("/pools", methods=["GET", "POST"])
    @csrf.exempt
    def pools(self):
        """Get Pools or set a Pool"""

        def kwargs_fn() -> dict:
            return (
                {
                    "name": get_required(request, "name"),
                    "slots": get_required(request, "slots"),
                    "description": get_optional(request, "description"),
                }
                if request.method == "POST"
                else {}
            )

        return starship_route(
            get=starship_compat.get_pools,
            post=starship_compat.set_pool,
            kwargs_fn=kwargs_fn,
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    @expose("/variables", methods=["GET", "POST"])
    @csrf.exempt
    def variables(self):
        """Get Variables or set a Variable"""

        def kwargs_fn() -> dict:
            return (
                {
                    "key": get_required(request, "key"),
                    "val": get_required(request, "val"),
                    "description": get_optional(request, "description"),
                }
                if request.method == "POST"
                else {}
            )

        return starship_route(
            get=starship_compat.get_variables,
            post=starship_compat.set_variable,
            kwargs_fn=kwargs_fn,
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    @expose("/connections", methods=["GET", "POST"])
    @csrf.exempt
    def connections(self):
        """Get Connections or set a Connection"""

        def kwargs_fn() -> dict:
            return (
                {
                    "conn_id": get_required(request, "conn_id"),
                    "conn_type": get_required(request, "conn_type"),
                    "host": get_optional(request, "host"),
                    "port": get_optional(request, "port"),
                    "schema": get_optional(request, "schema"),
                    "login": get_optional(request, "login"),
                    "password": get_optional(request, "password"),
                    "extra": get_optional(request, "extra"),
                    "description": get_optional(request, "description"),
                }
                if request.method == "POST"
                else {}
            )

        return starship_route(
            get=starship_compat.get_connections,
            post=starship_compat.set_connection,
            kwargs_fn=kwargs_fn,
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    @expose("/dags", methods=["GET", "PATCH"])
    @csrf.exempt
    def dags(self):
        def kwargs_fn() -> dict:
            return (
                {
                    "dag_id": get_required(request, "dag_id"),
                    "is_paused": get_required(request, "is_paused"),
                }
                if request.method == "PATCH"
                else {}
            )

        return starship_route(
            get=starship_compat.get_dags,
            patch=starship_compat.set_dag_is_paused,
            kwargs_fn=kwargs_fn,
        )

    @staticmethod
    def _get_dag_id_limit_offset(kwargs):
        if request.method == "GET":
            kwargs["dag_id"] = get_required(request, "dag_id", is_json=False)

            # Limit is the number of rows to return.
            kwargs["limit"] = get_optional(request, "limit", is_json=False)

            # Offset is the number of rows in the result set to skip before beginning to return rows.
            kwargs["offset"] = get_optional(request, "offset", is_json=False)
        return kwargs

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)])
    @expose("/dag_runs", methods=["GET", "POST"])
    @csrf.exempt
    def dag_runs(self):
        def kwargs_fn() -> dict:
            kwargs = {}
            self._get_dag_id_limit_offset(kwargs)
            if request.method == "POST":
                kwargs["dag_runs"] = get_required(request, "dag_runs")
            return kwargs

        return starship_route(
            get=starship_compat.get_dag_runs,
            post=starship_compat.set_dag_runs,
            kwargs_fn=kwargs_fn,
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)])
    @expose("/task_instances", methods=["GET", "POST"])
    @csrf.exempt
    def task_instances(self):
        def kwargs_fn() -> dict:
            kwargs = {}
            self._get_dag_id_limit_offset(kwargs)
            if request.method == "POST":
                kwargs["task_instances"] = get_required(request, "task_instances")
            return kwargs

        return starship_route(
            get=starship_compat.get_task_instances,
            post=starship_compat.set_task_instances,
            kwargs_fn=kwargs_fn,
        )


class StarshipAirflowSpec(ABC):
    @abstractmethod
    def get_airflow_version(self):
        raise NotImplementedError()

    @abstractmethod
    def get_env_vars(self):
        raise NotImplementedError()

    def set_env_vars(self):
        """This is set directly at the Astro API, so return an error"""
        res = jsonify({"error": "Set via the Astro/Houston API"})
        res.status_code = 409
        raise NotImplementedError()

    @abstractmethod
    def get_pools(self):
        raise NotImplementedError()

    @abstractmethod
    def set_pool(self, name: str, slots: int, description=""):
        raise NotImplementedError()

    @abstractmethod
    def get_variables(self):
        raise NotImplementedError()

    @abstractmethod
    def set_variable(self, key, val, description):
        raise NotImplementedError()

    @abstractmethod
    def get_dags(self):
        raise NotImplementedError()

    @abstractmethod
    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        raise NotImplementedError()

    @abstractmethod
    def get_dag_runs(self, dag_id: str, limit: int = 10):
        raise NotImplementedError()

    @abstractmethod
    def set_dag_runs(self, dag_runs: list):
        raise NotImplementedError()

    @abstractmethod
    def get_task_instances(self, dag_id: str, limit: int = 10):
        raise NotImplementedError()

    @abstractmethod
    def set_task_instances(self, task_instances: list):
        raise NotImplementedError()


class StarshipAirflow(StarshipAirflowSpec):
    """Base Class
    Contains methods that are expected to work across all Airflow versions
    When older versions require different behavior, they'll override this class
    and get created directly by StarshipCompatabilityLayer
    """

    def __init__(self):
        from airflow.settings import Session

        self.session = Session()

    # noinspection PyMethodMayBeStatic
    def get_env_vars(self):
        return dict(os.environ)

    # noinspection PyMethodMayBeStatic
    def get_airflow_version(self):
        return version

    def get_variables(self):
        from airflow.models import Variable

        variables = self.session.query(Variable).all()
        return [
            {
                "key": variable.key,
                "val": variable.val,
                "description": variable.description
                if getattr(variable, "description")
                else "",
            }
            for variable in variables
        ]

    def set_variable(self, key, val, description):
        from airflow.models import Variable

        try:
            variable = Variable(
                key=key,
                val=val,
                **({"description": description} if description else {}),
            )
            self.session.add(variable)
            self.session.commit()
            return {
                "key": variable.key,
                "val": variable.val,
                "description": variable.description
                if getattr(variable, "description")
                else "",
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def get_pools(self):
        """
        Get all pools, only load the pool name, slots, and description columns
        """
        from airflow.models import Pool
        from sqlalchemy.orm import load_only

        pools = (
            self.session.query(Pool)
            .options(load_only("pool", "slots", "description"))
            .all()
        )
        return [
            {
                "name": pool.pool,
                "slots": pool.slots,
                "description": pool.description,
            }
            for pool in pools
        ]

    def set_pool(self, name: str, slots: int, description=""):
        """Set name, slots, and description for a pool"""
        from airflow.models import Pool

        try:
            pool = Pool(
                pool=name,
                slots=slots,
                **({"description": description} if description else {}),
            )
            self.session.add(pool)
            self.session.commit()
            return {
                "name": pool.pool,
                "slots": pool.slots,
                "description": pool.description,
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def get_connections(self):
        from airflow.models import Connection

        connections = self.session.query(Connection).all()
        return [
            {
                "conn_id": connection.conn_id,
                "conn_type": connection.conn_type,
                "host": connection.host,
                "port": connection.port,
                "schema": connection.schema,
                "login": connection.login,
                "password": connection.password,
                "extra": connection.extra,
                "description": connection.description,
            }
            for connection in connections
        ]

    def set_connection(
        self,
        conn_id,
        conn_type,
        host,
        port,
        schema,
        login,
        password,
        extra,
        description,
    ):
        from airflow.models import Connection

        try:
            connection = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                port=port,
                schema=schema,
                login=login,
                password=password,
                extra=extra,
                description=description,
            )
            self.session.add(connection)
            self.session.commit()
            return {
                "conn_id": connection.conn_id,
                "conn_type": connection.conn_type,
                "host": connection.host,
                "port": connection.port,
                "schema": connection.schema,
                "login": connection.login,
                "password": connection.password,
                "extra": connection.extra,
                "description": connection.description,
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def get_dags(self):
        """Get all DAGs"""
        from airflow.models import DagModel, TaskInstance, DagRun
        from sqlalchemy.sql.functions import count
        from sqlalchemy import distinct

        try:
            dags = (
                self.session.query(
                    DagModel,
                    count(distinct(TaskInstance.task_id)).label("task_count"),
                    count(DagRun.run_id).label("dag_run_count"),
                )
                .join(
                    TaskInstance, TaskInstance.dag_id == DagModel.dag_id, isouter=True
                )
                .join(DagRun, DagRun.dag_id == DagModel.dag_id, isouter=True)
                .group_by(DagModel)
                .all()
            )
            return [
                {
                    "dag_id": dag.DagModel.dag_id,
                    "schedule_interval": dag.DagModel.schedule_interval,
                    "is_paused": dag.DagModel.is_paused,
                    "fileloc": dag.DagModel.fileloc,
                    "description": dag.DagModel.description,
                    "owners": dag.DagModel.owners,
                    "tags": dag.DagModel.tags,
                    "dag_run_count": dag.dag_run_count,
                    "task_count": dag.task_count,
                }
                for dag in dags
            ]
        except Exception as e:
            self.session.rollback()
            raise e

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        """Pause or unpause a DAG"""
        from airflow.models import DagModel
        from sqlalchemy import update

        try:
            self.session.execute(
                update(DagModel)
                .where(DagModel.dag_id == dag_id)
                .values(is_paused=is_paused)
            )
            self.session.commit()
            return {
                "dag_id": dag_id,
                "is_paused": is_paused,
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def _get_stats(self, dag_id: str):
        from airflow.models import DagRun
        from sqlalchemy.sql.functions import count
        from sqlalchemy import distinct

        try:
            return {
                "dag_run_count": self.session.query(
                    count(distinct(DagRun.run_id)).label("dag_run_count")
                )
                .where(DagRun.dag_id == dag_id)
                .one()["dag_run_count"],
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10):
        from sqlalchemy import desc
        from airflow.models import DagRun

        try:
            query = (
                self.session.query(DagRun)
                .filter(DagRun.dag_id == dag_id)
                .order_by(desc(DagRun.start_date))
            )
            if offset:
                query = query.offset(offset)
            dag_runs = query.limit(limit).all()
            dag_run_count = self._get_stats(dag_id)
            return {
                "dag_runs": [
                    {
                        "dag_id": dag_run.dag_id,
                        "queued_at": dag_run.queued_at,
                        "execution_date": dag_run.execution_date,
                        "start_date": dag_run.start_date,
                        "end_date": dag_run.end_date,
                        "state": dag_run.state,
                        "run_id": dag_run.run_id,
                        "creating_job_id": dag_run.creating_job_id,
                        "external_trigger": dag_run.external_trigger,
                        "run_type": dag_run.run_type,
                        "conf": dag_run.conf,
                        "data_interval_start": dag_run.data_interval_start,
                        "data_interval_end": dag_run.data_interval_end,
                        "last_scheduling_decision": dag_run.last_scheduling_decision,
                        "dag_hash": dag_run.dag_hash,
                    }
                    for dag_run in dag_runs
                ],
                "dag_run_count": dag_run_count["dag_run_count"],
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def set_dag_runs(self, dag_runs: list):
        dag_runs = self.insert_directly("dag_run", dag_runs)
        stats = self._get_stats(dag_runs[0]["dag_id"])
        return {"dag_runs": dag_runs, "dag_run_count": stats["dag_run_count"]}

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        from sqlalchemy import desc
        from airflow.models import DagRun, TaskInstance
        from sqlalchemy.orm import load_only

        try:
            sub_query = (
                self.session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id)
                .order_by(desc(DagRun.start_date))
                .limit(limit)
            )
            if offset:
                sub_query = sub_query.offset(offset)
            sub_query = sub_query.subquery()
            task_instances = (
                self.session.query(TaskInstance)
                .filter(DagRun.dag_id == dag_id)
                .filter(TaskInstance.run_id.in_(sub_query))
                .options(
                    load_only(
                        "dag_id",
                        "run_id",
                        "task_id",
                        "start_date",
                        "end_date",
                        "duration",
                        "state",
                        "max_tries",
                        "hostname",
                        "unixname",
                        "job_id",
                        "pool",
                        "pool_slots",
                        "queue",
                        "priority_weight",
                        "operator",
                        "queued_dttm",
                        "queued_by_job_id",
                        "pid",
                        # "executor_config",
                        "external_executor_id",
                        "trigger_id",
                        "trigger_timeout",
                        # "next_method",
                        # "next_kwargs",
                    )
                )
                .order_by(desc(TaskInstance.start_date))
                .limit(limit)
                .all()
            )
            dag_run_count = self._get_stats(dag_id)
            return {
                "task_instances": [
                    {
                        "dag_id": task_instance.dag_id,
                        "run_id": task_instance.run_id,
                        "task_id": task_instance.task_id,
                        "start_date": task_instance.start_date,
                        "end_date": task_instance.end_date,
                        "duration": task_instance.duration,
                        "state": task_instance.state,
                        "max_tries": task_instance.max_tries,
                        "hostname": task_instance.hostname,
                        "unixname": task_instance.unixname,
                        "job_id": task_instance.job_id,
                        "pool": task_instance.pool,
                        "pool_slots": task_instance.pool_slots,
                        "queue": task_instance.queue,
                        "priority_weight": task_instance.priority_weight,
                        "operator": task_instance.operator,
                        "queued_dttm": task_instance.queued_dttm,
                        "queued_by_job_id": task_instance.queued_by_job_id,
                        "pid": task_instance.pid,
                        # "executor_config": None,
                        "external_executor_id": task_instance.external_executor_id,
                        "trigger_id": task_instance.trigger_id,
                        "trigger_timeout": task_instance.trigger_timeout,
                        # Exception:
                        # /airflow/serialization/serialized_objects.py\", line 521, in deserialize
                        # KeyError: <Encoding.VAR: '__var'>
                        # "next_method": task_instance.next_method,
                        # "next_kwargs": task_instance.next_kwargs,
                    }
                    for task_instance in task_instances
                ],
                "dag_run_count": dag_run_count["dag_run_count"],
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def set_task_instances(self, task_instances: list):
        """These need to be inserted directly to skip TaskInstance.__init__"""
        task_instances = self.insert_directly("task_instance", task_instances)
        return {"task_instances": task_instances}

    def insert_directly(self, table_name, items):
        from sqlalchemy.exc import InvalidRequestError
        from sqlalchemy import MetaData
        import pickle

        if not items:
            return []

        # Clean data before inserting
        for item in items:
            # Dropping conf and executor_config because they are pickle objects
            for k in ["conf", "id", "executor_config"]:
                if k in item:
                    if k == "executor_config":
                        item[k] = pickle.dumps({})
                    else:
                        del item[k]
        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=[table_name])
            table = metadata.tables[table_name]
            self.session.execute(table.insert().values(items))
            self.session.commit()
            return items
        except (InvalidRequestError, KeyError):
            return self.insert_directly(f"airflow.{table_name}", items)
        except Exception as e:
            self.session.rollback()
            raise e


starship_compat = StarshipCompatabilityLayer()

starship_api_view = StarshipApi()
starship_api_bp = Blueprint(
    "starship_api",
    __name__,
)


class StarshipAPIPlugin(AirflowPlugin):
    name = "starship_api"
    flask_blueprints = [starship_api_bp]
    appbuilder_views = [
        {
            "view": starship_api_view,
        }
    ]
