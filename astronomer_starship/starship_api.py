from __future__ import annotations

import os
from abc import ABC
from airflow.models import DagRun
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.version import version
from airflow.www import auth
from airflow.www.app import csrf
from flask import Blueprint, request, jsonify
from flask_appbuilder import expose, BaseView
from sqlalchemy import distinct


def starship_route(
    get=None, post=None, put=None, delete=None, patch=None, *args, **kwargs
):
    if request.method == "GET":
        res = get(*args, **kwargs)
    elif request.method == "POST":
        from sqlalchemy.exc import IntegrityError

        try:
            res = post(*args, **kwargs)
        except IntegrityError:
            res = jsonify({"error": "Duplicate Record"})
            res.status_code = 409
    elif request.method == "PUT":
        res = put(*args, **kwargs)
    elif request.method == "DELETE":
        res = delete(*args, **kwargs)
    elif request.method == "PATCH":
        res = patch(*args, **kwargs)
    else:
        raise NotImplementedError()
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

    @expose("/env_vars", methods=["GET"])
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def env_vars(self):
        """Get the Environment Variables"""
        return starship_route(
            get=starship_compat.get_env_vars,
        )

    @expose("/pools", methods=["GET", "POST"])
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
    def pools(self):
        """Get Pools or set a Pool"""
        return starship_route(
            get=starship_compat.get_pools,
            post=starship_compat.set_pool,
            **{
                "name": request.json["name"],
                "slots": request.json["slots"],
                "description": request.json.get("description", None),
            }
            if request.method == "POST"
            else {},
        )

    @expose("/variables", methods=["GET", "POST"])
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def variables(self):
        """Get Variables or set a Variable"""
        return starship_route(
            get=starship_compat.get_variables,
            post=starship_compat.set_variable,
            **{
                "key": request.json["key"],
                "val": request.json["val"],
                "description": request.json.get("description", None),
            }
            if request.method == "POST"
            else {},
        )

    @expose("/connections", methods=["GET", "POST"])
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def connections(self):
        """Get Connections or set a Connection"""
        return starship_route(
            get=starship_compat.get_connections,
            post=starship_compat.set_connection,
            **{
                "conn_id": request.json["conn_id"],
                "conn_type": request.json["conn_type"],
                "host": request.json.get("host"),
                "port": request.json.get("port"),
                "schema": request.json.get("schema"),
                "login": request.json.get("login"),
                "password": request.json.get("password"),
                "extra": request.json.get("extra"),
                "description": request.json.get("description"),
            }
            if request.method == "POST"
            else {},
        )

    @expose("/dags", methods=["GET", "PATCH"])
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def dags(self):
        return starship_route(
            get=starship_compat.get_dags,
            patch=starship_compat.set_dag_is_paused,
            **{
                "dag_id": request.json["dag_id"],
                "is_paused": request.json["is_paused"],
            }
            if request.method == "PATCH"
            else {},
        )


class StarshipAirflowSpec(ABC):
    def get_airflow_version(self):
        raise NotImplementedError()

    def get_env_vars(self):
        raise NotImplementedError()

    def set_env_vars(self):
        raise NotImplementedError()

    def get_pools(self):
        raise NotImplementedError()

    def set_pools(self):
        raise NotImplementedError()

    def get_variables(self):
        raise NotImplementedError()

    def set_variable(self):
        raise NotImplementedError()

    def get_dags(self):
        raise NotImplementedError()

    def set_dag_is_paused(self):
        raise NotImplementedError()

    def get_dag_runs(self):
        raise NotImplementedError()

    def set_dag_run(self):
        raise NotImplementedError()


class StarshipAirflow:
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

    def set_env_vars(self):
        """This is set directly at the Astro API, so return an error"""
        res = jsonify({"error": "Set via the Astro/Houston API"})
        res.status_code = 409
        raise NotImplementedError()

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
            self.session.add(
                Pool(
                    pool=name,
                    slots=slots,
                    **({"description": description} if description else {}),
                )
            )
            self.session.commit()
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

        try:
            dags = (
                self.session.query(
                    DagModel,
                    count(distinct(TaskInstance.task_id)).label("task_count"),
                    count(DagRun.run_id).label("dag_run_count"),
                )
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
        except Exception as e:
            self.session.rollback()
            raise e

    def get_dag_runs(self, dag_id: str, limit: int = 10):
        from airflow.models import DagRun

        try:
            dag_runs = self.session.query(DagRun).limit(limit).all()
            return [
                {
                    "dag_id": dag_id,
                    "run_id": dag_run["run_id"],
                    "queued_at": dag_run["queued_at"],
                    "execution_date": dag_run["execution_date"],
                    "start_date": dag_run["start_date"],
                    "external_trigger": dag_run["external_trigger"],
                    "conf": dag_run["conf"],
                    "state": dag_run["state"],
                    "run_type": dag_run["run_type"],
                    "dag_hash": dag_run["dag_hash"],
                    "creating_job_id": dag_run["creating_job_id"],
                    "data_interval": dag_run["data_interval"],
                }
                for dag_run in dag_runs
            ]
        except Exception as e:
            self.session.rollback()
            raise e

    def set_dag_runs(self, dag_id: str, dag_runs: list):
        try:
            self.session.bulk_save_objects(
                [
                    DagRun(
                        dag_id=dag_id,
                        run_id=dag_run["run_id"],
                        queued_at=dag_run["queued_at"],
                        execution_date=dag_run["execution_date"],
                        start_date=dag_run["start_date"],
                        external_trigger=dag_run["external_trigger"],
                        conf=dag_run["conf"],
                        state=dag_run["state"],
                        run_type=dag_run["run_type"],
                        dag_hash=dag_run["dag_hash"],
                        creating_job_id=dag_run["creating_job_id"],
                        data_interval=dag_run["data_interval"],
                    )
                    for dag_run in dag_runs
                ]
            )
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def get_task_instances(self, dag_id: str, dag_run_ids: list):
        pass

    def set_task_instances(self, dag_id: str, task_instances: list):
        try:
            pass
            # from airflow.models import TaskInstance
            # self.session.bulk_save_objects([
            #     TaskInstance(
            #         dag_id=dag_id,
            #         run_id=task_instance["run_id"],
            #         task_id=task_instance["task_id"],
            #         start_date=task_instance["start_date"],
            #         end_date=task_instance["end_date"],
            #         duration=task_instance["duration"],
            #         state=task_instance["state"],
            #         max_tries=task_instance["max_tries"],
            #         hostname=task_instance["hostname"],
            #         unixname=task_instance["unixname"],
            #         job_id=task_instance["job_id"],
            #         pool=task_instance["pool"],
            #         pool_slots=task_instance["pool_slots"],
            #         queue=task_instance["queue"],
            #         priority_weight=task_instance["priority_weight"],
            #         operator=task_instance["operator"],
            #         queued_dttm=task_instance["queued_dttm"],
            #         queued_by_job_id=task_instance["queued_by_job_id"],
            #         pid=task_instance["pid"],
            #         executor_config=task_instance["executor_config"],
            #         external_executor_id=task_instance["external_executor_id"],
            #         trigger_id=task_instance["trigger_id"],
            #         trigger_timeout=task_instance["trigger_timeout"],
            #         next_method=task_instance["next_method"],
            #         next_kwargs=task_instance["next_kwargs"],
            #     ) for task_instance in task_instances
            # ])
            # self.session.commit()

            # if data is None:
            #     logging.warning(f"Received no data! data {data}")
            #     return
            #
            # engine = session.get_bind()
            # metadata_obj = MetaData(bind=engine)
            #
            # task_instances = [datum for datum in data if datum["table"] == "task_instance"]
            # dag_runs = [datum for datum in data if datum["table"] == "dag_run"]
            #
            # others = [
            #     datum for datum in data if datum["table"] not in ("task_instance", "dag_run")
            # ]
            # if len(others):
            #     logging.warning(f"Received unexpected records! {others} - skipping!")
            #
            # for data_list, table_name in (
            #         (dag_runs, "dag_run"),
            #         (task_instances, "task_instance"),
            # ):
            #     if not data_list:
            #         continue
            #     logging.debug("Removing keys that could cause issues...")
            #     for datum in data_list:
            #         # Dropping conf and executor_config because they are pickle objects
            #         # I can't figure out how to send them
            #         for k in ["conf", "id", "table", "conf", "executor_config"]:
            #             if k in datum:
            #                 if k == "executor_config":
            #                     datum[k] = pickle.dumps({})
            #                 else:
            #                     del datum[k]
            #
            #     try:
            #         table = Table(f"airflow.{table_name}", metadata_obj, autoload_with=engine)
            #     except NoSuchTableError:
            #         table = Table(table_name, metadata_obj, autoload_with=engine)
            #
            #     with engine.begin() as txn:
            #         txn.execute(insert(table).on_conflict_do_nothing(), data_list)

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
