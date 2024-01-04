from __future__ import annotations
import os
from abc import ABC
from airflow.plugins_manager import AirflowPlugin
from airflow.version import version
from airflow.www.app import csrf
from flask import Blueprint, request, jsonify
from flask_appbuilder import expose, BaseView
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only

from airflow.security import permissions
from airflow.www import auth


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
        [major, minor, patch] = version.split(".", maxsplit=2)
        if major == "2":
            # if minor >= 3:
            #     return StarshipAirflow2_3()
            # else:
            return StarshipAirflow()
        else:
            raise RuntimeError(f"Unsupported Airflow Version: {version}")


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

    def set_dag(self):
        raise NotImplementedError()


# noinspection PyMethodMayBeStatic
class StarshipAirflow:
    """Base Class
    Contains methods that are expected to work across all Airflow versions
    When older versions require different behavior, they'll override this class
    and get created directly by StarshipCompatabilityLayer
    """

    def __init__(self):
        from airflow.settings import Session

        self.session = Session()

    def get_env_vars(self):
        return dict(os.environ)

    def set_env_vars(self):
        """This is set directly at the Astro API, so return an error"""
        res = jsonify({"error": "Set via the Astro/Houston API"})
        res.status_code = 409
        raise NotImplementedError()

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
            self.session.add(
                Variable(
                    key=key,
                    val=val,
                    **({"description": description} if description else {}),
                )
            )
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def get_pools(self):
        """
        Get all pools, only load the pool name, slots, and description columns
        """
        from airflow.models import Pool

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
            self.session.add(
                Connection(
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
            )
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def get_dags(self, num_dag_runs=5):
        """
        Get all DAGs and some DAG Runs + Task Instances
        """
        from airflow.models import DAGModel

        self.session.query(DAGModel).options(load_only("dag_id", "owner", "tags")).all()
        # return [{
        #     "name": pool.pool,
        #     "slots": pool.slots,
        #     "description": pool.description,
        # } for pool in pools]

    def set_dag(self):
        pass
        # """Set name, slots, and description for a pool"""
        # from airflow.models import Pool
        # try:
        #     self.session.add(
        #         Pool(
        #             pool=name,
        #             slots=slots,
        #             **({"description": description} if description else {})
        #         )
        #     )
        #     self.session.commit()
        # except Exception as e:
        #     self.session.rollback()
        #     raise e


starship_compat = StarshipCompatabilityLayer()


def starship_route(
    get=None, post=None, put=None, delete=None, patch=None, *args, **kwargs
):
    if request.method == "GET":
        return get(*args, **kwargs)
    elif request.method == "POST":
        try:
            return post(*args, **kwargs)
        except IntegrityError:
            res = jsonify({"error": "Duplicate Record"})
            res.status_code = 409
            return res
    elif request.method == "PUT":
        return put(*args, **kwargs)
    elif request.method == "DELETE":
        return delete(*args, **kwargs)
    elif request.method == "PATCH":
        return patch(*args, **kwargs)
    else:
        raise NotImplementedError()


class StarshipApi(BaseView):
    route_base = "/api/starship"
    default_view = "airflow_version"

    @expose("/health", methods=["GET"])
    @csrf.exempt
    def health(self):
        return "OK"

    @expose("/airflow_version", methods=["GET"])
    @csrf.exempt
    def airflow_version(self):
        """Get the Airflow Version"""
        return starship_route(
            get=starship_compat.get_airflow_version,
        )

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

    @expose("/dags", methods=["GET", "POST"])
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def dags(self):
        return starship_route(
            get=starship_compat.get_dags,
            post=starship_compat.set_dag,
        )


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
