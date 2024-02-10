import json
from functools import partial
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from flask import Blueprint, request, jsonify
from flask_appbuilder import expose, BaseView

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable

from astronomer_starship.compat.starship_compatability import (
    StarshipCompatabilityLayer,
    get_kwargs_fn,
)


def starship_route(
    get=None,
    post=None,
    put=None,
    delete=None,
    patch=None,
    kwargs_fn: "Callable[[dict, dict], dict]" = None,
):
    try:
        request_method = request.method
        # noinspection PyArgumentList
        kwargs = (
            kwargs_fn(
                request_method=request_method,
                args=request.args
                if request_method in ["GET", "POST", "DELETE"]
                else {},
                json=(request.json if request.is_json else {}),
            )
            if kwargs_fn
            else {}
        )
    except RuntimeError as e:
        return jsonify({"error": e}), 400
    except Exception as e:
        return jsonify({"error": f"Unknown Error in kwargs_fn - {e}"}), 500

    if request.method not in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
        raise RuntimeError(f"Unsupported Method: {request.method}")

    try:
        if request.method == "GET":
            res = get(**kwargs)
        elif request.method == "POST":
            from sqlalchemy.exc import IntegrityError, DataError, StatementError

            try:
                res = post(**kwargs)
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
            res = put(**kwargs)
        elif request.method == "DELETE":
            res = delete(**kwargs)
        elif request.method == "PATCH":
            res = patch(**kwargs)
    except Exception as e:
        import traceback

        res = jsonify(
            {
                "error": "Unknown Error",
                "error_type": type(e),
                "error_message": f"{e}\n{traceback.format_exc()}",
                "kwargs": json.dumps(kwargs, default=str),
            }
        )
        res.status_code = 500
    # noinspection PyUnboundLocalVariable
    return res


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
        return starship_route(
            get=starship_compat.get_pools,
            post=starship_compat.set_pool,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.pool_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    @expose("/variables", methods=["GET", "POST"])
    @csrf.exempt
    def variables(self):
        """Get Variables or set a Variable"""
        return starship_route(
            get=starship_compat.get_variables,
            post=starship_compat.set_variable,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.variable_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    @expose("/connections", methods=["GET", "POST"])
    @csrf.exempt
    def connections(self):
        """Get Connections or set a Connection"""
        return starship_route(
            get=starship_compat.get_connections,
            post=starship_compat.set_connection,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.connection_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    @expose("/dags", methods=["GET", "PATCH"])
    @csrf.exempt
    def dags(self):
        return starship_route(
            get=starship_compat.get_dags,
            patch=starship_compat.set_dag_is_paused,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)])
    @expose("/dag_runs", methods=["GET", "POST"])
    @csrf.exempt
    def dag_runs(self):
        return starship_route(
            get=starship_compat.get_dag_runs,
            post=starship_compat.set_dag_runs,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_runs_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)])
    @expose("/task_instances", methods=["GET", "POST"])
    @csrf.exempt
    def task_instances(self):
        return starship_route(
            get=starship_compat.get_task_instances,
            post=starship_compat.set_task_instances,
            kwargs_fn=partial(
                get_kwargs_fn, attrs=starship_compat.task_instances_attrs()
            ),
        )


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
