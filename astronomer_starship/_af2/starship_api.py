import json
from functools import partial
from http import HTTPStatus
from typing import TYPE_CHECKING

import flask
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from flask import Blueprint, Response, jsonify, request
from flask_appbuilder import BaseView, expose

from astronomer_starship._af2.starship_compatability import (
    StarshipCompatabilityLayer,
)
from astronomer_starship.common import HttpError, get_kwargs_fn, telescope

if TYPE_CHECKING:
    from typing import Callable


def starship_route(  # noqa: C901
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
                args=(request.args if request_method in ["GET", "POST", "DELETE"] else {}),
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
            from sqlalchemy.exc import DataError, IntegrityError, StatementError

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
                res = jsonify({"error": "Data Error", "error_message": e, "kwargs": kwargs})
                res.status_code = 400
            except StatementError as e:
                res = jsonify({"error": "SQL Error", "error_message": e, "kwargs": kwargs})
                res.status_code = 400
        elif request.method == "PUT":
            res = put(**kwargs)
        elif request.method == "DELETE":
            res = delete(**kwargs)
        elif request.method == "PATCH":
            res = patch(**kwargs)

        if res is None:
            res = Response(status=HTTPStatus.NO_CONTENT)
    except HttpError as e:
        res = jsonify({"error": e.msg})
        res.status_code = e.status_code
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

    # https://github.com/pallets/flask/issues/4659
    # noinspection PyUnboundLocalVariable
    return jsonify(res) if flask.__version__ < "2.2" and isinstance(res, list) else res


class StarshipApi(BaseView):
    route_base = "/api/starship"
    default_view = "health"

    @expose("/health", methods=["GET"])
    @csrf.exempt
    def health(self) -> str:
        def ok():
            return "OK"

        return starship_route(get=ok)

    @expose("/telescope", methods=["GET"])
    @csrf.exempt
    def telescope(self):
        return telescope(
            organization=request.args["organization"],
            presigned_url=request.args.get("presigned_url", None),
        )

    @expose("/airflow_version", methods=["GET"])
    @csrf.exempt
    def airflow_version(self) -> str:
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(get=starship_compat.get_airflow_version)

    @expose("/info", methods=["GET"])
    @csrf.exempt
    def info(self) -> str:
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(get=starship_compat.get_info)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    @expose("/env_vars", methods=["GET"])
    @csrf.exempt
    def env_vars(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(get=starship_compat.get_env_vars)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
    @expose("/pools", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def pools(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_pools,
            post=starship_compat.set_pool,
            delete=starship_compat.delete_pool,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.pool_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    @expose("/variables", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def variables(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_variables,
            post=starship_compat.set_variable,
            delete=starship_compat.delete_variable,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.variable_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    @expose("/connections", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def connections(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_connections,
            post=starship_compat.set_connection,
            delete=starship_compat.delete_connection,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.connection_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    @expose("/dags", methods=["GET", "PATCH"])
    @csrf.exempt
    def dags(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_dags,
            patch=starship_compat.set_dag_is_paused,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)])
    @expose("/dag_runs", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def dag_runs(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_dag_runs,
            post=starship_compat.set_dag_runs,
            delete=starship_compat.delete_dag_runs,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_runs_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)])
    @expose("/task_instances", methods=["GET", "POST"])
    @csrf.exempt
    def task_instances(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_task_instances,
            post=starship_compat.set_task_instances,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instances_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE_HISTORY)])
    @expose("/task_instance_history", methods=["GET", "POST"])
    @csrf.exempt
    def task_instance_history(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_task_instance_history,
            post=starship_compat.set_task_instance_history,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instances_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)])
    @expose("/task_log", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def task_logs(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_task_log,
            post=starship_compat.set_task_log,
            delete=starship_compat.delete_task_log,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_log_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)])
    @expose("/xcom", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def xcom(self):
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_xcom,
            post=starship_compat.set_xcom,
            delete=starship_compat.delete_xcom,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.xcom_attrs()),
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

    @classmethod
    def on_load(cls, *args, **kwargs):
        # Initialize compatibility layer on plugin load to ensure it loads fine.
        # If not, a runtime error will be raised, disabling the plugin.
        StarshipCompatabilityLayer()
