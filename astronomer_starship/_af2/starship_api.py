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
from astronomer_starship.common import (
    HttpError,
    NotFoundError,
    build_source_connection_kwargs,
    get_kwargs_fn,
    normalize_source_conn_id,
    telescope,
)

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
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instance_history_attrs()),
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

    @expose("/cutover/waves", methods=["GET", "POST"])
    @csrf.exempt
    def cutover_waves(self):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _get():
            limit = int(request.args.get("limit", 25))
            return _cutover.list_migrations(limit=limit)

        def _post():
            body = request.json or {}
            strategy = body.get("strategy", "incremental")
            patterns = body.get("patterns", [])
            try:
                return _cutover.start_wave(strategy=strategy, patterns=patterns, config=body.get("config"))
            except ValueError as e:
                raise HttpError(str(e), 400) from e
            except RuntimeError as e:
                # Misconfigured source connection surfaces as RuntimeError
                # from the auth factory. That's user-actionable, not a 500.
                raise HttpError(str(e), 400) from e

        return starship_route(get=_get, post=_post)

    @expose("/cutover/waves/<migration_id>", methods=["GET"])
    @csrf.exempt
    def cutover_wave_detail(self, migration_id):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _get():
            migration = _cutover.enrich_wave_for_display(migration_id)
            if migration is None:
                raise HttpError(f"Wave '{migration_id}' not found", 404)
            return migration

        return starship_route(get=_get)

    @expose("/cutover/waves/<migration_id>/abort", methods=["POST"])
    @csrf.exempt
    def cutover_wave_abort(self, migration_id):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            _cutover.request_abort(migration_id)
            return {"migration_id": migration_id, "status": "abort_requested"}

        return starship_route(post=_post)

    @expose("/cutover/waves/<migration_id>/rollback", methods=["POST"])
    @csrf.exempt
    def cutover_wave_rollback(self, migration_id):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            body = request.json or {}
            dag_id = body.get("dag_id")
            if dag_id:
                _cutover.rollback_dag(migration_id=migration_id, dag_id=dag_id)
                return {"migration_id": migration_id, "dag_id": dag_id, "rolled_back": True}
            _cutover.rollback_migration(migration_id)
            return {"migration_id": migration_id, "rolled_back": True}

        return starship_route(post=_post)

    @expose("/cutover/waves/<migration_id>/retry", methods=["POST"])
    @csrf.exempt
    def cutover_wave_retry(self, migration_id):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            body = request.json or {}
            # Selector is either 'failed' / 'skipped' / a specific dag_id.
            selector = body.get("selector") or body.get("dag_id") or "failed"
            try:
                dag_ids = _cutover.retry_dags_in_wave(migration_id, selector)
            except ValueError as e:
                raise HttpError(str(e), 400) from e
            return {"migration_id": migration_id, "retry_dag_ids": dag_ids}

        return starship_route(post=_post)

    @expose("/cutover/waves/<migration_id>/purge", methods=["POST"])
    @csrf.exempt
    def cutover_wave_purge(self, migration_id):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            body = request.json or {}
            dag_id = body.get("dag_id")
            if dag_id:
                deleted = _cutover.purge_dag_metadata(dag_id=dag_id)
                return {"migration_id": migration_id, "dag_id": dag_id, "runs_deleted": deleted}
            return _cutover.purge_wave_metadata(migration_id)

        return starship_route(post=_post)

    @expose("/cutover/purge_all", methods=["POST"])
    @csrf.exempt
    def cutover_purge_all(self):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            return _cutover.purge_all_instance_dag_metadata()

        return starship_route(post=_post)

    @expose("/source_connection", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def source_connection(self):
        """Manage a source-Airflow Connection used by the Cutover Tool.

        The frontend POSTs a platform-specific payload including the
        desired ``conn_id`` (default ``starship_source``); we translate it
        to a standard Airflow HTTP Connection so the operator/template DAG
        and the wave engine can reuse it unchanged. GET and DELETE accept
        ``?conn_id=`` to operate on a specific connection.
        """
        starship_compat = StarshipCompatabilityLayer()

        def _get():
            conn_id = normalize_source_conn_id(request.args.get("conn_id"))
            conn = starship_compat.get_source_connection(conn_id=conn_id)
            if conn is None:
                raise NotFoundError(f"No source connection configured for conn_id={conn_id!r}")
            return conn

        def _post():
            payload = request.json or {}
            kwargs = build_source_connection_kwargs(payload)
            conn_id = kwargs["conn_id"]
            existed = starship_compat.source_connection_exists(conn_id)
            if existed:
                starship_compat.delete_connection(conn_id=conn_id)
            created = starship_compat.set_connection(**kwargs)
            return {
                **created,
                "action": "updated" if existed else "created",
                "conn_id": conn_id,
            }

        def _delete():
            conn_id = normalize_source_conn_id(request.args.get("conn_id"))
            return starship_compat.delete_connection(conn_id=conn_id)

        return starship_route(get=_get, post=_post, delete=_delete)

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
