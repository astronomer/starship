import json
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Annotated

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from astronomer_starship._af3.starship_compatability import StarshipAirflow, StarshipCompatabilityLayer
from astronomer_starship.common import (
    STARSHIP_SOURCE_CONN_ID,
    HttpError,
    _normalize_source_conn_id,
    build_source_connection_kwargs,
    get_kwargs_fn,
    read_source_connection,
    source_connection_exists,
    telescope,
)

if TYPE_CHECKING:
    from typing import Callable


@dataclass
class StarshipRoute:
    """StarshipRoute is prepopulated with request data for calling the corresponding endpoint."""

    method: str
    args: dict
    json: dict

    def __call__(  # noqa: C901
        self,
        get=None,
        post=None,
        put=None,
        delete=None,
        patch=None,
        kwargs_fn: "Callable[[dict, dict], dict]" = None,
    ):
        try:
            # noinspection PyArgumentList
            kwargs = (
                kwargs_fn(
                    request_method=self.method,
                    args=self.args,
                    json=self.json,
                )
                if kwargs_fn
                else {}
            )
        except RuntimeError as e:
            return JSONResponse({"error": e}, 400)
        except Exception as e:
            return JSONResponse({"error": f"Unknown Error in kwargs_fn - {e}"}, 500)

        try:
            if self.method == "GET":
                res = get(**kwargs)
            elif self.method == "POST":
                from sqlalchemy.exc import DataError, IntegrityError, StatementError

                try:
                    res = post(**kwargs)
                except IntegrityError as e:
                    return JSONResponse(
                        {
                            "error": "Integrity Error (Duplicate Record?)",
                            "error_message": str(e),
                            "kwargs": json.dumps(kwargs, default=str),
                        },
                        409,
                    )
                except DataError as e:
                    return JSONResponse(
                        {
                            "error": "Data Error",
                            "error_message": str(e),
                            "kwargs": json.dumps(kwargs, default=str),
                        },
                        400,
                    )
                except StatementError as e:
                    return JSONResponse(
                        {
                            "error": "SQL Error",
                            "error_message": str(e),
                            "kwargs": json.dumps(kwargs, default=str),
                        },
                        400,
                    )
            elif self.method == "PUT":
                res = put(**kwargs)
            elif self.method == "DELETE":
                res = delete(**kwargs)
            elif self.method == "PATCH":
                res = patch(**kwargs)
            else:
                raise RuntimeError(f"Unsupported Method: {self.method}")

            if res is None:
                return None

            return JSONResponse(res)
        except HttpError as e:
            return JSONResponse({"error": e.msg}, e.status_code)
        except Exception as e:
            import traceback

            return JSONResponse(
                {
                    "error": "Unknown Error",
                    "error_type": str(type(e)),
                    "error_message": f"{e}\n{traceback.format_exc()}",
                    "kwargs": json.dumps(kwargs, default=str),
                },
                500,
            )


async def starship_route(request: Request) -> StarshipRoute:
    """async 'dependable' to build StarshipRoute from Request"""
    body = await request.body()
    return StarshipRoute(
        method=request.method,
        args=(request.query_params if request.method in ["GET", "POST", "DELETE"] else {}),
        json=await request.json() if body else {},
    )


async def starship_compat() -> StarshipAirflow:
    """async 'dependable' to provide StarshipCompatabilityLayer"""
    return StarshipCompatabilityLayer()


router = AirflowRouter(tags=["Starship"])


class StarshipApi(FastAPI):
    def __init__(self):
        super().__init__(
            title="Starship API",
            description=("This is the Starship API"),
        )
        self.include_router(router)

    @router.get("/health")
    def health():
        return "OK"

    @router.get("/telescope")
    @staticmethod
    def telescope(organization: str, presigned_url: str | None = None):
        return telescope(
            organization=organization,
            presigned_url=presigned_url,
        )

    @router.get("/airflow_version")
    @staticmethod
    def airflow_version(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(get=starship_compat.get_airflow_version)

    @router.get("/info")
    @staticmethod
    def info(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(get=starship_compat.get_info)

    @router.get("/env_vars")
    @staticmethod
    def env_vars(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(get=starship_compat.get_env_vars)

    @router.api_route("/pools", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def pools(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_pools,
            post=starship_compat.set_pool,
            delete=starship_compat.delete_pool,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.pool_attrs()),
        )

    @router.api_route("/variables", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def variables(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_variables,
            post=starship_compat.set_variable,
            delete=starship_compat.delete_variable,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.variable_attrs()),
        )

    @router.api_route("/connections", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def connections(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_connections,
            post=starship_compat.set_connection,
            delete=starship_compat.delete_connection,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.connection_attrs()),
        )

    @router.api_route("/dags", methods=["GET", "PATCH"])
    @staticmethod
    def dags(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_dags,
            patch=starship_compat.set_dag_is_paused,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_attrs()),
        )

    @router.api_route("/dag_runs", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def dag_runs(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_dag_runs,
            post=starship_compat.set_dag_runs,
            delete=starship_compat.delete_dag_runs,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_runs_attrs()),
        )

    @router.api_route("/task_instances", methods=["GET", "POST"])
    @staticmethod
    def task_instances(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_task_instances,
            post=starship_compat.set_task_instances,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instances_attrs()),
        )

    @router.api_route("/task_instance_history", methods=["GET", "POST"])
    @staticmethod
    def task_instance_history(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_task_instance_history,
            post=starship_compat.set_task_instance_history,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instance_history_attrs()),
        )

    @router.api_route("/task_log", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def task_logs(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_task_log,
            post=starship_compat.set_task_log,
            delete=starship_compat.delete_task_log,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_log_attrs()),
        )

    @router.api_route("/cutover/waves", methods=["GET", "POST"])
    @staticmethod
    def cutover_waves(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _get():
            try:
                limit = int(starship_route.args.get("limit", 25))
            except (TypeError, ValueError):
                limit = 25
            return _cutover.list_migrations(limit=limit)

        def _post():
            body = starship_route.json or {}
            strategy = body.get("strategy", "incremental")
            patterns = body.get("patterns", [])
            try:
                return _cutover.start_wave(strategy=strategy, patterns=patterns, config=body.get("config"))
            except ValueError as e:
                raise HttpError(str(e), 400) from e
            except RuntimeError as e:
                raise HttpError(str(e), 400) from e

        return starship_route(get=_get, post=_post)

    @router.get("/cutover/waves/{migration_id}")
    @staticmethod
    def cutover_wave_detail(
        migration_id: str,
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _get():
            migration = _cutover.enrich_wave_for_display(migration_id)
            if migration is None:
                raise HttpError(f"Wave '{migration_id}' not found", 404)
            return migration

        return starship_route(get=_get)

    @router.post("/cutover/waves/{migration_id}/abort")
    @staticmethod
    def cutover_wave_abort(
        migration_id: str,
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            _cutover.request_abort(migration_id)
            return {"migration_id": migration_id, "status": "abort_requested"}

        return starship_route(post=_post)

    @router.post("/cutover/waves/{migration_id}/rollback")
    @staticmethod
    def cutover_wave_rollback(
        migration_id: str,
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            body = starship_route.json or {}
            dag_id = body.get("dag_id")
            if dag_id:
                _cutover.rollback_dag(migration_id=migration_id, dag_id=dag_id)
                return {"migration_id": migration_id, "dag_id": dag_id, "rolled_back": True}
            _cutover.rollback_migration(migration_id)
            return {"migration_id": migration_id, "rolled_back": True}

        return starship_route(post=_post)

    @router.post("/cutover/waves/{migration_id}/retry")
    @staticmethod
    def cutover_wave_retry(
        migration_id: str,
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            body = starship_route.json or {}
            selector = body.get("selector") or body.get("dag_id") or "failed"
            try:
                dag_ids = _cutover.retry_dags_in_wave(migration_id, selector)
            except ValueError as e:
                raise HttpError(str(e), 400) from e
            return {"migration_id": migration_id, "retry_dag_ids": dag_ids}

        return starship_route(post=_post)

    @router.post("/cutover/waves/{migration_id}/purge")
    @staticmethod
    def cutover_wave_purge(
        migration_id: str,
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            body = starship_route.json or {}
            dag_id = body.get("dag_id")
            if dag_id:
                deleted = _cutover.purge_dag_metadata(dag_id=dag_id)
                return {"migration_id": migration_id, "dag_id": dag_id, "runs_deleted": deleted}
            return _cutover.purge_wave_metadata(migration_id)

        return starship_route(post=_post)

    @router.post("/cutover/purge_all")
    @staticmethod
    def cutover_purge_all(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
    ):
        from astronomer_starship.providers.starship import cutover as _cutover

        def _post():
            return _cutover.purge_all_instance_dag_metadata()

        return starship_route(post=_post)

    @router.api_route("/source_connection", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def source_connection(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        """Manage a source-Airflow Connection used by the Cutover Tool."""

        def _resolve_conn_id(sources):
            for src in sources:
                if src:
                    return _normalize_source_conn_id(src)
            return STARSHIP_SOURCE_CONN_ID

        def _get():
            conn_id = _resolve_conn_id([starship_route.args.get("conn_id")])
            return read_source_connection(starship_compat.session, conn_id=conn_id)

        def _post():
            payload = starship_route.json or {}
            kwargs = build_source_connection_kwargs(payload)
            conn_id = kwargs["conn_id"]
            existed = source_connection_exists(starship_compat.session, conn_id)
            # Delete-if-exists; real errors will surface from set_connection.
            try:
                starship_compat.delete_connection(conn_id=conn_id)
            except Exception:  # nosec B110
                pass
            created = starship_compat.set_connection(**kwargs)
            return {
                **created,
                "action": "updated" if existed else "created",
                "conn_id": conn_id,
            }

        def _delete():
            conn_id = _resolve_conn_id([starship_route.args.get("conn_id")])
            return starship_compat.delete_connection(conn_id=conn_id)

        return starship_route(get=_get, post=_post, delete=_delete)

    @router.api_route("/xcom", methods=["GET", "POST", "DELETE"])
    @staticmethod
    def xcom(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(
            get=starship_compat.get_xcom,
            post=starship_compat.set_xcom,
            delete=starship_compat.delete_xcom,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.xcom_attrs()),
        )


class StarshipAPIPlugin(AirflowPlugin):
    name = "starship_api"
    fastapi_apps = [
        {
            "app": StarshipApi(),
            "url_prefix": "/api/starship",
            "name": "Starship API",
        }
    ]

    @classmethod
    def on_load(cls, *args, **kwargs):
        # Initialize compatibility layer on plugin load to ensure it loads fine.
        # If not, a runtime error will be raised, disabling the plugin.
        StarshipCompatabilityLayer()
