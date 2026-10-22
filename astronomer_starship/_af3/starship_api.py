import json
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Annotated

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    requires_access_configuration,
    requires_access_connection,
    requires_access_pool,
    requires_access_variable,
)
from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from astronomer_starship._af3.starship_compatability import StarshipAirflow, StarshipCompatabilityLayer
from astronomer_starship.common import HttpError, get_kwargs_fn, telescope

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


async def _dag_ids_in_request(request: Request) -> "set[str]":
    """Every `dag_id` the request names, in the query string, the JSON body, or the records of a bulk JSON body.

    Starship handlers read `dag_id` from the body before the query string (see `get_from_request`),
    and bulk bodies (e.g. `{"dag_runs": [...]}`) name a DAG per record, so all of them must be authorized.
    """
    dag_ids = [request.query_params.get("dag_id")]
    if await request.body():
        try:
            body = await request.json()
        except ValueError:
            raise HTTPException(400, "Request body is not valid JSON") from None
        if isinstance(body, dict):
            dag_ids.append(body.get("dag_id"))
            for value in body.values():
                if isinstance(value, list):
                    dag_ids.extend(item.get("dag_id") for item in value if isinstance(item, dict))
    if any(dag_id is not None and not isinstance(dag_id, str) for dag_id in dag_ids):
        raise HTTPException(400, "'dag_id' must be a string")
    return {dag_id for dag_id in dag_ids if dag_id and dag_id != "~"}


def _is_authorized_dags(method, access_entity, dag_ids, user) -> bool:
    from airflow.configuration import conf
    from airflow.models import DagModel

    # Airflow 3.1+ can scope DAGs to teams
    multi_team = conf.getboolean("core", "multi_team", fallback=False) and hasattr(DagModel, "get_team_name")

    def details(dag_id):
        if dag_id and multi_team:
            return DagDetails(id=dag_id, team_name=DagModel.get_team_name(dag_id))
        return DagDetails(id=dag_id)

    return get_auth_manager().batch_is_authorized_dag(
        requests=[
            {"method": method, "access_entity": access_entity, "details": details(dag_id)}
            for dag_id in sorted(dag_ids) or [None]
        ],
        user=user,
    )


def requires_access_dags(method, access_entity=None):
    """Like Airflow's `requires_access_dag`, but for every DAG the request names, see `_dag_ids_in_request`.
    Without a `dag_id`, the user needs access to all DAGs.
    """

    async def inner(request: Request, user: GetUserDep) -> None:
        dag_ids = await _dag_ids_in_request(request)
        # The auth manager may query the database, keep it off the event loop
        if not await run_in_threadpool(_is_authorized_dags, method, access_entity, dag_ids, user):
            raise HTTPException(403, "Forbidden")

    return inner


router = AirflowRouter(
    tags=["Starship"],
    # Baseline for every route: users with access to the Airflow configuration.
    # Routes that read or write Airflow resources add the matching resource check.
    dependencies=[Depends(requires_access_configuration("GET"))],
)


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

    @router.get(
        "/env_vars",
        # Environment variables can hold connections (`AIRFLOW_CONN_*`) and variables (`AIRFLOW_VAR_*`).
        dependencies=[Depends(requires_access_connection("GET")), Depends(requires_access_variable("GET"))],
    )
    @staticmethod
    def env_vars(
        starship_route: Annotated[StarshipRoute, Depends(starship_route)],
        starship_compat: Annotated[StarshipAirflow, Depends(starship_compat)],
    ):
        return starship_route(get=starship_compat.get_env_vars)

    @router.get("/pools", dependencies=[Depends(requires_access_pool("GET"))])
    @router.post("/pools", dependencies=[Depends(requires_access_pool("POST"))])
    @router.delete("/pools", dependencies=[Depends(requires_access_pool("DELETE"))])
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

    @router.get("/variables", dependencies=[Depends(requires_access_variable("GET"))])
    @router.post("/variables", dependencies=[Depends(requires_access_variable("POST"))])
    @router.delete("/variables", dependencies=[Depends(requires_access_variable("DELETE"))])
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

    @router.get("/connections", dependencies=[Depends(requires_access_connection("GET"))])
    @router.post("/connections", dependencies=[Depends(requires_access_connection("POST"))])
    @router.delete("/connections", dependencies=[Depends(requires_access_connection("DELETE"))])
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

    @router.get("/dags", dependencies=[Depends(requires_access_dags("GET"))])
    @router.patch("/dags", dependencies=[Depends(requires_access_dags("PUT"))])
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

    @router.get("/dag_runs", dependencies=[Depends(requires_access_dags("GET", DagAccessEntity.RUN))])
    @router.post("/dag_runs", dependencies=[Depends(requires_access_dags("POST", DagAccessEntity.RUN))])
    @router.delete("/dag_runs", dependencies=[Depends(requires_access_dags("DELETE", DagAccessEntity.RUN))])
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

    @router.get("/task_instances", dependencies=[Depends(requires_access_dags("GET", DagAccessEntity.TASK_INSTANCE))])
    @router.post("/task_instances", dependencies=[Depends(requires_access_dags("POST", DagAccessEntity.TASK_INSTANCE))])
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

    @router.get(
        "/task_instance_history", dependencies=[Depends(requires_access_dags("GET", DagAccessEntity.TASK_INSTANCE))]
    )
    @router.post(
        "/task_instance_history", dependencies=[Depends(requires_access_dags("POST", DagAccessEntity.TASK_INSTANCE))]
    )
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

    @router.get("/task_log", dependencies=[Depends(requires_access_dags("GET", DagAccessEntity.TASK_LOGS))])
    @router.post("/task_log", dependencies=[Depends(requires_access_dags("POST", DagAccessEntity.TASK_INSTANCE))])
    @router.delete("/task_log", dependencies=[Depends(requires_access_dags("DELETE", DagAccessEntity.TASK_INSTANCE))])
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

    @router.get("/xcom", dependencies=[Depends(requires_access_dags("GET", DagAccessEntity.XCOM))])
    @router.post("/xcom", dependencies=[Depends(requires_access_dags("POST", DagAccessEntity.XCOM))])
    @router.delete("/xcom", dependencies=[Depends(requires_access_dags("DELETE", DagAccessEntity.XCOM))])
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
