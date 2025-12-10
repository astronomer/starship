import json
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Annotated

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

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
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instances_attrs()),
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
