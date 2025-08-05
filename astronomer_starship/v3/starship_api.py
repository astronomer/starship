import json
from functools import partial
import requests
from airflow.plugins_manager import AirflowPlugin
from astronomer_starship.compat.starship_compatability import (
    StarshipCompatabilityLayer,
    get_kwargs_fn,
)
from fastapi import FastAPI, Request, APIRouter
from fastapi.responses import JSONResponse
from typing import Any, Dict, List, Union
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


def get_json_or_clean_str(o: str) -> Union[List[Any], Dict[Any, Any], Any]:
    """For Aeroscope - Either load JSON (if we can) or strip and split the string, while logging the error"""
    from json import JSONDecodeError
    import logging

    try:
        return json.loads(o)
    except (JSONDecodeError, TypeError) as e:
        logging.debug(e)
        logging.debug(o)
        return o.strip()


def clean_airflow_report_output(log_string: str) -> Union[dict, str]:
    r"""For Aeroscope - Look for the magic string from the Airflow report and then decode the base64 and convert to json
    Or return output as a list, trimmed and split on newlines
    >>> clean_airflow_report_output('INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\naGVsbG8gd29ybGQ=')
    'hello world'
    >>> clean_airflow_report_output(
    ...   'INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\neyJvdXRwdXQiOiAiaGVsbG8gd29ybGQifQ=='
    ... )
    {'output': 'hello world'}
    """
    from json import JSONDecodeError
    import base64

    log_lines = log_string.split("\n")
    enumerated_log_lines = list(enumerate(log_lines))
    found_i = -1
    for i, line in enumerated_log_lines:
        if "%%%%%%%" in line:
            found_i = i + 1
            break
    if found_i != -1:
        output = base64.decodebytes(
            "\n".join(log_lines[found_i:]).encode("utf-8")
        ).decode("utf-8")
        try:
            return json.loads(output)
        except JSONDecodeError:
            return get_json_or_clean_str(output)
    else:
        return get_json_or_clean_str(log_string)


starship_compat = StarshipCompatabilityLayer()
app = FastAPI()
router = APIRouter()


async def get_json(request: Request):
    return await request.json()


async def starship_route(
    request: Request,
    get=None,
    post=None,
    put=None,
    delete=None,
    patch=None,
    kwargs_fn=None,
):
    try:
        request_json = await request.json()
    except json.JSONDecodeError:
        # TODO log the error
        request_json = {}
    try:
        request_method = request.method

        kwargs = (
            kwargs_fn(
                request_method=request_method,
                args=(
                    request.query_params
                    if request_method in ["GET", "POST", "DELETE"]
                    else {}
                ),
                json=request_json,
            )
            if kwargs_fn
            else {}
        )
    except RuntimeError as e:
        return JSONResponse(content={"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            content={"error": f"Unknown Error in kwargs_fn - {e}"}, status_code=500
        )
    if request.method not in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
        raise RuntimeError(f"Unsupported Method: {request.method}")

    try:
        if request.method == "GET":
            res = get(**kwargs)
        elif request.method == "POST":
            from sqlalchemy.exc import IntegrityError, DataError, StatementError

            try:
                res = post(**kwargs)
            except IntegrityError:
                return JSONResponse(
                    content={
                        "error": "Integrity Error (Duplicate Record?)",
                        "kwargs": kwargs,
                    },
                    status_code=409,
                )
            except DataError as e:
                return JSONResponse(
                    content={
                        "error": "Data Error",
                        "error_message": e,
                        "kwargs": kwargs,
                    },
                    status_code=400,
                )
            except StatementError as e:
                return JSONResponse(
                    content={
                        "error": "SQL Error",
                        "error_message": e,
                        "kwargs": kwargs,
                    },
                    status_code=400,
                )
        elif request.method == "PUT":
            res = put(**kwargs)
        elif request.method == "DELETE":
            res = delete(**kwargs)
        elif request.method == "PATCH":
            res = patch(**kwargs)
    except Exception as e:
        import traceback

        res = JSONResponse(
            content={
                "error": "Unknown Error",
                "error_type": type(e),
                "error_message": f"{e}\n{traceback.format_exc()}",
                "kwargs": json.dumps(kwargs, default=str),
            },
            status_code=500,
        )
    return JSONResponse(content=res) if isinstance(res, list) else res


@router.get("/")
@router.get("/health")
async def health(request: Request):
    """
    Returns the health of the Starship API

    ---

    ### `GET /api/starship/health`

    **Parameters:** None

    **Response**:
    ```
    OK
    ```
    """

    def ok():
        return "OK"

    return await starship_route(request=request, get=ok)


@router.get("/telescope")
async def telescope(request: Request):
    from socket import gethostname
    import io
    import runpy
    from urllib.request import urlretrieve
    from contextlib import redirect_stdout, redirect_stderr
    from urllib.error import HTTPError
    from datetime import datetime, timezone
    import os

    aero_version = os.getenv("TELESCOPE_REPORT_RELEASE_VERSION", "latest")
    a = "airflow_report.pyz"
    aero_url = (
        "https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz"
        if aero_version == "latest"
        else f"https://github.com/astronomer/telescope/releases/download/{aero_version}/airflow_report.pyz"
    )
    try:
        urlretrieve(aero_url, a)
    except HTTPError as e:
        raise RuntimeError(
            f"Error finding specified version:{aero_version} -- Reason:{e.reason}"
        )

    s = io.StringIO()
    with redirect_stdout(s), redirect_stderr(s):
        runpy.run_path(a)
    report = {
        "telescope_version": "aeroscope-latest",
        "report_date": datetime.now(timezone.utc).isoformat()[:10],
        "organization_name": request.query_params["organization"],
        "local": {
            gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}
        },
    }
    presigned_url = request.query_params.get("presigned_url", False)
    if presigned_url:
        try:
            upload = requests.put(presigned_url, data=json.dumps(report))
            return upload.content, upload.status_code
        except requests.exceptions.ConnectionError as e:
            return str(e), 400
    return report


@router.get("/airflow_version")
async def airflow_version(request: Request):
    """
    Returns the version of Airflow that the Starship API is connected to.

    ---

    ### `GET /api/starship/airflow_version`

    **Parameters:** None

    **Response**:
    ```
    3.0.3+astro.1
    ```
    """
    return await starship_route(
        request=request, get=starship_compat.get_airflow_version
    )


@router.get("/env_vars")
async def env_vars(request: Request):
    """
    Get the Environment Variables, which may be used to set Airflow Connections, Variables, or Configurations

    ---

    ### `GET /api/starship/env_vars`

    **Parameters:** None

    **Response**:
    ```
    {
        "AIRFLOW_HOME": "/usr/local/airflow",
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN": "sqlite:////usr/local/airflow/airflow.db",
        ...
    }
    ```

    """
    return await starship_route(request=request, get=starship_compat.get_env_vars)


@router.get("/pools")
@router.post("/pools")
@router.delete("/pools")
async def pools(request: Request):
    """
    Get Pools or set a Pool

    **Model:** `airflow.models.Pool`

    **Table:** `pools`

    ---

    ### GET /api/starship/pools

    **Parameters:** None

    **Response**:
    ```json
    [
        {
            "name": "my_pool",
            "slots": 5,
            "description": "My Pool",
            "include_deferred": True
        },
        ...
    ]
    ```

    ### POST /api/starship/pools

    **Parameters:** JSON

    | Field (*=Required) | Version | Type | Example |
    |---------------------|---------|------|---------|
    | name*               |         | str  | my_pool |
    | slots*              |         | int  | 5       |
    | description         |         | str  | My Pool |
    | include_deferred*   | >=2.7   | bool | True    |

    **Response:** List of Pools, as `GET` Response

    ### DELETE /api/starship/pools

    **Parameters:** Args

    | Field (*=Required) | Version | Type | Example |
    |---------------------|---------|------|---------|
    | name*               |         | str  | my_pool |

    **Response:** None
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_pools,
        post=starship_compat.set_pool,
        delete=starship_compat.delete_pool,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.pool_attrs()),
    )


@router.get("/variables")
@router.post("/variables")
@router.delete("/variables")
async def variables(request: Request):
    """
    Get Variables or set a Variable

    **Model:** `airflow.models.Variable`

    **Table:** `variable`

    ---

    ### `GET /api/starship/variable`

    **Parameters:** None

    **Response**:
    ```json
    [
        {
            "key": "key",
            "val": "val",
            "description": "My Var"
        },
        ...
    ]
    ```

    ### `POST /api/starship/variable`

    **Parameters:** JSON

    | Field (*=Required) | Version | Type | Example |
    |---------------------|---------|------|---------|
    | key*                |         | str  | key     |
    | val*                |         | str  | val     |
    | description         |         | str  | My Var  |

    **Response:** List of Variables, as `GET` Response

    ### `DELETE /api/starship/variable`

    **Parameters:** Args

    | Field (*=Required) | Version | Type | Example |
    |---------------------|---------|------|---------|
    | key*                |         | str  | key     |

    **Response:** None
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_variables,
        post=starship_compat.set_variable,
        delete=starship_compat.delete_variable,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.variable_attrs()),
    )


@router.get("/connections")
@router.post("/connections")
@router.delete("/connections")
async def connections(request: Request):
    """
    Get Connections or set a Connection

    **Model:** `airflow.models.Connections`

    **Table:** `connection`

    ---

    ### `GET /api/starship/connection`

    **Parameters:** None

    **Response**:
    ```json
    [
        {
            "conn_id": "my_conn",
            "conn_type": "http",
            "host": "localhost",
            "port": "1234",
            "schema": "https",
            "login": "user",
            "password": "foobar",  # pragma: allowlist secret
            "extra": {},
            "description": "My Var"
        },
        ...
    ]
    ```

    ### `POST /api/starship/connection`

    **Parameters:** JSON

    | Field (*=Required) | Version | Type | Example   |
    |--------------------|---------|------|-----------|
    | conn_id*           |         | str  | my_conn   |
    | conn_type*         |         | str  | http      |
    | host               |         | str  | localhost |
    | port               |         | int  | 1234      |
    | schema             |         | str  | https     |
    | login              |         | str  | user      |
    | password           |         | str  | ******    |
    | extra              |         | dict | {}        |
    | description        |         | str  | My Conn   |

    **Response:** List of Connections, as `GET` Response

    ### DELETE /api/starship/connections

    **Parameters:** Args

    | Field (*=Required) | Version | Type | Example |
    |---------------------|---------|------|---------|
    | conn_id*            |         | str  | my_conn |

    **Response:** None
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_connections,
        post=starship_compat.set_connection,
        delete=starship_compat.delete_connection,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.connection_attrs()),
    )


@router.get("/dags")
@router.patch("/dags")
async def dags(request: Request):
    """
    Get DAG or pause/unpause a DAG

    **Model:** `airflow.models.DagModel`

    **Table:** `dags`

    ---

    ### `GET /api/starship/dags`

    **Parameters:** None

    **Response**:
    ```json
    [
        {
            "dag_id": "dag_0",
            "timetable_summary": "0 0 * * *",
            "is_paused": True,
            "fileloc": "/usr/local/airflow/dags/dag_0.py",
            "description": "My Dag",
            "owners": "user",
            "tags": ["tag1", "tag2"],
            "dag_run_count": 2,
            "bundle_name": "my_dag",
            "bundle_version": "my_version",
            "relative_fileloc": "dags/dag_0.py",
        },
        ...
    ]
    ```

    ### `PATCH /api/starship/dags`

    **Parameters:** JSON

    | Field (*=Required) | Version | Type | Example   |
    |--------------------|---------|------|-----------|
    | dag_id*            |         | str  | dag_0     |
    | is_paused*         |         | bool | True      |

    ```json
    {
        "dag_id": "dag_0",
        "is_paused": true
    }
    ```
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_dags,
        patch=starship_compat.set_dag_is_paused,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_attrs()),
    )


@router.get("/dag_runs")
@router.post("/dag_runs")
@router.delete("/dag_runs")
async def dag_runs(request: Request):
    """
    Get DAG Runs or set DAG Runs

    **Model:** `airflow.models.DagRun`

    **Table:** `dag_run`

    ---

    ### `GET /api/starship/dag_runs`

    **Parameters:** Args

    | Field (*=Required)       | Version | Type               | Example                           |
    |--------------------------|---------|--------------------|-----------------------------------|
    | dag_id*                  |         | str                | dag_0                             |
    | limit                    |         | int                | 10                                |
    | offset                   |         | int                | 0                                 |

    **Response**:
    ```json
    {
        "dag_run_count": 1,
        "dag_runs":
            [
                {
                    "dag_id": "dag_0",
                    "run_id": "manual__1970-01-01T00:00:00+00:00",
                    "queued_at": "1970-01-01T00:00:00+00:00",
                    "logical_date": "1970-01-01T00:00:00+00:00",
                    "start_date": "1970-01-01T00:00:00+00:00",
                    "end_date": "1970-01-01T00:00:00+00:00",
                    "state": "SUCCESS",
                    "creating_job_id": 123,
                    "run_type": "manual",
                    "conf": {"my_param": "my_value"},
                    "data_interval_start": "1970-01-01T00:00:00+00:00",
                    "data_interval_end": "1970-01-01T00:00:00+00:00",
                    "last_scheduling_decision": "1970-01-01T00:00:00+00:00",
                    "triggered_by": "dag_1",
                    "backfill_id": 1,
                    "created_dag_version_id": "...",
                    "bundle_version": "my_version",
                    "run_after": "1970-01-01T00:00:00+00:00",
                },
                ...
            ]
    }
    ```

    ### `POST /api/starship/dag_runs`

    **Parameters:** JSON

    | Field (*=Required)       | Version | Type               | Example                           |
    |--------------------------|---------|--------------------|-----------------------------------|
    | dag_runs           |         | list[DagRun]             | [ ... ]                           |

    ```json
    {
        "dag_runs": [ ... ]
    }
    ```

    **DAG Run:**

    | Field (*=Required)       | Version | Type | Example                           |
    |--------------------------|---------|------|-----------------------------------|
    | dag_id*                  |         | str  | dag_0                             |
    | queued_at                |         | date | 1970-01-01T00:00:00+00:00         |
    | execution_date*          | <3.0    | date | 1970-01-01T00:00:00+00:00         |
    | logical_date*            | >= 3.0  | date | 1970-01-01T00:00:00+00:00         |
    | start_date               |         | date | 1970-01-01T00:00:00+00:00         |
    | end_date                 |         | date | 1970-01-01T00:00:00+00:00         |
    | state                    |         | str  | SUCCESS                           |
    | run_id*                  |         | str  | manual__1970-01-01T00:00:00+00:00 |
    | creating_job_id          |         | int  | 123                               |
    | external_trigger         | <3.0    | bool | true                              |
    | run_type*                |         | str  | manual                            |
    | conf                     |         | dict | {}                                |
    | data_interval_start      | >2.1    | date | 1970-01-01T00:00:00+00:00         |
    | data_interval_end        | >2.1    | date | 1970-01-01T00:00:00+00:00         |
    | last_scheduling_decision |         | date | 1970-01-01T00:00:00+00:00         |
    | dag_hash                 | <3.0    | str  | ...                               |
    | clear_number             | >=2.8   | int  | 0                                 |
    | triggered_by             | >=3.0   | str  | dag_1                             |
    | backfill_id              | >=3.0   | int  | 1                                 |
    | created_dag_version_id   | >=3.0   | UUID | ...                               |
    | bundle_version           | >=3.0   | str  | my_version                        |
    | run_after                | >=3.0   | date | 1970-01-01T00:00:00+00:00         |

    ### DELETE /api/starship/dag_runs

    **Parameters:** Args

    | Field (*=Required)       | Version | Type               | Example                           |
    |--------------------------|---------|--------------------|-----------------------------------|
    | dag_id*                  |         | str                | dag_0                             |

    **Response:** None
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_dag_runs,
        post=starship_compat.set_dag_runs,
        delete=starship_compat.delete_dag_runs,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_runs_attrs()),
    )


@router.get("/task_instances")
@router.post("/task_instances")
async def task_instances(request: Request):
    """
    Get TaskInstances or set TaskInstances

    **Model:** `airflow.models.TaskInstance`

    **Table:** `task_instance`

    ---

    ### `GET /api/starship/task_instances`

    **Parameters:** Args

    | Field (*=Required)       | Version | Type               | Example                           |
    |--------------------------|---------|--------------------|-----------------------------------|
    | dag_id*                  |         | str                | dag_0                             |
    | limit                    |         | int                | 10                                |
    | offset                   |         | int                | 0                                 |

    **Response**:
    ```json
    {
        "task_instances": [
            {
                "dag_id": "dag_0"",
                "run_id": "manual__1970-01-01T00:00:00+00:00",
                "task_id": "task_0",
                "map_index": -1,
                "start_date": "1970-01-01T00:00:00+00:00",
                "end_date": "1970-01-01T00:00:00+00:00",
                "duration": 0.0,
                "max_tries": 2,
                "hostname": "host",
                "unixname": "unixname",
                "pool": "default_pool",
                "pool_slots": 1,
                "queue": "queue",
                "priority_weight": 1,
                "operator": "BashOperator",
                "queued_dttm": "1970-01-01T00:00:00+00:00",
                "queued_by_job_id": 123,
                "pid": 123,
                "external_executor_id": 124,
                "trigger_id": "...",
                "trigger_timeout": "1970-01-01T00:00:00+00:00",
                "executor_config": "...",
                "last_heartbeat_at": "1970-01-01T00:00:00+00:00",
                "dag_version_id": "...",
                "scheduled_dttm": "1970-01-01T00:00:00+00:00"
            },
            ...
        ],
        "dag_run_count": 2,
    }
    ```

    ### `POST /api/starship/task_instances`

    **Parameters:** JSON

    | Field (*=Required)       | Version | Type               | Example                           |
    |--------------------------|---------|--------------------|-----------------------------------|
    | task_instances           |         | list[TaskInstance] | [ ... ]                           |

    ```json
    {
        "task_instances": [ ... ]
    }
    ```

    **Task Instance:**

    | Field (*=Required)       | Version | Type | Example                           |
    |--------------------------|---------|------|-----------------------------------|
    | dag_id*                  |         | str  | dag_0                             |
    | run_id*                  | >2.1    | str  | manual__1970-01-01T00:00:00+00:00 |
    | task_id*                 |         | str  | task_0                            |
    | map_index*               | >2.2    | int  | -1                                |
    | execution_date*          | <=2.1   | date | 1970-01-01T00:00:00+00:00         |
    | start_date               |         | date | 1970-01-01T00:00:00+00:00         |
    | end_date                 |         | date | 1970-01-01T00:00:00+00:00         |
    | duration                 |         | float | 0.0                              |
    | max_tries                |         | int  | 2                                 |
    | hostname                 |         | str  | host                              |
    | unixname                 |         | str  | unixname                          |
    | job_id                   | <3.0    | int  | 123                               |
    | pool*                    |         | str  | default_pool                      |
    | pool_slots               |         | int  | 1                                 |
    | queue                    |         | str  | queue                             |
    | priority_weight          |         | int  | 1                                 |
    | operator                 |         | str  | BashOperator                      |
    | queued_dttm              |         | date | 1970-01-01T00:00:00+00:00         |
    | queued_by_job_id         |         | int  | 123                               |
    | pid                      |         | int  | 123                               |
    | external_executor_id     |         | int  |                                   |
    | trigger_id               | >2.1    | str  |                                   |
    | trigger_timeout          | >2.1    | date | 1970-01-01T00:00:00+00:00         |
    | executor_config          |         | str  |                                   |
    | last_heartbeat_at        | >=3.0   | date | 1970-01-01T00:00:00+00:00         |
    | dag_version_id           | >=3.0   | UUID |                                   |
    | scheduled_dttm           | >=3.0   | date | 1970-01-01T00:00:00+00:00         |
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_task_instances,
        post=starship_compat.set_task_instances,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_instances_attrs()),
    )


@router.get("/task_log")
@router.post("/task_log")
@router.delete("/task_log")
async def task_log(request: Request):
    """
    **EXPERIMENTAL**

    Get, set or delete task logs.

    **Requirements:**

    - Airflow 2.8+
    - Astro hosted deployments or local astro dev environment

    ---

    ### `GET /api/starship/task_log`

    **Parameters:** Args

    | Field (*=Required)       | Version | Type               | Example                              |
    |--------------------------|---------|--------------------|--------------------------------------|
    | dag_id*                  |         | str                | dag_0                                |
    | run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
    | task_id*                 |         | str                | task_0                               |
    | map_index*               |         | int                | -1                                   |
    | try_number*              |         | int                | 1                                    |
    | block_size               |         | int                | 1048576                              |


    **Response**:

    ```txt
    [2025-06-30T21:02:11.417+0000] ...

    ... Task exited with return code 0
    ```

    ### `POST /api/starship/task_log`

    **Parameters:** Args

    | Field (*=Required)       | Version | Type               | Example                              |
    |--------------------------|---------|--------------------|--------------------------------------|
    | dag_id*                  |         | str                | dag_0                                |
    | run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
    | task_id*                 |         | str                | task_0                               |
    | map_index*               |         | int                | -1                                   |
    | try_number*              |         | int                | 1                                    |
    | block_size               |         | int                | 1048576                              |

    **Request**:

    ```txt
    [2025-06-30T21:02:11.417+0000] ...

    ... Task exited with return code 0
    ```

    **Response:** None

    ### DELETE /api/starship/task_log

    **Parameters:** Args

    | Field (*=Required)       | Version | Type               | Example                              |
    |--------------------------|---------|--------------------|--------------------------------------|
    | dag_id*                  |         | str                | dag_0                                |
    | run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
    | task_id*                 |         | str                | task_0                               |
    | map_index*               |         | int                | -1                                   |
    | try_number*              |         | int                | 1                                    |

    **Response:** None
    """
    return await starship_route(
        request=request,
        get=starship_compat.get_task_log,
        post=starship_compat.set_task_log,
        delete=starship_compat.delete_task_log,
        kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.task_log_attrs()),
    )


app.include_router(router)

app_with_metadata = {"app": app, "url_prefix": "/api/starship", "name": "starship_api"}


class StarshipAPIPlugin(AirflowPlugin):
    name = "starship_api"
    fastapi_apps = [app_with_metadata]
