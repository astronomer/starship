import json
from functools import partial
from http import HTTPStatus
from typing import TYPE_CHECKING

import flask
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from flask import Blueprint, Response, jsonify, request
from flask_appbuilder import BaseView, expose

from astronomer_starship import common
from astronomer_starship._af2.starship_compatability import (
    StarshipCompatabilityLayer,
    get_kwargs_fn,
)

if TYPE_CHECKING:
    from typing import Callable


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
                args=(
                    request.args if request_method in ["GET", "POST", "DELETE"] else {}
                ),
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

        if res is None:
            res = Response(status=HTTPStatus.NO_CONTENT)
    except common.HttpError as e:
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
        """
        Returns the health of the Starship API

        DEPRECATED: Instead use [`/api/starship/info`](./#starship-info) which provides the same functionality
        and additional information about Airflow and Starship.

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

        return starship_route(get=ok)

    @expose("/telescope", methods=["GET"])
    @csrf.exempt
    def telescope(self):
        return common.telescope(
            organization=request.args["organization"],
            presigned_url=request.args.get("presigned_url", None),
        )

    @expose("/airflow_version", methods=["GET"])
    @csrf.exempt
    def airflow_version(self) -> str:
        """
        Returns the version of Airflow that the Starship API is connected to.

        DEPRECATED: Instead use [`/api/starship/info`](./#starship-info) to get both Airflow and Starship versions
        plus additional information.

        ---

        ### `GET /api/starship/airflow_version`

        **Parameters:** None

        **Response**:
        ```
        2.11.0+astro.1
        ```
        """
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(get=starship_compat.get_airflow_version)

    @expose("/info", methods=["GET"])
    @csrf.exempt
    def info(self) -> str:
        """
        Returns relevant information related to Starship and the Airflow deployment.

        ---

        ### `GET /api/starship/info`

        **Parameters:** None

        **Response**:
        ```
        {
          "airflow_version": "2.11.0+astro.1",
          "starship_version": "2.5.0",
        }
        ```
        """
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(get=starship_compat.get_info)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    @expose("/env_vars", methods=["GET"])
    @csrf.exempt
    def env_vars(self):
        """
        Get the Environment Variables, which may be used to set Airflow Connections, Variables, or Configurations

        ---

        ### `GET /api/starship/env_vars`

        **Parameters:** None

        **Response**:
        ```
        {
            "FOO": "bar",
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN": "sqlite:////usr/local/airflow/airflow.db",
            ...
        }
        ```

        """
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(get=starship_compat.get_env_vars)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
    @expose("/pools", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def pools(self):
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
                "description": "My Pool
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
                "extra": "{}",
                "conn_type": "http",
                "conn_type": "http",
                "conn_type": "http",
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
        | extra              |         | dict  | {}       |
        | description        |         | str  | My Conn   |

        **Response:** List of Connections, as `GET` Response

        ### DELETE /api/starship/connections

        **Parameters:** Args

        | Field (*=Required) | Version | Type | Example |
        |---------------------|---------|------|---------|
        | conn_id*            |         | str  | my_conn |

        **Response:** None
        """
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
                "schedule_interval": "0 0 * * *",
                "is_paused": true,
                "fileloc": "/usr/local/airflow/dags/dag_0.py",
                "description": "My Dag",
                "owners": "user",
                "tags": ["tag1", "tag2"],
                "dag_run_count": 2,
            },
            ...
        ]
        ```

        ### `PATCH /api/starship/dags`

        **Parameters:** JSON

        | Field (*=Required) | Version | Type | Example   |
        |--------------------|---------|------|-----------|
        | dag_id*            |         | str  | dag_0     |
        | is_paused*         |         | bool | true      |

        ```json
        {
            "dag_id": "dag_0",
            "is_paused": true
        }
        ```
        """
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
                        "queued_at": "1970-01-01T00:00:00+00:00",
                        "execution_date": "1970-01-01T00:00:00+00:00",
                        "start_date": "1970-01-01T00:00:00+00:00",
                        "end_date": "1970-01-01T00:00:00+00:00",
                        "state": "SUCCESS",
                        "run_id": "manual__1970-01-01T00:00:00+00:00",
                        "creating_job_id": 123,
                        "external_trigger": true,
                        "run_type": "manual",
                        "conf": {"my_param": "my_value"},
                        "data_interval_start": "1970-01-01T00:00:00+00:00",
                        "data_interval_end": "1970-01-01T00:00:00+00:00",
                        "last_scheduling_decision": "1970-01-01T00:00:00+00:00",
                        "dag_hash": "...."
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
        | execution_date*          |         | date | 1970-01-01T00:00:00+00:00         |
        | start_date               |         | date | 1970-01-01T00:00:00+00:00         |
        | end_date                 |         | date | 1970-01-01T00:00:00+00:00         |
        | state                    |         | str  | SUCCESS                           |
        | run_id*                  |         | str  | manual__1970-01-01T00:00:00+00:00 |
        | creating_job_id          |         | int  | 123                               |
        | external_trigger         |         | bool | true                              |
        | run_type*                |         | str  | manual                            |
        | conf                     |         | dict | {}                                |
        | data_interval_start      | >2.1    | date | 1970-01-01T00:00:00+00:00         |
        | data_interval_end        | >2.1    | date | 1970-01-01T00:00:00+00:00         |
        | last_scheduling_decision |         | date | 1970-01-01T00:00:00+00:00         |
        | dag_hash                 |         | str  | ...                               |
        | clear_number             | >=2.8   | int  | 0                                 |

        ### DELETE /api/starship/dag_runs

        **Parameters:** Args

        | Field (*=Required)       | Version | Type               | Example                           |
        |--------------------------|---------|--------------------|-----------------------------------|
        | dag_id*                  |         | str                | dag_0                             |

        **Response:** None
        """
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
                    "task_instances": []
                    "run_id": "manual__1970-01-01T00:00:00+00:00",
                    "queued_at": "1970-01-01T00:00:00+00:00",
                    "execution_date": "1970-01-01T00:00:00+00:00",
                    "start_date": "1970-01-01T00:00:00+00:00",
                    "end_date": "1970-01-01T00:00:00+00:00",
                    "state": "SUCCESS",
                    "creating_job_id": 123,
                    "external_trigger": true,
                    "run_type": "manual",
                    "conf": {"my_param": "my_value"},
                    "data_interval_start": "1970-01-01T00:00:00+00:00",
                    "data_interval_end": "1970-01-01T00:00:00+00:00",
                    "last_scheduling_decision": "1970-01-01T00:00:00+00:00",
                    "dag_hash": "...."
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
        | job_id                   |         | int  | 123                               |
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
        """
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_task_instances,
            post=starship_compat.set_task_instances,
            kwargs_fn=partial(
                get_kwargs_fn, attrs=starship_compat.task_instances_attrs()
            ),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE_HISTORY)])
    @expose("/task_instance_history", methods=["GET", "POST"])
    @csrf.exempt
    def task_instance_history(self):
        """
        Get and set TaskInstanceHistory records.

        **Model:** `airflow.models.TaskInstanceHistory`

        **Table:** `task_instance_history`

        ---

        ### `GET /api/starship/task_instance_history`

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
                    "task_instances": []
                    "run_id": "manual__1970-01-01T00:00:00+00:00",
                    "queued_at": "1970-01-01T00:00:00+00:00",
                    "execution_date": "1970-01-01T00:00:00+00:00",
                    "start_date": "1970-01-01T00:00:00+00:00",
                    "end_date": "1970-01-01T00:00:00+00:00",
                    "state": "SUCCESS",
                    "creating_job_id": 123,
                    "external_trigger": true,
                    "run_type": "manual",
                    "conf": {"my_param": "my_value"},
                    "data_interval_start": "1970-01-01T00:00:00+00:00",
                    "data_interval_end": "1970-01-01T00:00:00+00:00",
                    "last_scheduling_decision": "1970-01-01T00:00:00+00:00",
                    "dag_hash": "...."
                },
                ...
            ],
            "dag_run_count": 2,
        }
        ```

        ### `POST /api/starship/task_instance_history`

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
        | job_id                   |         | int  | 123                               |
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
        """
        starship_compat = StarshipCompatabilityLayer()
        return starship_route(
            get=starship_compat.get_task_instance_history,
            post=starship_compat.set_task_instance_history,
            kwargs_fn=partial(
                get_kwargs_fn, attrs=starship_compat.task_instances_attrs()
            ),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)])
    @expose("/task_log", methods=["GET", "POST", "DELETE"])
    @csrf.exempt
    def task_logs(self):
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
        """
        **EXPERIMENTAL**

        Get, set or delete XComs.

        **Requirements:**

        - Airflow 2.8+

        ---

        ### `GET /api/starship/xcom`

        **Parameters:** Args

        | Field (*=Required)       | Version | Type               | Example                              |
        |--------------------------|---------|--------------------|--------------------------------------|
        | dag_id*                  |         | str                | dag_0                                |
        | run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
        | task_id*                 |         | str                | task_0                               |
        | map_index*               |         | int                | -1                                   |

        **Response**:

        ```json
        [
            {
                "dag_id": "example_xcom",
                "key": "example_str",
                "map_index": -1,
                "run_id": "scheduled__2025-07-17T00:00:00+00:00",
                "task_id": "run",
                "value": "bnVsbA=="
            }
        ]
        ```

        ### `POST /api/starship/task_log`

        **Parameters:** JSON

        | Field (*=Required)       | Version | Type               | Example                              |
        |--------------------------|---------|--------------------|--------------------------------------|
        | dag_id*                  |         | str                | dag_0                                |
        | run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
        | task_id*                 |         | str                | task_0                               |
        | map_index*               |         | int                | -1                                   |
        | key*                     |         | str                | return_value                         |
        | value*                   |         | str                | bnVsbA==                             |

        **Request**:

        ```json
        {
            "dag_id": "example_xcom",
            "key": "example_str",
            "map_index": -1,
            "run_id": "scheduled__2025-07-17T00:00:00+00:00",
            "task_id": "run",
            "value": "bnVsbA=="
        }
        ```

        **Response**: None

        ### DELETE /api/starship/task_log

        **Parameters:** Args

        | Field (*=Required)       | Version | Type               | Example                              |
        |--------------------------|---------|--------------------|--------------------------------------|
        | dag_id*                  |         | str                | dag_0                                |
        | run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
        | task_id*                 |         | str                | task_0                               |
        | map_index*               |         | int                | -1                                   |

        **Response:** None
        """
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
