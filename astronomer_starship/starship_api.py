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
    def health(self) -> str:
        """
        Returns the health of the Starship API

        ---

        ### `GET /starship/api/health`

        **Parameters:** None

        **Response**:
        ```
        OK
        ```
        """

        def ok():
            return "OK"

        return starship_route(get=ok)

    @expose("/airflow_version", methods=["GET"])
    @csrf.exempt
    def airflow_version(self) -> str:
        """
        Returns the version of Airflow that the Starship API is connected to.

        ---

        ### `GET /starship/api/airflow_version`

        **Parameters:** None

        **Response**:
        ```
        OK
        ```
        """
        return starship_route(get=starship_compat.get_airflow_version)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    @expose("/env_vars", methods=["GET"])
    @csrf.exempt
    def env_vars(self):
        """
        Get the Environment Variables, which may be used to set Airflow Connections, Variables, or Configurations

        ---

        ### `GET /starship/api/env_vars`

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
        return starship_route(get=starship_compat.get_env_vars)

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
    @expose("/pools", methods=["GET", "POST"])
    @csrf.exempt
    def pools(self):
        """
        Get Pools or set a Pool

        **Model:** `airflow.models.Pool`

        **Table:** `pools`

        ---

        ### GET /starship/api/pools

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

        ### POST /starship/api/pools

        **Parameters:** JSON

        | Field (*=Required) | Version | Type | Example |
        |---------------------|---------|------|---------|
        | name*               |         | str  | my_pool |
        | slots*              |         | int  | 5       |
        | description         |         | str  | My Pool |
        | include_deferred*   | >=2.7   | bool | True    |

        **Response:** List of Pools, as `GET` Response
        """
        return starship_route(
            get=starship_compat.get_pools,
            post=starship_compat.set_pool,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.pool_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    @expose("/variables", methods=["GET", "POST"])
    @csrf.exempt
    def variables(self):
        """
        Get Variables or set a Variable

        **Model:** `airflow.models.Variable`

        **Table:** `variable`

        ---

        ### `GET /starship/api/variable`

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

        ### `POST /starship/api/variable`

        **Parameters:** JSON

        | Field (*=Required) | Version | Type | Example |
        |---------------------|---------|------|---------|
        | key*                |         | str  | key     |
        | val*                |         | str  | val     |
        | description         |         | str  | My Var  |

        **Response:** List of Variables, as `GET` Response
        """
        return starship_route(
            get=starship_compat.get_variables,
            post=starship_compat.set_variable,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.variable_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    @expose("/connections", methods=["GET", "POST"])
    @csrf.exempt
    def connections(self):
        """
        Get Connections or set a Connection

        **Model:** `airflow.models.Connections`

        **Table:** `connection`

        ---

        ### `GET /starship/api/connection`

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

        ### `POST /starship/api/connection`

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
        """
        return starship_route(
            get=starship_compat.get_connections,
            post=starship_compat.set_connection,
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

        ### `GET /starship/api/dags`

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
                "task_count": 3
            },
            ...
        ]
        ```

        ### `PATCH /starship/api/dags`

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
        return starship_route(
            get=starship_compat.get_dags,
            patch=starship_compat.set_dag_is_paused,
            kwargs_fn=partial(get_kwargs_fn, attrs=starship_compat.dag_attrs()),
        )

    # @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)])
    @expose("/dag_runs", methods=["GET", "POST"])
    @csrf.exempt
    def dag_runs(self):
        """
        Get DAG Runs or set DAG Runs

        **Model:** `airflow.models.DagRun`

        **Table:** `dag_run`

        ---

        ### `GET /starship/api/dag_runs`

        **Parameters:** Args

        | Field (*=Required)       | Version | Type               | Example                           |
        |--------------------------|---------|--------------------|-----------------------------------|
        | dag_id*                  |         | str                | dag_0                             |
        | limit                    |         | int                | 10                                |
        | offset                   |         | int                | 0                                 |

        **Response**:
        ```json
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
                "conf": None,
                "data_interval_start": "1970-01-01T00:00:00+00:00",
                "data_interval_end": "1970-01-01T00:00:00+00:00",
                "last_scheduling_decision": "1970-01-01T00:00:00+00:00",
                "dag_hash": "...."
            },
            ...
        ]
        ```

        ### `POST /starship/api/dag_runs`

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
        | dash_hash                |         | str  | ...                               |
        | clean_number             | >=2.8   | int  | 0                                 |
        """
        return starship_route(
            get=starship_compat.get_dag_runs,
            post=starship_compat.set_dag_runs,
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

        ### `GET /starship/api/task_instances`

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
                    "conf": None,
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

        ### `POST /starship/api/task_instances`

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
