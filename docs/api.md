# API

## Error Responses

In the event of an error, the API will return a JSON response with an `error` key
and an HTTP `status_code`. The `error` key will contain a message describing the error.

| **Type**                          | **Status Code** | **Response Example**                                                                        |
|-----------------------------------|-----------------|---------------------------------------------------------------------------------------------|
| **Request kwargs - RuntimeError** | 400             | ```{"error": "..."}```                                                                      |
| **Request kwargs - Exception**    | 500             | ```{"error": "Unknown Error in kwargs_fn - ..."}```                                         |
| **Unknown Error**                 | 500             | ```{"error": "Unknown Error", "error_type": ..., "error_message": ..., "kwargs": ...}```    |
| **`POST` Integrity Error**        | 409             | ```{"error": "Integrity Error (Duplicate Record?)", "error_message": ..., "kwargs": ...}``` |
| **`POST` Data Error**             | 400             | ```{"error": "Data Error", "error_message": ..., "kwargs": ...}```                          |
| **`POST` SQL Error**              | 400             | ```{"error": "SQL Error", "error_message": ..., "kwargs": ...}```                           |

## Airflow Version

Returns the version of Airflow that the Starship API is connected to.

!!! warning "Deprecated"
    Instead use [`/api/starship/info`](#starship-info) to get both Airflow and Starship versions plus additional information.

---

### `GET /api/starship/airflow_version`

**Parameters:** None

**Response**:

```txt
2.11.0+astro.1
```

## Starship Info

Returns relevant information related to Starship and the Airflow deployment.

---

### `GET /api/starship/info`

**Parameters:** None

**Response**:

```json
{
    "airflow_version": "2.11.0+astro.1",
    "starship_version": "2.5.0",
}
```

## Health

Returns the health of the Starship API

!!! warning "Deprecated"
    Instead use [`/api/starship/info`](#starship-info) to get both Airflow and Starship versions plus additional information.

---

### `GET /api/starship/health`

**Parameters:** None

**Response**:

```txt
OK
```

## Environment Variables

Get the Environment Variables, which may be used to set Airflow Connections, Variables, or Configurations

---

### `GET /api/starship/env_vars`

**Parameters:** None

**Response**:

```json
{
    "FOO": "bar",
    "AIRFLOW__CORE__SQL_ALCHEMY_CONN": "sqlite:////usr/local/airflow/airflow.db",
    ...
}
```

## Variable

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

## Pools

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

## Connections

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

## DAGs

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

## DAG Runs

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

## Task Instances

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

## Task Instance History

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

## Task Log

Get, set or delete task logs.

!!! danger "Experimental"
    This feature is considered experimental and subject to change.

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

## XCom

Get, set or delete XComs.

!!! danger "Experimental"
    This feature is considered experimental and subject to change.

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

### `POST /api/starship/xcom`

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

### DELETE /api/starship/xcom

**Parameters:** Args

| Field (*=Required)       | Version | Type               | Example                              |
|--------------------------|---------|--------------------|--------------------------------------|
| dag_id*                  |         | str                | dag_0                                |
| run_id*                  |         | str                | scheduled__2025-06-30T20:00:00+00:00 |
| task_id*                 |         | str                | task_0                               |
| map_index*               |         | int                | -1                                   |

**Response:** None
