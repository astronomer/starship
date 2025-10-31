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

::: astronomer_starship.compat.StarshipApi.airflow_version
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Starship Info

::: astronomer_starship.compat.StarshipApi.info
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Health

::: astronomer_starship.compat.StarshipApi.health
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Environment Variables

::: astronomer_starship.compat.StarshipApi.env_vars
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Variable

::: astronomer_starship.compat.StarshipApi.variables
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Pools

::: astronomer_starship.compat.StarshipApi.pools
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Connections

::: astronomer_starship.compat.StarshipApi.connections
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## DAGs

::: astronomer_starship.compat.StarshipApi.dags
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## DAG Runs

::: astronomer_starship.compat.StarshipApi.dag_runs
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Task Instances

::: astronomer_starship.compat.StarshipApi.task_instances
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Task Instance History

::: astronomer_starship.compat.StarshipApi.task_instance_history
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## Task Log

::: astronomer_starship.compat.StarshipApi.task_logs
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false

## XCom

::: astronomer_starship.compat.StarshipApi.xcom
    options:
        show_root_toc_entry: false
        show_root_heading: false
        show_source: false
        show_header: false
