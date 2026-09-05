# Starship Migration DAG
The `StarshipAirflowMigrationDAG` can be used to migrate Airflow Variables, Pools, Connections,
and DAG History from one Airflow instance to another.

The `StarshipAirflowMigrationDAG` should be used in instances where the **source** Airflow Webserver
is unable to correctly host a Plugin. The Target must still have a functioning Starship Plugin installed,
be running the same version of Airflow, and have the same set of DAGs deployed.

The `StarshipAirflowMigrationDAG` should be used if migrating from a
Google Cloud Composer 1 (with Airflow 2.x) or MWAA v2.0.2 environment.
These environments do not support webserver plugins and will require using the `StarshipAirflowMigrationDAG`
to migrate data.

## Installation
Add the following line to your `requirements.txt` in your source environment:

   ```
   astronomer-starship
   ```

!!! note
    Earlier releases required the `[provider]` extra
    (`astronomer-starship[provider]`) to pull in `apache-airflow-providers-http`.
    That package is now part of the core dependencies, so the extra is no longer
    necessary. It is still accepted for backwards compatibility and installs the
    same package.

## Setup

### Target connection

Make a connection in Airflow with the following details:

- **Conn ID**: `starship_default`
- **Conn Type**: `HTTP`
- **Host**: the URL of the homepage of Airflow (excluding `/home` on the end of the URL)
  - For example, if your deployment URL is `https://astronomer.astronomer.run/abcdt4ry/home`, you'll use `https://astronomer.astronomer.run/abcdt4ry`
- **Schema**: `https`
- **Extra**: `{"Authorization": "Bearer <token>"}`

### Source connection (Airflow 3 only)

On Airflow 3 the migration DAG cannot use direct database access from workers,
so the source hook now talks to the source Airflow's Starship HTTP API. This
requires a second Airflow connection **on the source deployment itself**:

- **Conn ID**: `starship_source`
- **Conn Type**: `HTTP`
- **Host**: the base URL of the source Airflow (same rules as above -- exclude any `/home` suffix)
- **Schema**: `https`
- **Extra**: `{"Authorization": "Bearer <token>"}`

The token must be valid for the source Airflow's `/api/starship/*` endpoints.
If the connection is missing when the DAG runs, Starship raises a clear
`RuntimeError` naming the connection and explaining the requirement, so you can
add the connection and re-run.

On Airflow 2 no source connection is needed -- the source hook reads directly
from the local metadata DB, exactly as it did in prior releases.

## Usage
1. Add the following DAG to your source environment:

    ```python title="dags/starship_airflow_migration_dag.py"
    from astronomer_starship.providers.starship.operators.starship import (
        StarshipAirflowMigrationDAG,
    )

    globals()["starship_airflow_migration_dag"] = StarshipAirflowMigrationDAG(
        http_conn_id="starship_default"
    )
    ```

2. Unpause the DAG in the Airflow UI
3. Once the DAG successfully runs, your connections, variables, and environment variables should all be migrated to Astronomer

## Configuration

The `StarshipAirflowMigrationDAG` can be configured as follows:

```python
StarshipAirflowMigrationDAG(
    http_conn_id="starship_default",
    variables=None,  # None to migrate all, or ["var1", "var2"] to migrate specific items, or empty list to skip all
    pools=None,  # None to migrate all, or ["pool1", "pool2"] to migrate specific items, or empty list to skip all
    connections=None,  # None to migrate all, or ["conn1", "conn2"] to migrate specific items, or empty list to skip all
    dag_ids=None,  # None to migrate all, or ["dag1", "dag2"] to migrate specific items, or empty list to skip all
)
```

You can use this DAG to migrate all items, or specific items by providing a list of names.

You can skip migration by providing an empty list.

## Python API

### Hooks

::: astronomer_starship.providers.starship.hooks.starship
    options:
        heading_level: 4
        show_root_toc_entry: false
        show_root_heading: false
        inherited_members: true
        show_source: false

### Operators, TaskGroups, DAG

::: astronomer_starship.providers.starship.operators.starship
    options:
        heading_level: 4
        show_root_toc_entry: false
        show_root_heading: false
        inherited_members: true
        show_source: false
