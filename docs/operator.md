The Starship Operator should be used in instances where the Airflow Webserver is unable to correctly host a Plugin.

The `AstroMigrationOperator` should be used if migrating from a
Google Cloud Composer 1 (with Airflow 2.x) or MWAA v2.0.2 environment.
These environments do not support webserver plugins and will require using the `AstroMigrationOperator`
to migrate data.

## Usage

1. Add the following line to your `requirements.txt` in your source environment:
    ```
    astronomer-starship==1.2.1
    ```

1. Add the following DAG to your source environment:
    ```python file="dags/astronomer_migration_dag.py"
    from airflow import DAG

    from astronomer.starship.operators import AstroMigrationOperator
    from datetime import datetime

    with DAG(
        dag_id="astronomer_migration_dag",
        start_date=datetime(2020, 8, 15),
        schedule_interval=None,
    ) as dag:

        AstroMigrationOperator(
            task_id="export_meta",
            deployment_url='{{ dag_run.conf["deployment_url"] }}',
            token='{{ dag_run.conf["astro_token"] }}',
        )
    ```
3. Deploy this DAG to your source Airflow environment, configured as described in the **Configuration** section below
4. Once the DAG is available in the Airflow UI, click the "Trigger DAG" button, then click "Trigger DAG w/ config", and input the following in the configuration dictionary:
    - `astro_token`: To retrieve anf Astronomer token, navigate to [cloud.astronomer.io/token](https://cloud.astronomer.io/token) and log in using your Astronomer credentials
    - `deployment_url`: To retrieve a deployment URL - navigate to the Astronomer Airlow deployment that you'd like to migrate to in the Astronomer UI, click `Open Airflow` and copy the page URL (excluding `/home` on the end of the URL)
        - For example, if your deployment URL is `https://astronomer.astronomer.run/abcdt4ry/home`, you'll use `https://astronomer.astronomer.run/abcdt4ry`
    - The config dictionary used when triggering the DAG should be formatted as:

    ```
    {
        "deployment_url": "your-deployment-url",
        "astro_token": "your-astro-token"
    }
    ```
5. Once the DAG successfully runs, your connections, variables, and environment variables should all be migrated to Astronomer

### Configuration

The `AstroMigrationOperator` can be configured as follows:

- `variables_exclude_list`: List the individual Airflow Variables which you **do not** want to be migrated. Any Variables not listed will be migrated to the desination Airflow deployment.
- `connection_exclude_list`: List the individual Airflow Connections which you **do not** want to be migrated. Any Variables not listed will be migrated to the desination Airflow deployment.
- `env_include_list`: List the individual Environment Variables which you **do** want to be migrated. Only the Environment Variables listed will be migrated to the desination Airflow deployment. None are migrated by default.

```
AstroMigrationOperator(
    task_id="export_meta",
    deployment_url='{{ dag_run.conf["deployment_url"] }}',
    token='{{ dag_run.conf["astro_token"] }}',
    variables_exclude_list=["some_var_1"],
    connection_exclude_list=["some_conn_1"],
    env_include_list=["FOO", "BAR"],
)
```
