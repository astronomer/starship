# Astronomer Migration Provider

Apache Airflow Provider containing Operators from Astronomer. The purpose of these operators is to better assist customers migrating to Astronomer hosted Airflow environments from MWAA, GCC, OSS

## Installation
Install and update using [pip](https://pip.pypa.io/en/stable/getting-started/):
```text
pip install https://astro-migration-provider.s3.us-west-2.amazonaws.com/astronomer-migration-provider-0.1.1.tar.gz
```

## Usage
1. Add the following line to your `requirements.txt` in your source environment:
   ```text
    https://astro-migration-provider.s3.us-west-2.amazonaws.com/astronomer-migration-provider-0.1.1.tar.gz
    ```
2. Add the following DAG to your source environment:
    ```python
   from airflow import DAG
   
   from astronomer.migration.operators import AstroMigrationOperator
   from datetime import datetime
   
   with DAG(
      dag_id="astronomer_migration_dag",
      start_date=datetime(2020, 8, 15),
      schedule_interval="@once",
   ) as dag:
   
      AstroMigrationOperator(
          task_id='export_meta',
          deployment_url='{{ var.value.deployment_url }}',
          token='{{ var.value.astro_token }}',
          variables_exclude_list=["deployment_url", "astro_token"],
          connection_exclude_list=["some_conn_1"],
          env_include_list=["FOO", "BAR"]
      )
    ```
3. In the DAG params, update the default values for `astro_token` and `deployment_url`
4. Update the list of environment variable names under the `env_include_list` parameter that need to be migrated to Astronomer. Please note that if you have existing environment variables on Astronomer that are not included here - they will need to be recreated in Astronomer.
5. (Optional) - if there are any Airflow Variables or Airflow Connections that should NOT be migrated, add them to the `variable_exclude_list` & `connection_exclude_list` parameters.
6. Deploy these changes to your source Airflow environment
7. In the source Airflow environment, create the following Airflow variables:
   - `astro_token`:  To get user token for astronomer navigate to [cloud.astronomer.io/token](https://cloud.astronomer.io/token) and login using your Astronomer credentials
   - `deployment_url`: To retrieve a deployment URL - navigate to the deployment that you'd like to migrate to in the Astronomer UI, click `Open Airflow` and copy the page URL (excluding `/home` on the end of the URL)
8. Unpause the `astronomer_migration_dag` and let it run. Once the DAG successfully runs, your connections, variables, and environment variables should all be migrated to Astronomer
