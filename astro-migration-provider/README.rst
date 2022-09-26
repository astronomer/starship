Astronomer Migration Provider
=============================

Apache Airflow Provider containing Operators from Astronomer. The purpose of these operators is to better assist customers migrating to Astronomer hosted Airflow environments from MWAA, GCC, OSS. This provider is meant for MWAA 2.0.2 and Composer 1 since the plugin methods are unavailable.

Installation
------------

Install and update using `pip <https://pip.pypa.io/en/stable/getting-started/>`_:

.. code-block:: bash

    pip install astronomer-migration-provider



Starship
========

Starship is an Astronomer utility that assist end users with migrating Variables, Connections, and Environment Variables from their source Airflow environments to an Astronomer environment.

Usage
-----
1. Add the following line to your ``requirements.txt`` in your source environment:

.. code-block:: bash

    astronomer-migration-provider

2. Add the following DAG to your source environment:

.. code-block:: python

   from airflow import DAG

   from astronomer.migration.operators import AstroMigrationOperator
   from datetime import datetime

   with DAG(
      dag_id="astronomer_migration_dag",
      start_date=datetime(2020, 8, 15),
      schedule_interval=None,
   ) as dag:

      AstroMigrationOperator(
          task_id='export_meta',
          deployment_url='{{ dag_run.conf["deployment_url"] }}',
          token='{{ dag_run.conf["astro_token"] }}',
      )

3. Deploy this DAG to your source Airflow environment, configured as described in the section below
4. Hit the Trigger DAG button in the Airflow UI when the DAG appears, and input the following in the configuration dictionary:
   - ``astro_token``:  To get user token for astronomer navigate to `cloud.astronomer.io/token <https://cloud.astronomer.io/token>`_ and login using your Astronomer credentials
   - ``deployment_url``: To retrieve a deployment URL - navigate to the deployment that you'd like to migrate to in the Astronomer UI, click ``Open Airflow`` and copy the page URL (excluding ``/home`` on the end of the URL)

5. Once the DAG successfully runs, your connections, variables, and environment variables should all be migrated to Astronomer

Configuration
--------------
The `AstroMigrationOperator` can be configured as follows:
-  You can update the list of environment variable names under the ``env_include_list`` parameter that need to be migrated to Astronomer. None are migrated by default.
- if there are any Airflow Variables or Airflow Connections that should NOT be migrated, add them to the ``variable_exclude_list`` & ``connection_exclude_list`` parameters.

.. code-block:: python

      AstroMigrationOperator(
          task_id='export_meta',
          deployment_url='{{ dag_run.conf["deployment_url"] }}',
          token='{{ dag_run.conf["astro_token"] }}',
          variables_exclude_list=["deployment_url", "astro_token"],
          connection_exclude_list=["some_conn_1"],
          env_include_list=["FOO", "BAR"]
      )


   

     
