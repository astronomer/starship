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
          token='{{  dag_run.conf["astro_token"] }}',
      )

3. Update the list of environment variable names under the ``env_include_list`` parameter that need to be migrated to Astronomer. Please note that if you have existing environment variables on Astronomer that are not included here - they will need to be recreated in Astronomer.
4. (Optional) - if there are any Airflow Variables or Airflow Connections that should NOT be migrated, add them to the ``variable_exclude_list` & `connection_exclude_list`` parameters.
5. Deploy these changes to your source Airflow environment
6. In the source Airflow environment, create the following Airflow variables:

   - ``astro_token``:  To get user token for astronomer navigate to `cloud.astronomer.io/token <https://cloud.astronomer.io/token>`_ and login using your Astronomer credentials
   - ``deployment_url``: To retrieve a deployment URL - navigate to the deployment that you'd like to migrate to in the Astronomer UI, click ``Open Airflow`` and copy the page URL (excluding ``/home`` on the end of the URL)

7. Unpause the ``astronomer_migration_dag`` and let it run. Once the DAG successfully runs, your connections, variables, and environment variables should all be migrated to Astronomer

Limitations
-----------
If there are existing env variables on the targeted Astronomer environment (that don't exist in your source environment) they will be deleted when this runs.

Aeroscope
=========

Aeroscope is an Astronomer develop tool to get the status of your current airflow environment

Usage
-----
1. Add the following line to your ``requirements.txt`` in your source environment:

.. code-block:: bash

    astronomer-migration-provider

2. Add the following DAG to your source environment:

.. code-block:: python

    from datetime import datetime
    from astronomer.aeroscope.operators import AeroscopeOperator

    from airflow import DAG

    with DAG(
        dag_id="astronomer_aeroscope_dag",
        start_date=datetime(2020, 8, 15),
        schedule_interval=None,
    ) as dag:

        execute = AeroscopeOperator(
          task_id="execute",
          presigned_url='{{ dag_run.conf["presigned_url"] }}',
          email='{{ dag_run.conf["email"] }}',
        )

3. Ask your Astronomer Representive for a presigned url
4. Trigger the ``astronomer_aeroscope_dag`` DAG w/ the following config:

.. code-block:: json

  {"presigned_url":"<astronomer-provided-url>",
  "email": "<your_company_email>"}

   

     