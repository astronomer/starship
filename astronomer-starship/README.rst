Astronomer Starship Plugin
==========================

Apache Airflow Plugin meant to assist customers migrating Variables, Connections, and Environment Variables from a source Airflow environment to an Astronomer environment.

Initial Setup
-------------
1. Add the following line to your ``requirements.txt`` in your source environment:

.. code-block:: bash

    astronomer-starship

2. Once your source environment installs ``astronomer-starship`` via the ``requirements.txt``, you will see a new Astronomer menu. Hover over that menu and select the ``Migration Tool 🚀`` option

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/menu-item.png
   :width: 800

3. After opening the plugin page, you will need to authenticate to Astronomer. To do this, please:

    a. Click the ``Get Token`` button
    b. If you are prompted to sign-in to cloud.astronomer.io please do so
    c. Copy the access token that appears in the new tab
    d. Paste the access token into the ``Authentication Token`` field
    e. Click the ``Sign In`` button

4. After authenticating to Astronomer, you will need to select the deployment that you are sending metadata to. To do this, select a deployment from the ``Target Deployment`` dropdown and click the ``Select`` button

Migrating Airflow Connections
-----------------------------

To migrate connections from your source Airflow meta-database:

1. Click on the ``Connections`` tab:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/connections-tab.png
   :width: 800

2. In the table displaying the connections that can be migrated, click the ``Migrate`` button for each connection that needs to be sent to the Target Deployment:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/connections-migrate.png
   :width: 800

3. Once the ``Migrate`` button is clicked, the connection will be sent to the Target Deployment and will show as ``Migrated ✅`` in the plugin UI:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/connections-migrate-complete.png
   :width: 800

Migrating Airflow Variables
---------------------------

To migrate variables from your source Airflow meta-database:

1. Click on the ``Variables`` tab:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/variables-tab.png
   :width: 800

2. In the table displaying the variables that can be migrated, click the ``Migrate`` button for each connection that needs to be sent to the Target Deployment

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/variables-migrate.png
   :width: 800

3. Once the ``Migrate`` button is clicked, the variable will be sent to the Target Deployment and will show as ``Migrated ✅`` in the plugin UI:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/variables-migrate-complete.png
   :width: 800

Migrating Environment Variables
-------------------------------

To migrate environment variables from your source Airflow:

1. Click on the ``Environment Variables`` tab:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/env-tab.png
   :width: 800

2. In the table displaying the environment variables that can be migrated, ensure the checkbox is ticked for each environment variable that needs to be sent to the Target Deployment

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/env-migrate.png
   :width: 800

3. Once all of the desired environment variable checkboxes have been selected, click the ``Migrate`` button in the table header

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/env-migrate-button.png
   :width: 800

4. After clicking the ``Migrate`` button in the table header, each selected environment variable will be sent to the Target Deployment and the ticked checkbox will display ``Migrated ✅``

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/env-migrate-complete.png
   :width: 800

Utilizing DAGs Cutover Tab
--------------------------

The DAGs Cutover Tab can be utilized to pause DAGs in the source environment and unpause DAGs in the target environment (as long as the DAG id in both the source and target environment match). To do so, please:

1. Click on the ``DAGs Cutover`` tab:

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/cutover-tab.png
   :width: 800

2. In the table displaying the DAGs present in both the source and target environments, click the Pause icon under ``Local``

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/cutover-pause-local.png
   :width: 800

3. In the table displaying the DAGs present in both the source and target environments, click the Start icon under ``Remote``

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/cutover-start-remote.png
   :width: 800

4. After completing this process, you will see the DAG is paused in the ``Local`` environment (so you will see a Start Icon) and is unpaused in the ``Remote`` environment (so you will see a Pause icon)

.. image:: https://github.com/astronomer/starship/raw/master/astronomer-starship/images/cutover-complete.png
   :width: 800


Astronomer Aeroscope Provider
--------------------------

The Astronomer Aeroscope Provider contains a Plugin and Operator from Astronomer. 
These provide a method to run Telescope in environments where other methods are unavailable 

Usage Option 1) Plugin Usage
- Navigate to the top navigation bar in your Source Airflow environment.
- Click the `Astronomer` menu, then `Run Report`

Usage Option 2) Operator Usage
- Add the following DAG to your source Airflow environment
.. code-block:: python

   from datetime import datetime

   from airflow.models import DAG
   from astronomer.aeroscope.operators import AeroscopeOperator

   with DAG(
   dag_id="astronomer_aeroscope",
   schedule_interval=None,
   start_date=datetime(2021, 1, 1),
   ) as dag:
   AeroscopeOperator(
         task_id="run_report",
         presigned_url='{{ dag_run.conf["presigned_url"] }}',
         organization='{{ dag_run.conf["organization"] }}',
   )

- Trigger the `astronomer_aeroscope` DAG with the configuration given by your Astronomer Representative

.. code-block:: json
   {
      "presigned_url": "presigned-url-here",
      "organization": "organization-name-here"
   }




License
-------

`License <LICENSE.txt>`_