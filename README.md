Astronomer Starship
=============================
A suite of Apache Airflow Provider packages containing Plugins and Operators from Astronomer. The purpose of these utilities is to better assist customers migrating Variables, Connections, and Environment Variables to Astronomer hosted Airflow environments from MWAA, GCC, and OSS environments, as well as Astronomer Software and Nebula instances. 

Depending on the source environment, either the Webserver Plugin or the `AstroMigrationOperator` should be used for migrating these elements.  

**Note:** In order to use the Starship utilities, the source Airflow environment must be running Airflow 2.x

Choosing the right package
------------
- The [AstroMigrationOperator](https://github.com/astronomer/starship/tree/master/astronomer-starship-provider) should be used if migrating from a Google Cloud Composer 1 (with Airflow 2.x) or MWAA v2.0.2 environment. These environments do not support webserver plugins and will require using the `AstroMigrationOperator` to migrate Connections, Variables, and Environment Variables. 
- The [Starship Plugin](https://github.com/astronomer/starship/tree/master/astronomer-starship) should be used for migrating from all other environments including Goocle Cloud Composer 2, MWAA v2.2.2, OSS, and Astronomer Software/Nebula instances.
