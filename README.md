Astronomer Starship
=============================
A suite of Apache Airflow utilities containing Plugins and Operators from Astronomer. The purpose of these utilities is to better assist customers migrating Variables, Connections, and Environment Variables to Astronomer hosted Airflow environments from MWAA, GCC, and OSS environments, as well as Astronomer Software and Nebula instances. 

Depending on the source environment, either the Webserver Plugin or the `AstroMigrationOperator` should be used for migrating these elements.  

**Note:** In order to use the Starship utilities, the source Airflow environment must be running Airflow 2.x


Choosing the right package
------------
- The [AstroMigrationOperator](https://github.com/astronomer/starship/tree/master/astronomer-starship-provider) should be used if migrating from a Google Cloud Composer 1 (with Airflow 2.x) or MWAA v2.0.2 environment. These environments do not support webserver plugins and will require using the `AstroMigrationOperator` to migrate Connections, Variables, and Environment Variables. 
- The [Starship Plugin](https://github.com/astronomer/starship/tree/master/astronomer-starship) should be used for migrating from all other environments including Goocle Cloud Composer 2, MWAA v2.2.2, OSS, and Astronomer Software/Nebula instances.


Sending connections to the a Secrets Store
-----------------
----
### AWS Secrets Manager 
To export to AWS Secrets manager you need to set the following Environment variables

    SECRETS_BACKEND_TYPE=AwsSecretsManager
    AWS_ACCESS_KEY_ID=<Your AWS Access KEY>
    AWS_SECRET_ACCESS_KEY=<Your AWS Secret Key>
    AWS_DEFAULT_REGION=<Your desired AWS Region>

The following are Environment Variables which are Optional along with their defaults

        VARIABLES_PREFIX=/airflow/variables
        CONNECTIONS_PREFIX=/airflow/connections
        SECRETS_SEPARATOR=/


### AWS Systems Manager Parameter Store
To export to AWS Systems Manager Parameter Store you need to set the following Environment variables

    SECRETS_BACKEND_TYPE=AwsSystemsManager
    AWS_ACCESS_KEY_ID=<Your AWS Access KEY>
    AWS_SECRET_ACCESS_KEY=<Your AWS Secret Key>
    AWS_DEFAULT_REGION=<Your desired AWS Region>

The following are Environment Variables which are Optional along with their defaults

        VARIABLES_PREFIX=/airflow/variables
        CONNECTIONS_PREFIX=/airflow/connections
        SECRETS_SEPARATOR=/


### Azure Key Vault
To export to AWS Secrets manager you need to set the following Environment variables

    SECRETS_BACKEND_TYPE=AzureKeyVault
    AZURE_CLIENT_ID=<Your Azure Client ID> # Found on App Registration page > 'Application (Client) ID'
    AZURE_TENANT_ID=<Your Azure Tenant ID> # Found on App Registration page > 'Directory (tenant) ID'
    AZURE_CLIENT_SECRET=<Your Azure Client Secret> # Found on App Registration Page > Certificates and Secrets > Client Secrets > 'Value'
    AZURE_VAULT_URL=<Your Azure Vault URL>

The following are Environment Variables which are Optional along with their defaults

        VARIABLES_PREFIX=airflow-variables
        CONNECTIONS_PREFIX=airflow-connections
        SECRETS_SEPARATOR=-


### Google Secrets Manager 
To export to Google Secrets manager you need to set the following Environment variables

    SECRETS_BACKEND_TYPE=GoogleSecretsManager
The following are Environment Variables which are Optional along with their defaults

        VARIABLES_PREFIX=airflow-variables
        CONNECTIONS_PREFIX=airflow-connections
        SECRETS_SEPARATOR=-

In addition to setting this Environment Variable a connection with Type `Google Cloud` with the ID `destination_gcp` must be created


