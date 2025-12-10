# FAQ

## Airflow 1

**I'm on Airflow 1, can I use Starship?**

_No, Starship is only compatible with Airflow 2.x and above, see [Compatibility](../#compatability)._

## Test connections

**I'm on Airflow>=2.7 and can't test connections.**

_You must have `AIRFLOW__CORE__TEST_CONNECTION` set. See [release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#disable-default-allowing-the-testing-of-connections-in-ui-api-and-cli-32052)._

## Missing menu item in Google Cloud Composer

**I'm using Google Cloud Composer 2.x and Airflow 2.x and do not see the `Astronomer` menu and/or the Starship Airflow Plugin?**

_Run the following commands to ensure you are a privileged user._

```sh
gcloud config set project <PROJECT_NAME>
gcloud composer environments run <ENVIRONMENT_NAME> --location <LOCATION> users add-role -- -e <USER_EMAIL> -r Admin
```
