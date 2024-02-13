<p align="center">
  <img
    width="200px" height="200px"
    src="https://raw.githubusercontent.com/astronomer/starship/v2/starship.svg"
    alt="Logo of Spaceship"
  />
</p>
<p align="center">
  <b>Astronomer Starship</b> can send your Airflow workloads to new places!
</p>

## What is it?

Starship is a utility to migrate Airflow metadata such as Airflow Variables,
Connections, Environment Variables, Pools, and DAG History between two Airflow instances.

<p align="center">
  <img
    width="600px" height="200px"
    src="https://raw.githubusercontent.com/astronomer/starship/v2/starship_diagram.svg"
    alt="Logo of Spaceship"
  />
</p>

## Installation
```shell
pip install astronomer-starship
```

## Usage
1. Create a Workspace in Astro to hold your Deployments
2. [Create an Airflow Deployment](https://cloud.astronomer.io/) to match your existing Airflow
3. Run `astro dev init` with the [Astro CLI](https://docs.astronomer.io/astro/cli/overview) to create a new Airflow Deployment locally
4. Deploy your DAGs to the new Airflow Deployment with `astro deploy`
5. [Install Starship](#installation) (and any additional Python Dependencies) to your new Airflow Deployment
6. [Install Starship](#installation) to your existing Airflow Deployment
7. In the Airflow UI, navigate to the new `Astronomer` menu and select the `Migration Tool 🚀` option
8. Follow the prompts to migrate your metadata, or if needed, look at the instructions to use the Operator

## Compatability

| Source              | Compatible             |
|---------------------|------------------------|
| Airflow 1           | ❌                      |
| GCC 1 - Airflow 2.x | [Operator](./operator) |
| GCC 2 - Airflow 2.x | ✅                      |
| MWAA v2.0.2         | [Operator](./operator) |
| MWAA ≥ v2.2.2       | ✅                      |
| OSS Airflow VM      | ✅                      |
| Astronomer Products | ✅                      |


## FAQ
- **I'm on Airflow 1, can I use Starship?**

    _No, Starship is only compatible with Airflow 2.x and above_, see [Compatibility](#compatability)

- **I'm on Airflow>=2.7 and can't test connections?**

  _You must have `AIRFLOW__CORE__TEST_CONNECTION` set. See notes [here](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#disable-default-allowing-the-testing-of-connections-in-ui-api-and-cli-32052)_

- **I'm using Google Cloud Composer 2.x and Airflow 2.x and do not see the `Astronomer` menu and/or the Starship Airflow Plugin?**

    _Run the following to ensure you are a privileged user._
    ```
    gcloud config set project <PROJECT_NAME>
    gcloud composer environments run <ENVIRONMENT_NAME> --location <LOCATION> users add-role -- -e <USER_EMAIL> -r Admin
    ```

## Security Notice
This project is an Airflow Plugin that adds custom API routes. Ensure your environments are correctly secured.

---

**Artwork**
Orbiter logo [by Lorenzo](https://thenounproject.com/lorenzo.verdenelli/) used with permission
from [The Noun Project](https://thenounproject.com/icon/starship-6088295/)
under [Creative Commons](https://creativecommons.org/licenses/by/3.0/us/legalcode).
