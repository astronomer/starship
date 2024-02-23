<p align="center">
  <img
    width="200px" height="200px"
    src="https://raw.githubusercontent.com/astronomer/starship/main/starship.svg"
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
    src="https://raw.githubusercontent.com/astronomer/starship/main/starship_diagram.svg"
    alt="Logo of Spaceship"
  />
</p>

## Installation
```shell
pip install astronomer-starship
```

## Usage
1. Create a [Workspace](https://docs.astronomer.io/astro/manage-workspaces) in [Astro](https://cloud.astronomer.io/) or a [Software](https://docs.astronomer.io/software) installation to hold Astro Deployments
2. [Create an Astro Deployment](https://docs.astronomer.io/astro/create-deployment) matching the source Airflow deployment configuration as possible
3. Run `astro dev init` with the [Astro CLI](https://docs.astronomer.io/astro/cli/overview) to create a [Astro Project](https://docs.astronomer.io/astro/cli/develop-project) locally in your terminal
4. Add any DAGs to the `/dags` folder in the Astro Project
5. Complete any additional setup required to convert your existing Airflow deployment to an Astro Project
5. [Install Starship](#installation) (and any additional Python Dependencies) to the Astro Project
6. [Install Starship](#installation) to your existing Airflow Deployment
4. [Deploy the Astro Project](https://docs.astronomer.io/astro/cli/astro-deploy) to the Astro Deployment with `astro deploy`
7. In the Airflow UI of the source Airflow deployment, navigate to the new `Astronomer` menu and select the `Migration Tool 🚀` option
8. Follow the UI prompts to migrate, or if needed, look at the instructions to use the Operator

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
Starship logo [by Lorenzo](https://thenounproject.com/lorenzo.verdenelli/) used with permission
from [The Noun Project](https://thenounproject.com/icon/starship-6088295/)
under [Creative Commons](https://creativecommons.org/licenses/by/3.0/us/legalcode).
