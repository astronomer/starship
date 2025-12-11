<p align="center">
  <img
    width="200px" height="200px"
    src="https://raw.githubusercontent.com/astronomer/starship/main/docs/starship.svg"
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
    src="https://raw.githubusercontent.com/astronomer/starship/main/docs/starship_diagram.svg"
    alt="Logo of Spaceship"
  />
</p>

## Installation

```shell
pip install astronomer-starship
```

## Usage

1. Create a [Workspace](https://docs.astronomer.io/astro/manage-workspaces) in [Astro](https://cloud.astronomer.io/) or [Software](https://docs.astronomer.io/software) to hold Astro Deployments
2. [Create an Astro Deployment](https://docs.astronomer.io/astro/create-deployment) matching the source Airflow deployment configuration as possible
3. Run `astro dev init` with the [Astro CLI](https://docs.astronomer.io/astro/cli/overview) to create a [Astro Project](https://docs.astronomer.io/astro/cli/develop-project) locally in your terminal
4. Add any DAGs to the `/dags` folder in the Astro Project
5. Complete any additional setup required to convert your existing Airflow deployment to an Astro Project
6. [Install Starship](#installation) (and any additional Python Dependencies) to the Astro Project
7. [Install Starship](#installation) to your existing Airflow Deployment
8. [Deploy the Astro Project](https://docs.astronomer.io/astro/cli/astro-deploy) to the Astro Deployment with `astro deploy`
9. In the Airflow UI of the source Airflow deployment, navigate to the new `Astronomer` menu and select the `Migration Tool 🚀` option
10. Follow the UI prompts to migrate, or if needed, look at the instructions to use the Operator

## Compatability

| Source              | Compatible             |
|---------------------|------------------------|
| Airflow 1           | ❌                      |
| GCC 1 - Airflow 2.x | [Operator](./operator) |
| GCC 2 - Airflow 2.x | ✅                      |
| GCC 3 - Airflow 2.x & 3.x| ✅                 |
| MWAA - Airflow 2.0  | [Operator](./operator) |
| MWAA - Airflow 2.2+ | ✅                      |
| MWAA - Airflow 3.x  | ✅                      |
| OSS Airflow VM      | ✅                      |
| Astronomer Products | ✅                      |

## Supported Airflow Features

The following Airflow features are supported by the current version of Starship when running
on the corresponding minimum Airflow version.

| Feature | Minimum Airflow Version | Supported |
| --- | --- | --- |
| Connections | 2.0 | ✅ |
| Dag runs | 2.0 | ✅ |
| Environment variables | 2.0 | ✅ |
| Pools | 2.0 | ✅ |
| Task instances | 2.0 | ✅ |
| Variables | 2.0 | ✅ |
| Task Logs | 2.0 | ❌ |
| XComs | 2.0 | ❌ |
| Task instance history | 2.10 | ✅ |
| Assets | 3.0 | ❌ |
| Backfills | 3.0 | ❌ |
| Dag versions | 3.0 | ❌ |
| Task instance notes | 3.0 | ❌ |
| HITL | 3.1 | ❌ |
| Teams | 3.1 | ❌ |

## Security Notice

This project is an Airflow Plugin that adds custom API routes. Ensure your environments are correctly secured.

---

**Artwork**
Starship logo [by Lorenzo](https://thenounproject.com/lorenzo.verdenelli/) used with permission
from [The Noun Project](https://thenounproject.com/icon/starship-6088295/)
under [Creative Commons](https://creativecommons.org/licenses/by/3.0/us/legalcode).
