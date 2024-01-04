This project welcomes contributions. All Pull Requests should include proper testing, documentation, and follow all existing checks and practices.

<!--TOC-->

- [IDE Setup](#ide-setup)
- [Pre-commit](#pre-commit)
- [Other helpful commands](#other-helpful-commands)
- [Setting a new version](#setting-a-new-version)
  - [Build+Release](#buildrelease)
  - [For dev (test pypi):](#for-dev-test-pypi)
    - [Setup:](#setup)
    - [Run:](#run)
    - [test:](#test)
- [Developing CICD](#developing-cicd)
- [Easily test the plugin in a local astro project](#easily-test-the-plugin-in-a-local-astro-project)
- [Alternatively](#alternatively)
- [Issues](#issues)
  - [SSL CERTIFICATE_VERIFY_FAILED](#ssl-certificate_verify_failed)
  - [Pytest Debugging](#pytest-debugging)

<!--TOC-->

# IDE Setup

Install `ruff` and `black` plugins for your IDE

# Pre-commit

Pre-commit is utilized to run common checks or linting.
Install it locally to prevent needing to fix things after they fail in CICD

```shell
make pre-commit-install
```

It will run when you commit, or you can run

```shell
pre-commit run
```

# Other helpful commands

Check out what is in the [Justfile](./justfile)
You can run `just help` for an overview

# Setting a new version

to set the version:

```
poetry version x.y.z
```

or edit it directly in pyproject.toml.
Always set a new version before merging a PR to `main` and creating a Release

## Build+Release

**NOTE: This automatically happens when a tag is pushed, you don't need to run this**
For production:
setup:

```shell
poetry config pypi-token.pypi pypi-PYPITOKENPYPITOKENPYPITOKEN
```

run:

```shell
poetry build
poetry publish
```

or

```shell
poetry --build publish
```

## For dev (test pypi):

### Setup:

```shell
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry config pypi-token.testpypi pypi-PYPITOKENPYPITOKENPYPITOKEN
```

### Run:

```shell
poetry build --publish -r testpypi
```

or

```shell
act push -W .github/workflows/test_publish.yml --container-architecture linux/amd64 -s TEST_PYPI_TOKEN=pypi-PYPITOKENTESTPYPITOKENTESTPYPITOKEN
```

or hit the "Run Workflow" button in Github Actions

### test:

by adding this to a `requirements.txt`

```shell
--index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ astronomer-starship
```

and add this to `airflow_settings.yaml` and then run `astro d object import`

```shell
airflow:
  connections:
    - conn_id: aws
      conn_type: aws
      conn_host:
      conn_schema:
      conn_login:
      conn_password:
      conn_port:
      conn_extra:
  pools:
    - pool_name: foo
      pool_slot: 999
      pool_description:
  variables:
    - variable_name: foo
      variable_value: bar
```

# Developing CICD

Use https://github.com/nektos/act to run and test CICD changes locally.

# Easily test the plugin in a local astro project

1. Make a new project `astro dev init`
2. Symlink in starship `ln -s /path/to/starship starship`
3. add the file `docker-compose.override.yml`
    ```yaml
    version: "3.1"
    services:
      webserver:
        volumes:
          - ./starship:/usr/local/airflow/starship:rw
        command: >
          bash -c 'if [[ -z "$$AIRFLOW__API__AUTH_BACKEND" ]] && [[ $$(pip show -f apache-airflow | grep basic_auth.py) ]];
            then export AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth ;
            else export AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.default ; fi &&
            { airflow users create "$$@" || airflow create_user "$$@" ; } &&
            { airflow sync-perm || airflow sync_perm ;} &&
            airflow webserver -d' -- -r Admin -u admin -e admin@example.com -f admin -l user -p admin
    ```
4. Edit the file `Dockerfile`
    ```Dockerfile
    FROM quay.io/astronomer/astro-runtime:8.4.0

    ENV AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=true
    ENV FLASK_ENV=development
    ENV FLASK_DEBUG=true

    COPY --chown=astro:astro --chmod=777 starship starship
    USER root
    RUN pip install --upgrade pip && pip install ./starship
    USER astro
    ```
5. Build with your symlink starship `tar -czh . | docker build -t local -`
6. Start (or restart) the astro project `astro dev <re>start -i local`

# Alternatively

you may be able to run flask directly,
see [this](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html#troubleshooting)


# Issues
## SSL CERTIFICATE_VERIFY_FAILED
If you see a message like `E   jwt.exceptions.PyJWKClientConnectionError: Fail to fetch data from the url, err: "<urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)>"`, do this: https://stackoverflow.com/a/58525755

## Pytest Debugging
`pytest-xdist` can prevent a debugger from attaching correctly due to it's distributed/non-local behavior
You can fix this by commenting out `--num-processes=auto` from `pyproject.toml` or running with `--dist no` to return to normal sequential pytest behavior
