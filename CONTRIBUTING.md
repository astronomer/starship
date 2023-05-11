# IDE Setup
Install `ruff` and `black` plugins for your IDE

# Pre-commit
Pre-commit is utilized to run common checks or linting.
Install it locally to prevent needing to fix things after they fail in CICD
```shell
pre-commit install
```
It will run when you commit, or you can run
```shell
pre-commit run
```

# Tagging A Release
```
git tag $(TAG)
git push origin $(TAG)
```

## Deleting a Tag
```
git tag -d $(TAG)
git push origin --delete $(TAG)
```

# Building, Releasing
to set the version:
```
poetry version
```

NOTE: This automatically happens when a tag is pushed, you don't need to run this
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

For dev (test pypi):
setup:
```shell
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry config pypi-token.testpypi pypi-PYPITOKENPYPITOKENPYPITOKEN
```
run:
```shell
poetry build --publish -r testpypi
```
or
```shell
act push -W .github/workflows/test_publish.yml --container-architecture linux/amd64 -s TEST_PYPI_TOKEN=pypi-PYPITOKENTESTPYPITOKENTESTPYPITOKEN
```
or hit the "Run Workflow" button in Github Actions

then test, by adding this to a `requirements.txt`
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
2. add the file `docker-compose.override.yml`
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
3. Edit the file `Dockerfile`
    ```Dockerfile
    FROM quay.io/astronomer/astro-runtime:7.3.0

    ENV AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=true
    ENV FLASK_ENV=development
    ENV FLASK_DEBUG=true

    COPY --chown=astro:astro --chmod=777 starship starship
    USER root
    RUN pip install -e ./starship/
    USER astro
    ```
