# IDE Setup
Install `ruff` and `black` plugins for your IDE

# Pre-commit
Pre-commit is utilized to run common checks or linting.
Install it locally to prevent needing to fix things after they fail in CICD
```shell
pre-commit install
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

For production:
```shell
poetry build
poetry publish
```
or
```shell
poetry --build publish
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
