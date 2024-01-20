This project welcomes contributions. All Pull Requests should include proper testing, documentation, and follow all existing checks and practices.

<!--TOC-->

- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
  - [Versioning](#versioning)
  - [Linting](#linting)
  - [Testing](#testing)
  - [Development](#development)
    - [Pre-Commit](#pre-commit)
    - [IDE Setup](#ide-setup)
- [Pre-commit](#pre-commit-1)
- [Other helpful commands](#other-helpful-commands)
- [Test](#test)
- [Developing CICD](#developing-cicd)
- [Easily test the plugin in a local astro project](#easily-test-the-plugin-in-a-local-astro-project)
- [Alternatively](#alternatively)
- [Issues](#issues)
  - [SSL CERTIFICATE_VERIFY_FAILED](#ssl-certificate_verify_failed)
  - [Pytest Debugging](#pytest-debugging)

<!--TOC-->
# Project Structure
- [`astronomer_starship/index.html`](astronomer_starship/index.html) is used for development,
and `vite build` uses it to compile the Javascript and CSS into the `static` folder
- [`astronomer_starship/src`](astronomer_starship/src) contains the React App
- [`astronomer_starship/starship.py`](astronomer_starship/starship.py) contains the Airflow Plugin to inject the React App
    - [`astronomer_starship/template/index.html`](astronomer_starship/templates/index.html) contains the Flask View HTML
- [`astronomer_starship/starship_api.py`](astronomer_starship/starship_api.py) contains the Airflow Plugin to that provides
the Starship API Routes

# Development Workflow

1. Create a branch off `main`
2. Develop, add tests, ensure all tests are passing
3. Push up to GitHub (running pre-commit)
4. Create a PR, get approval
5. Merge the PR to `main`
6. On `main`: Create a tag
    ```shell
    VERSION="v$(python -c 'import astronomer_starship; print(astronomer_starship.__version__)')"; git tag -d $VERSION; git tag $VERSION
    ```
7. Do any manual or integration testing
8. Push the tag to GitHub `git push origin --tag`, which will create
   a `Draft` [release](https://github.com/astronomer/astronomer-starship/releases) and upload
   to [test.pypi.org](https://test.pypi.org/project/astronomer-starship/) via CICD
9. Approve the [release](https://github.com/astronomer/astronomer-starship/releases) on GitHub, which
   will upload to [pypi.org](https://pypi.org/project/astronomer-starship/) via CICD

## Versioning
This project follows [Semantic Versioning](https://semver.org/)

## Linting

This project
uses [`black` (link)](https://black.readthedocs.io/en/stable/), [`blacken-docs` (link)](https://github.com/adamchainz/blacken-docs),
and [`ruff` (link)](https://beta.ruff.rs/). They run with pre-commit but you can run them directly with `ruff check .`
in the root.

## Testing

This project utilizes [Doctests](https://docs.python.org/3/library/doctest.html) and `pytest`.
With the `dev` extras installed, you can run all tests with `pytest` in the root of the project. It will automatically
pick up it's configuration in `pyproject.toml`

## Development

### Pre-Commit

Pre-commit is utilized to run common checks or linting.
Install it locally to prevent needing to fix things after they fail in CICD

- This project uses pre-commit
- Install it with

```shell
pre-commit install
```

### IDE Setup

Install `ruff` and `black` plugins for your IDE

# Pre-commit

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

# Test

The package can be built and manually pushed to Test PyPi.
Note: `twine` must be installed
Further instructions are [here](https://packaging.python.org/en/latest/specifications/pypirc/#the-pypirc-file)
and [here](https://packaging.python.org/en/latest/guides/using-testpypi/)

Test by adding this to a `requirements.txt`

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

    COPY --chown=astro:astro --chmod=777 starship starship
    USER root
    RUN pip install --upgrade pip && pip install ./starship
    USER astro
    ```
5. Build with your symlink starship `tar -czh . | docker build -t local -`
6. Start (or restart) the astro project `astro dev <re>start -i local`
    1. Quickly restart just the webserver with `docker restart $(docker container ls --filter name=webserver --format="{{.ID}}")`

# Alternatively

you may be able to run flask directly,
see [this](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html#troubleshooting)


# Issues
## SSL CERTIFICATE_VERIFY_FAILED
If you see a message like `E   jwt.exceptions.PyJWKClientConnectionError: Fail to fetch data from the url, err: "<urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)>"`, do this: https://stackoverflow.com/a/58525755

## Pytest Debugging
`pytest-xdist` can prevent a debugger from attaching correctly due to it's distributed/non-local behavior
You can fix this by commenting out `--num-processes=auto` from `pyproject.toml` or running with `--dist no` to return to normal sequential pytest behavior
