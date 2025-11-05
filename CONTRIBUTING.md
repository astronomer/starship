# Contributing

This project welcomes contributions. All Pull Requests should include proper testing, documentation, and follow all existing checks and practices.

<!--TOC-->

- [Contributing](#contributing)
  - [Project Structure](#project-structure)
  - [Development Workflow](#development-workflow)
    - [Versioning](#versioning)
    - [Testing](#testing)
    - [Pre-Commit](#pre-commit)
      - [prek](#prek)
    - [Ruff](#ruff)
  - [Other helpful commands](#other-helpful-commands)
  - [Test](#test)
  - [Developing CICD](#developing-cicd)
  - [Run current code](#run-current-code)
    - [Local](#local)
    - [Astro](#astro)
  - [Alternatively](#alternatively)
  - [Issues](#issues)
    - [SSL CERTIFICATE_VERIFY_FAILED](#ssl-certificate_verify_failed)
    - [Pytest Debugging](#pytest-debugging)

<!--TOC-->
## Project Structure

- [`astronomer_starship/index.html`](astronomer_starship/index.html) is used for development,
and `vite build` uses it to compile the Javascript and CSS into the `static` folder
- [`astronomer_starship/src`](astronomer_starship/src) contains the React App
- [`astronomer_starship/starship.py`](astronomer_starship/starship.py) contains the Airflow Plugin to inject the React App
- [`astronomer_starship/template/index.html`](astronomer_starship/templates/index.html) contains the Flask View HTML
- [`astronomer_starship/starship_api.py`](astronomer_starship/starship_api.py) contains the Airflow Plugin to that provides
the Starship API Routes

## Development Workflow

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

### Versioning

This project follows [Semantic Versioning](https://semver.org/)

### Testing

This project utilizes [Doctests](https://docs.python.org/3/library/doctest.html) and `pytest`.
With the `dev` extras installed, you can run all tests with `pytest` in the root of the project. It will automatically
pick up it's configuration in `pyproject.toml`

### Pre-Commit

[Pre-commit](https://pre-commit.com/) is utilized to run common checks or linting.

```bash
brew install pre-commit
```

Activate it locally for the project in order to enable automatic checks for changes before committing.

```shell
pre-commit install
```

You can also run pre-commit checks for all files.

```shell
pre-commit run --all-files
```

#### prek

[prek](https://github.com/j178/prek) is an alternative to pre-commit written in Rust.

```shell
brew install prek
```

It is being used in the same way is pre-commit.

```shell
prek install

prek run --all-files
```

### Ruff

This project uses [`ruff`](https://docs.astral.sh/ruff/) for linting and formatting.

```shell
brew install ruff
```

It is automatically run with pre-commit but you can also run it directly,

```shell
ruff check

ruff format
```

You may install the corresponding `ruff` plugin for your IDE.

## Other helpful commands

Check out what is in the [Justfile](./justfile)
You can run `just help` for an overview

## Test

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

## Developing CICD

Use <https://github.com/nektos/act> to run and test CICD changes locally.

## Run current code

Checkout [dev](./dev) for options of running the local version of Starship in Airflow.

```shell
just dev
```

### Local

Spin up a local astro dev environment with the local version of Starship installed.

```shell
just dev start
```

Quickly reload the webserver with refreshed assets and plugin code.

```shell
just dev reload
```

### Astro

Deploy to a deployment in Astro with the local version of Starship installed.

```shell
just dev deploy mydeploymentid
```

## Alternatively

you may be able to run flask directly,
see [this](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html#troubleshooting)

## Issues

### SSL CERTIFICATE_VERIFY_FAILED

If you see a message like `E   jwt.exceptions.PyJWKClientConnectionError: Fail to fetch data from the url, err: "<urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)>"`, do this: <https://stackoverflow.com/a/58525755>

### Pytest Debugging

`pytest-xdist` can prevent a debugger from attaching correctly due to it's distributed/non-local behavior
You can fix this by commenting out `--num-processes=auto` from `pyproject.toml` or running with `--dist no` to return to normal sequential pytest behavior
