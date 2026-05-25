# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Starship is an Airflow plugin (Python + React) that migrates Airflow metadata — Variables, Connections, Env Vars, Pools, DAG History — from a source Airflow instance to an Astro/Airflow target. It ships as a PyPI package (`astronomer-starship`) and registers two Airflow plugins via entry points: `starship` (UI) and `starship_api` (REST endpoints).

## Common commands

The project is driven by [`just`](./justfile). `just help` / `just --list` shows everything.

Backend:
- `just install` — clean, install frontend (`npm install` in `astronomer_starship/`) and backend (`pip install -c constraints.txt '.[dev]'`)
- `just test-backend` — pytest (uses `pyproject.toml` config, runs doctests, includes `--doctest-modules`)
- `pytest tests/path/to/test_x.py::test_name` — single test (note pytest config lives in `pyproject.toml`, `testpaths = ["tests"]`)
- `just lint-backend` / `ruff check` / `ruff format`

Frontend (run from repo root via just, or `cd astronomer_starship` for npm scripts):
- `just build-frontend` — `vite build` into `astronomer_starship/static/`
- `just build-frontend-watch` — rebuild on changes
- `just test-frontend` / `just test-frontend-watch` — vitest
- `cd astronomer_starship && npm run lint` — eslint
- `cd astronomer_starship && npm run format` — prettier

Local dev (full Airflow stack):
- `just dev2 start` / `just dev3 start` — boot a local Astro project with editable Starship installed (Airflow 2 or 3). Delegates to `dev2/justfile` / `dev3/justfile`.
- `just dev2 reload` / `just dev3 reload` — rebuild frontend and restart the webserver container only (fast iteration on UI/plugin code)
- `just dev2 deploy <deployment-id>` / `just dev3 deploy <deployment-id>` — deploy local Starship to a remote Astro deployment
- `just dev2 logs` / `just dev2 bash` — webserver logs / shell

Docs: `just serve-docs` (mkdocs).

Pre-commit is configured (`prek` or `pre-commit`); `ruff`, `bandit`, `detect-secrets`, and a local `prettier` hook for `astronomer_starship/{src,tests}/` all run.

## Architecture

### Airflow version compatibility split

This is the central architectural pattern. Starship supports both Airflow 2 and Airflow 3, which have **fundamentally different plugin APIs** (Flask/FlaskAppBuilder vs FastAPI). The codebase handles this with a compatibility layer:

- `astronomer_starship/compat/__init__.py` — detects the installed Airflow major version at import time and re-exports `StarshipPlugin`, `StarshipApi`, `StarshipAPIPlugin`, `StarshipCompatabilityLayer` from either `_af2/` or `_af3/`.
- `astronomer_starship/_af2/` — Airflow 2 implementation. UI is a Flask `BaseView` registered via `flask_blueprints` + `appbuilder_views`. Auth uses `airflow.www.auth.has_access` with FAB permissions.
- `astronomer_starship/_af3/` — Airflow 3 implementation. UI is a `FastAPI` app registered via the plugin's `fastapi_apps` + `external_views`. Auth differs accordingly.
- `astronomer_starship/starship.py` and `starship_api.py` — thin top-level shims that import from `compat/`. These are what the entry points in `pyproject.toml` point at.
- `astronomer_starship/common.py` — Airflow-version-agnostic helpers (`HttpError`, `get_kwargs_fn`, `telescope`, attr descriptors).

**When adding/changing API behavior, you almost always need to touch BOTH `_af2/` and `_af3/` mirror files.** The `StarshipCompatabilityLayer` in each subpackage encapsulates the version-specific ORM differences (e.g., model field names that changed between AF2 and AF3).

### Plugin entry points (`pyproject.toml`)

- `airflow.plugins` → `starship = astronomer_starship.starship:StarshipPlugin` and `starship_api = astronomer_starship.starship_api:StarshipAPIPlugin`
- `apache_airflow_provider` → `provider_info = astronomer_starship.__init__:get_provider_info`

### Proxy pattern

The React app talks to the *target* Airflow via a `/proxy` endpoint on the *source* Airflow plugin (CORS workaround). The proxy accepts a `Starship-Proxy-Token` header, forwards the request to a `?url=` query param with `Authorization: Bearer <token>`. In AF3 it also handles GZIP, the `/api/v1/health` → `/api/v2/monitor/health` URL rewrite, and Retry-After forwarding. Token-based auth is enforced; admin-equivalent access is required to use Starship.

### Frontend

- `astronomer_starship/src/` — React 18 + Chakra UI + react-router. Pages in `src/pages/` (SetupPage, VariablesPage, ConnectionsPage, PoolsPage, EnvVarsPage, DAGHistoryPage, TelescopePage). Reusable components in `src/component/`.
- `index.html` is the dev entrypoint; `vite build` outputs to `astronomer_starship/static/` which is shipped in the wheel via `[tool.setuptools.package-data]`.
- The AF2 plugin serves the React app via Flask template `templates/index.html`; the AF3 plugin serves `static/index.html` directly via FastAPI `FileResponse`.

### Operator package (programmatic migration)

`astronomer_starship/providers/starship/` ships hooks and operators for users who want to run migrations from within a DAG instead of clicking the UI. `operators/starship.py` branches on `AIRFLOW_V_2` / `AIRFLOW_V_3` for the `DAG`, `BaseOperator`, `TaskGroup`, and `task` imports (AF3 uses `airflow.sdk`). `hooks/starship.py` defines `StarshipLocalHook` (reads from local Airflow) and `StarshipHttpHook` (writes to remote via the Starship API).

### Tests

- `tests/` — pytest. `tests/conftest.py` defines a `manual_tests` skip marker gated on `MANUAL_TESTS` env var. `tests/docker_test/` and `tests/resources/` are excluded from collection (`norecursedirs`). `tests/e2e/{gcc,mwaa}/` are integration scaffolds run via their own justfiles.
- `astronomer_starship/tests/` — vitest tests for the React frontend.
- Both AF2 and AF3 implementations are exercised — running tests requires `apache-airflow<3.2` from the dev extra. The `--doctest-modules` flag means docstrings with `>>>` examples run as tests.

## Constraints worth knowing

- `requires-python = ">=3.6"` (the package still supports the very old Pythons that older Airflow 2 deployments ran on). Ruff is pinned to `target-version = "py37"` accordingly. Don't use 3.8+ syntax (walrus, f-string `=`, etc.) in shipped code.
- Starship only supports Airflow up to 3.1 right now (`apache-airflow<3.2` in dev extra).
- Releases: a release manager runs `just release MAJOR|MINOR|PATCH` from an up-to-date `main` branch. This uses [commitizen](https://commitizen-tools.github.io/commitizen/) to bump `__version__` in `astronomer_starship/__init__.py`, create a bump commit and a `v<version>` tag, and push both to GitHub. CI then builds and publishes the package directly to PyPI.
