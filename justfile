#!/usr/bin/env just --justfile
set dotenv-load := true
SRC_DIR := "astronomer_starship"
DOCS_DIR := "docs"
VERSION := `echo $(python -c 'from astronomer_starship import __version__; print(__version__)')`

default:
    @just --choose

# Print this help text
help:
    @just --list

# install frontend requirements
install-frontend:
    cd astronomer_starship && npm install

# install backend requirements
install-backend EDITABLE="":
    pip install {{EDITABLE}} -c constraints.txt '.[dev]'

# Install the project
install: clean install-frontend install-backend

# Run container test via docker
test-run-container-test IMAGE="apache/airflow:2.3.4":
    docker run --entrypoint=bash -e DOCKER_TEST=True \
      -v "$HOME/starship:/usr/local/airflow/starship:rw" \
      {{IMAGE}} -- \
      /usr/local/airflow/starship/tests/docker_test/run_container_test.sh \
      {{IMAGE}}

# Test Starship Python API
test-backend:
    pytest -c pyproject.toml

# Test Starship Python API with coverage
test-backend-with-coverage:
    pytest -c pyproject.toml --cov=./ --cov-report=xml

# Test Starship Webapp Frontend
test-frontend:
    cd astronomer_starship && npx vitest run

# Test the frontend and retest while watching for changes
test-frontend-watch:
    cd astronomer_starship && npx vitest watch

# Run unit tests
test: test-frontend test-backend

# Run unit tests with coverage
test-with-coverage: test-frontend test-backend-with-coverage

# Run integration tests
test-integration $MANUAL_TESTS="true":
    @just test

# Test CICD setup with `act`
test-cicd:
    act pull-request -W .github/workflows/checks.yml --container-architecture linux/amd64

# Lint the frontend code
lint-frontend:
    cd astronomer_starship && npx eslint .. --ext js,jsx --report-unused-disable-directives --max-warnings 0

# Lint the backend code
lint-backend:
    ruff -c pyproject.toml

# Run all linting
lint: lint-frontend lint-backend

# Build Starship Webapp Frontend
build-frontend: clean-frontend-build
    cd astronomer_starship && npx vite build

# Build the frontend and rebuild while watching for changes
build-frontend-watch:
    cd astronomer_starship && npx vite build --watch

# Build Starship Package
build-backend: clean-backend-build
    python -m build

# Build the project
build: install clean build-frontend build-backend

# Clean up any temp and dependency directories
clean-backend-build:
    rm -rf dist dist
    rm -rf *.egg-info

# Clean artifacts created after building the frontend
clean-frontend-build:
    rm -rf astronomer_starship/static

# Clean artifacts used by the frontend
clean-frontend-install:
    rm -rf astronomer_starship/node_modules

# Clean everything
clean: clean-backend-build clean-frontend-build clean-frontend-install

# Tag as v$(<src>.__version__) and push to GH
tag:
    # Delete tag if it already exists
    git tag -d v{{VERSION}} || true
    # Tag and push
    git tag v{{VERSION}}

# Deploy the project
deploy-tag: tag
    git push origin v{{VERSION}}

# Deploy the project
deploy: deploy-tag

# Upload to TestPyPi for testing (note: you can only use each version once)
upload-testpypi: clean install build
    python -m twine check dist/*
    TWINE_USER=${TWINE_USER} TWINE_PASS=${TWINE_PASS} python -m twine upload --repository testpypi dist/*

# Upload to PyPi - DO NOT USE THIS, GHA DOES THIS AUTOMATICALLY
upload-pypi: clean install build
    python -m twine check dist/*
    TWINE_USER=${TWINE_USER} TWINE_PASS=${TWINE_PASS} python -m twine upload dist/*

# create a test project at a path
create-test TESTPATH:
    #!/usr/bin/env bash
    set -euxo pipefail
    if [[ -d {{TESTPATH}} ]]
    then
      echo "starship_scratch already exists"
    else
      mkdir -p {{TESTPATH}}
      ln -sf $(pwd) {{ TESTPATH }}/starship
      cd {{TESTPATH}}
      astro dev init <<< 'y'
      echo "COPY --chown=astro:astro --chmod=777 starship /usr/local/starship" >> Dockerfile
      echo "USER root" >> Dockerfile
      echo "RUN pip install --upgrade pip && pip install -e '/usr/local/starship'" >> Dockerfile
      echo "USER astro" >> Dockerfile
      echo "# ALTERNATIVELY, COMMENT THIS OUT AND ADD TO REQUIREMENTS.TXT" >> Dockerfile
      echo "# --extra-index-url=https://test.pypi.org/simple/" >> requirements.txt
      echo "# astronomer-starship==?.?.?" >> requirements.txt
      echo "version: \"3.1\"" >> docker-compose.override.yml
      echo "services:" >> docker-compose.override.yml
      echo "  webserver:" >> docker-compose.override.yml
      echo "    volumes:" >> docker-compose.override.yml
      echo "      - ./starship:/usr/local/starship:rw" >> docker-compose.override.yml
    fi

## (Start/Restart) a test astro project with Starship installed
start-test TESTPATH START="start":
    cd {{TESTPATH}} && tar --exclude='node_modules' -czh . | docker build -t test - && astro dev {{START}} -i test
    just logs-test {{TESTPATH}}

## (Start/Restart) a test astro project with Starship installed without symlinks
start-test-no-symlink TESTPATH START="start":
    cd {{TESTPATH}} && astro dev {{START}}

## (Stop/Kill) a test astro project with Starship installed
stop-test TESTPATH STOP="stop":
    cd {{TESTPATH}} && astro dev {{STOP}}

# Deploy a test astro project with Starship installed
deploy-test TESTPATH:
    cd {{TESTPATH}} && tar --exclude='node_modules' -czh . | docker build -t test --platform=linux/x86_64 - && astro deploy -i test

# Get logs from a test astro project with Starship installed
logs-test TESTPATH:
    cd {{TESTPATH}} && astro dev logs -f

# Deploy a test astro project with Starship installed without symlinks
deploy-test-no-symlink TESTPATH:
    cd {{TESTPATH}} && astro deploy

# Serve Webapp on localhost
serve-frontend: build
    cd astronomer_starship && npx vite

# Restart just the webserver container (e.g. to reload the plugin)
restart-webserver:
    docker restart $(docker container ls --filter name=webserver --format="{{{{.ID}}") \
      && docker logs -f $(docker container ls --filter name=webserver --format="{{{{.ID}}") --since 1m


# Update the baseline for detect-secrets / pre-commit
update-secrets:
    detect-secrets scan  > .secrets.baseline  # pragma: allowlist secret

# Render and serve documentation locally
serve-docs:
    mkdocs serve -w {{DOCS_DIR}} -w {{SRC_DIR}}

# Build documentation locally (likely unnecessary)
build-docs: clean
    mkdocs build

# Deploy the documentation to GitHub Pages
deploy-docs UPSTREAM="origin": clean
    mkdocs gh-deploy -r {{UPSTREAM}}

# run just commands from the dev2 project for running the local version of Starship in Airflow 2
dev2 *ARGS:
    @just dev2/{{ARGS}}
