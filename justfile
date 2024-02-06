#!/usr/bin/env just --justfile
set dotenv-load := true
VERSION := `echo $(python -c 'from astronomer_starship import __version__; print(__version__)')`

default:
  @just --choose

# Print this help text
help:
    @just --list

# Install pre-commit
install-precommit:
    pre-commit install

# install frontend requirements
install-frontend:
    cd astronomer_starship && npm install

# install backend requirements
install-backend:
    pip install -e '.[dev]'

# Install the project
install: clean install-frontend install-backend

# Test Starship Python API
test-backend:
    pytest -c pyproject.toml

# Test Starship Webapp Frontend
test-frontend:
    cd astronomer_starship && npm run test

# Run unit tests
test: test-frontend test-backend

test-cicd:
  act pull-request -W .github/workflows/checks.yml --container-architecture linux/amd64

# Build Starship Webapp Frontend
build-frontend:
    cd astronomer_starship && npm run build

# Build Starship Package
build-backend:
    python -m build

# Build the project
build: install clean build-backend build-frontend

# Clean up any temp and dependency directories
clean:
    rm -rf dist
    rm -rf *.egg-info
    rm -rf astronomer_starship/static
    rm -rf astronomer_starship/node_modules

deploy-tag: tag
    git push origin v{{VERSION}}

deploy: deploy-tag

# Upload to TestPyPi for testing (note: you can only use each version once)
upload-testpypi: build install clean
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
    cd astronomer_starship && npm run dev

# Update the baseline for detect-secrets / pre-commit
update-secrets:
    detect-secrets scan  > .secrets.baseline  # pragma: allowlist secret

# Generate and serve the documentation (or `build`)
generate-docs CMD="serve":
    mkdocs {{CMD}}

# Deploy the documentation to GitHub Pages
deploy-docs UPSTREAM="origin":
    mkdocs gh-deploy -r {{UPSTREAM}}
