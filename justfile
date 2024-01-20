#!/usr/bin/env just --justfile
set dotenv-load := true

default:
  @just --choose

# Print this help text
help:
    @just --list

# Install pre-commit
install-pre-commit:
    pre-commit install

# Install the project
install: clean
    cd astronomer_starship && npm install
    pip install -e '.[dev]'

# Build the project
build: install clean
    cd astronomer_starship && npm run build
    python -m build

# Clean up any temp and dependency directories
clean:
    rm -rf dist
    rm -rf *.egg-info
    rm -rf astronomer_starship/static
    rm -rf astronomer_starship/node_modules

upload-testpypi: build install clean
    python -m twine check dist/*
    TWINE_USER=${TWINE_USER} TWINE_PASS=${TWINE_PASS} python -m twine upload --repository testpypi dist/*

upload-pypi: clean install build
    python -m twine check dist/*
    TWINE_USER=${TWINE_USER} TWINE_PASS=${TWINE_PASS} python -m twine upload dist/*

# create a test project at the path
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
    cd {{TESTPATH}} && tar -czh . | docker build -t test - && astro dev {{START}} -i test

## (Stop/Kill) a test astro project with Starship installed
stop-test TESTPATH STOP="stop":
    cd {{TESTPATH}} && astro dev {{STOP}} -i test

deploy-test TESTPATH:
    cd {{TESTPATH}} && tar -czh . | docker build -t test --platform=linux/x86_64 - && astro deploy -i test
