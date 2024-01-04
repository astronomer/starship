#!/usr/bin/env just --justfile

# Install the project
install:
    cd astronomer_starship && npm install
    pip install -e '.[dev]'

build:
    vite build
