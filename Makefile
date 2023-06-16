#* Variables
SHELL := /usr/bin/env bash
PYTHON := python3

BRANCH := $(shell git branch --show-current)
STARSHIP_VERSION := $(shell poetry version --short)
STARSHIP_TAG := "v$(STARSHIP_VERSION)"


#* Poetry
.PHONY: poetry-download
poetry-download: ## Download and install poetry to system
	curl -sSL https://install.python-poetry.org | $(PYTHON) -

.PHONY: poetry-remove
poetry-remove:  ## Uninstall poetry from system
	curl -sSL https://install.python-poetry.org | $(PYTHON) - --uninstall

#* Installation
.PHONY: install
install: ## install project and all dependency groups via poetry to a local venv
	poetry lock -n
	poetry install -n


.PHONY: install-pre-commit
install-pre-commit: install
	poetry run pip install pre-commit

.PHONY: pre-commit-install
pre-commit-install: install ## Install files required for pre-commit (highly recommended!)
	poetry run install-pre-commit
	poetry run pre-commit install

#* Formatters + Linting
.PHONY: test
test: ## Run unit tests
	poetry run pytest -c pyproject.toml

.PHONY: test-with-coverage
test-with-coverage: ## Run unit tests and emit a coverage report
	poetry run pytest -c pyproject.toml --cov=./ --cov-report=xml

.PHONY: codestyle
codestyle: ## Fix config file and code style via plugins defined in .pre-commit-config.yaml and pyproject.toml
	@echo "Running Black, Blacken-docs, Ruff, etc from Pre-Commit"
	poetry run pre-commit run

.PHONY: formatting
formatting: codestyle ## Alias of codestyle

.PHONY: lint
lint: test codestyle ## Run unit tests and check+fix the style of files

#* Cleaning
.PHONY: pycache-remove
pycache-remove: ## Clean up __pycache__ folders and .pyc files
	find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf

.PHONY: dist-remove
dist-remove: ## Clean the /dist folder
	rm -rf dist/

.PHONY: clean-all
clean-all: pycache-remove dist-remove ## Clean various temp files from the repo

.PHONY: publish_test
publish_test: clean-all  ## Push current Starship Version to Test PyPi (You can also do this via a GHA Button)
	poetry publish --repository testpypi --skip-existing -n || echo " Note: you first need to run `poetry config repositories.testpypi https://test.pypi.org/legacy/` and `poetry config pypi-token.pypi pypi-A.............` with a token"


.PHONY: publish
publish:  ## Push current Starship Version to PyPi (Don't do this, GHA does!)
	poetry publish --build --skip-existing -n || echo " Note: you first need to run `poetry config pypi-token.pypi pypi-A.............` with a token"

.PHONY: delete-tag
delete-tag: ## Delete the current Starship version as a tag from Github (Don't do this, affects GHA and PyPi!)
	- git tag -d $(STARSHIP_TAG)
	- git push origin --delete $(STARSHIP_TAG)


.PHONY:tag
tag: clean-all delete-tag ## Push a new tag for the current Starship version
	git tag $(STARSHIP_TAG)
	git push origin $(STARSHIP_TAG)


.PHONY: help
help: ## Show this help text
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
