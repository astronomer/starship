include .env
TWINE=$(shell which twine)
PYTHON3=$(shell which python3)

help:
	@grep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

ship: ## create and ship pypi dist
	$(PYTHON3) setup.py sdist
	$(TWINE) upload dist/astronomer-migration-provider-${VERSION}.tar.gz -u ${TWINE_USER} -p ${TWINE_PASS}