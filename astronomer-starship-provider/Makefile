include .env
TWINE=$(shell which twine)
PYTHON3=$(shell which python3)
VERSION:=$(shell python3 gha_retrieve_version.py)
TAG:= "astronomer-starship-provider-v$(VERSION)"

help:
	@grep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

check: ## check doc formatting
	$(PYTHON3) setup.py checkdocs

package: ## create a new pypi dist
	$(PYTHON3) setup.py sdist

test-ship: ## ship to test pypi dist
	$(TWINE) upload --repository testpypi dist/astronomer-starship-provider-${VERSION}.tar.gz -u ${TWINE_USER} -p ${TWINE_TEST_PASS}

ship: ## ship pypi dist
ship: package
	$(TWINE) upload dist/astronomer-starship-provider-${VERSION}.tar.gz -u ${TWINE_USER} -p ${TWINE_PASS}

.PHONY: delete-tag
delete-tag:
	- git tag -d $(VERSION)
	- git push origin --delete $(VERSION)

.PHONY: tag
tag: delete-tag
	git tag $(VERSION)
	git push origin $(VERSION)

.PHONY: build
build:
	python3 setup.py sdist
