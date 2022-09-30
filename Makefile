.PHONY: install_dev
install_dev:
	python -m pip install --upgrade pip
	python -m pip install -r dev-requirements.txt

.PHONY: pre-commit-install
pre-commit-install:
	pre-commit install
