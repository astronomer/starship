import os
import pytest
from pathlib import Path

manual_tests = pytest.mark.skipif(
    not bool(os.getenv("MANUAL_TESTS")), reason="requires env setup"
)


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture()
def app():
    from airflow.www.app import create_app

    app = create_app(testing=True)
    yield app


@pytest.fixture(autouse=True)
def app_context(app):
    with app.app_context():
        yield
