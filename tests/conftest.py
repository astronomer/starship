import os
import pytest
from airflow import __version__
from pathlib import Path

manual_tests = pytest.mark.skipif(
    not bool(os.getenv("MANUAL_TESTS")), reason="requires env setup"
)


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


[major, _] = __version__.split(".", maxsplit=1)
if int(major) == 2:

    @pytest.fixture()
    def app():
        from airflow.www.app import create_app

        app = create_app(testing=True)
        yield app

    @pytest.fixture(autouse=True)
    def app_context(app):
        with app.app_context():
            yield
