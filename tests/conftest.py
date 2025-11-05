import os
from pathlib import Path

import pytest

manual_tests = pytest.mark.skipif(not bool(os.getenv("MANUAL_TESTS")), reason="requires env setup")


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent
