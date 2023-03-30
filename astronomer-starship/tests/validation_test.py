import pytest


def pytest_addoption(parser):
    parser.addoption("--package", action="store")


@pytest.fixture(scope="session")
def package(request):
    package_value = request.config.option.package
    if package_value is None:
        pytest.skip()
    return package_value


def test_version(package):
    import configparser
    import requests

    config = configparser.RawConfigParser()
    config.read_file(open(rf"{package}/setup.cfg"))
    local_version = config.get("metadata", "version")
    releases = requests.get(f"https://pypi.org/pypi/{package}/json").json()["releases"]
    shipped_versions = []
    [shipped_versions.append(version) for version, details in releases.items()]

    assert local_version not in shipped_versions
