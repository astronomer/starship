from asyncio import FIRST_EXCEPTION, Future
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.conftest import manual_tests


def skip_no_docker(has_docker):
    """Skips this test if we don't have docker"""
    if not has_docker:
        pytest.skip("skipped, no docker")


@pytest.fixture
def local_version(project_root, *args):
    from astronomer_starship import __version__

    return __version__


@pytest.fixture(scope="session")
def docker_client():
    import docker

    return docker.from_env()


@pytest.fixture(scope="session")
def has_docker():
    from shutil import which

    return which("docker") is not None


def test_is_pip_installable(project_root):
    # noinspection PyUnresolvedReferences
    sh = pytest.importorskip("sh")
    actual = sh.pip("install", "-e", ".", _cwd=project_root)
    expected = "Successfully installed astronomer-starship"
    assert expected in actual, "we can `pip install -e .` our project"


@manual_tests  # requires docker
def test_install(has_docker, docker_client, project_root):
    skip_no_docker(has_docker)

    with ThreadPoolExecutor() as executor:

        def run_test_for_image(image: str):
            logs = docker_client.containers.run(
                image=image,
                user="root",
                entrypoint="/bin/bash",
                command=[
                    "-euxo",
                    "pipefail",
                    "-c",
                    "python -m pip install /usr/local/airflow/starship",
                ],
                volumes=[f"{project_root}:/usr/local/airflow/starship:rw"],
                stdout=True,
                stderr=True,
                remove=True,
            )
            assert b"ERROR" not in logs
            assert b"Successfully installed" in logs

        tests = [
            executor.submit(run_test_for_image, image)
            for image in [
                "quay.io/astronomer/ap-airflow:2.0.2-buster-onbuild",
                "quay.io/astronomer/ap-airflow:2.1.4-buster-onbuild",
                "quay.io/astronomer/ap-airflow:2.2.5-onbuild",
                "quay.io/astronomer/ap-airflow:2.3.4-onbuild",
                "quay.io/astronomer/astro-runtime:4.2.8",
                "quay.io/astronomer/ap-airflow:2.4.3-onbuild",
                "quay.io/astronomer/astro-runtime:5.4.0",
                "quay.io/astronomer/astro-runtime:6.6.0",
                "quay.io/astronomer/astro-runtime:7.6.0",
                "quay.io/astronomer/astro-runtime:8.5.0",
                "quay.io/astronomer/astro-runtime:9.2.0",
            ]
        ]
        for test in futures.wait(tests, return_when=FIRST_EXCEPTION)[0]:
            test: Future
            if test.exception():
                raise test.exception()


def test_version(local_version):
    import requests

    package = "astronomer-starship"

    releases = requests.get(f"https://pypi.org/pypi/{package}/json").json()["releases"]
    shipped_versions = []
    [shipped_versions.append(version) for version, details in releases.items()]

    assert local_version not in shipped_versions
