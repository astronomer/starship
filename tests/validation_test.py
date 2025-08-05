import os
import subprocess
from pathlib import Path

from asyncio import FIRST_EXCEPTION, Future
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import pytest
from docker.errors import ImageNotFound

IS_ARM = os.uname().machine == "arm64"

ASTRO_IMAGES = [
    # Not used, harder to install on
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

IMAGES = [
    "apache/airflow:slim-2.11.0",
    "apache/airflow:slim-2.10.3",
    "apache/airflow:slim-2.9.3",
    "apache/airflow:slim-2.8.1",
    # "apache/airflow:slim-2.7.3",
    # "apache/airflow:slim-2.6.0",
    # "apache/airflow:slim-2.5.3",
    # "apache/airflow:slim-2.4.0",
    # "apache/airflow:2.3.4",
    "apache/airflow:2.2.4",
    "apache/airflow:2.1.3",
    "apache/airflow:2.0.2",
    # # "apache/airflow:1.10.15",
    # # "apache/airflow:1.10.10",
]


def skip_no_docker(has_docker):
    """Skips this test if we don't have docker"""
    if not has_docker:
        pytest.skip("skipped, no docker")


@pytest.fixture
def local_version(project_root):
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


def run_in_container(
    docker_client,
    image: str,
    command: str,
    volumes: list,
    environment: dict,
    platform: str,
) -> tuple[int, bytes]:
    from docker.models.containers import Container

    container: Container = docker_client.containers.run(
        image=image,
        platform=platform,
        # user="root",
        entrypoint="/bin/bash",
        environment=environment,
        command=command,
        volumes=volumes,
        stdout=True,
        stderr=True,
        detach=True,
    )
    exit_code = container.wait()
    logs = container.logs()
    container.remove()
    return exit_code, logs


def test_docker_pytest(has_docker, docker_client, project_root, local_version):
    skip_no_docker(has_docker)
    version = local_version.replace("v", "")

    with ThreadPoolExecutor() as executor:

        def run_test_for_image(image: str):
            if IS_ARM:
                try:
                    docker_client.images.pull(image, platform="linux/amd64")
                    platform = "linux/amd64"
                except ImageNotFound:
                    docker_client.images.pull(image, platform="linux/x86_64")
                    platform = "linux/x86_64"
            else:
                docker_client.images.pull(image, platform="linux/x86_64")
                platform = "linux/x86_64"

            exit_code, logs = run_in_container(
                docker_client,
                image=image,
                platform=platform,
                command=f"/usr/local/airflow/starship/tests/docker_test/run_container_test.sh "
                f"{image} "
                f"starship/dist/astronomer_starship-{version}-py3-none-any.whl",
                volumes=[
                    f"{project_root}/dist/:/usr/local/airflow/starship/dist/:rw",
                    f"{project_root}/tests/docker_test:/usr/local/airflow/starship/tests/docker_test:rw",
                ],
                environment={
                    "AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE": "False",
                    "DOCKER_TEST": "True",
                    "PYTHON_PATH": "$PYTHON_PATH:/usr/local/airflow/:/op/airflow/",
                },
            )
            if exit_code["StatusCode"] != 0:
                print(f"[IMAGE={image}] exit code: {exit_code}\n{logs.decode()}")
                log_file_name = f'{image.rsplit(":", maxsplit=1)[-1]}.test.log'
                Path(log_file_name).write_bytes(logs)
            assert exit_code == {"StatusCode": 0}, f"exit code: {exit_code}\n{logs}"
            assert (
                b"[STARSHIP-PYTEST-SUCCESS" in logs
            ), f"Looking for success in {exit_code}\n{logs}"

        print("Building...")
        build_logs = subprocess.run(
            "just build-backend", shell=True, capture_output=True, check=True
        )
        print(build_logs)
        tests = [executor.submit(run_test_for_image, image) for image in IMAGES]
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
