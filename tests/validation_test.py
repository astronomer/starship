from pathlib import Path

import pytest
import docker

from tests.conftest import manual_tests


@pytest.fixture
def local_version(*args):
    try:
        import tomllib

        read_mode = "rb"
    except ImportError:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import toml as tomllib

        read_mode = "r"

    with open(Path(__file__).parent.parent / "pyproject.toml", read_mode) as t:
        return tomllib.load(t)["tool"]["poetry"]["version"]


@pytest.fixture
def local_branch_or_tag(*args):
    import subprocess

    return subprocess.check_output(
        "git symbolic-ref -q --short HEAD || git describe --tags --exact-match",
        shell=True,
    ).decode()


@pytest.fixture
def docker_client(*args):
    return docker.from_env()


@manual_tests  # requires docker
@pytest.mark.parametrize(
    "image,docker_client,local_branch_or_tag",
    [
        ("quay.io/astronomer/ap-airflow:2.0.2-buster-onbuild", None, None),
        ("quay.io/astronomer/ap-airflow:2.1.4-buster-onbuild", None, None),
        ("quay.io/astronomer/ap-airflow:2.2.5-onbuild", None, None),
        ("quay.io/astronomer/ap-airflow:2.3.4-onbuild", None, None),
        ("quay.io/astronomer/astro-runtime:4.2.8", None, None),
        ("quay.io/astronomer/ap-airflow:2.4.3-onbuild", None, None),
        ("quay.io/astronomer/astro-runtime:5.4.0", None, None),
        ("quay.io/astronomer/astro-runtime:6.6.0", None, None),
        ("quay.io/astronomer/astro-runtime:7.6.0", None, None),
        ("quay.io/astronomer/astro-runtime:8.5.0", None, None),
    ],
    indirect=["docker_client", "local_branch_or_tag"],
)
def test_install(image, docker_client, local_branch_or_tag):
    logs = docker_client.containers.run(
        image=image,
        user="root",
        entrypoint="/bin/bash",
        command=[
            "-euxo",
            "pipefail",
            "-c",
            "python -m pip install /usr/local/airflow/starship"
            if "2.1.4" not in image
            else "python -m pip install /usr/local/airflow/starship rich==10.9.0",
        ],
        volumes=[f"{Path(__file__).parent.parent}:/usr/local/airflow/starship"],
        stdout=True,
        stderr=True,
        remove=True,
    )
    assert b"ERROR" not in logs
    assert b"Successfully installed" in logs


def test_version(local_version):
    import requests

    package = "astronomer-starship"

    releases = requests.get(f"https://pypi.org/pypi/{package}/json").json()["releases"]
    shipped_versions = []
    [shipped_versions.append(version) for version, details in releases.items()]

    assert local_version not in shipped_versions
