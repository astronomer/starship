from pathlib import Path


def test_version():
    import requests
    import tomllib

    with open(Path(__file__).parent.parent / "pyproject.toml", "rb") as t:
        local_version = tomllib.load(t)["tool"]["poetry"]["version"]

    package = "astronomer-starship"

    releases = requests.get(f"https://pypi.org/pypi/{package}/json").json()["releases"]
    shipped_versions = []
    [shipped_versions.append(version) for version, details in releases.items()]

    assert local_version not in shipped_versions
