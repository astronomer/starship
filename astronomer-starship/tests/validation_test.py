import sh

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


def test_render_docs():
    """test_render_docs
    https://packaging.python.org/en/latest/guides/making-a-pypi-friendly-readme/#validating-restructuredtext-markup
    """
    sh.python("setup.py", "sdist")
    sh.twine("check", "dist/*")
    assert True, "we check .rst without errors"
