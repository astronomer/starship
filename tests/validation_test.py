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


# https://packaging.python.org/en/latest/guides/making-a-pypi-friendly-readme/#validating-restructuredtext-markup

# ----------
# python -c "import airflow.plugins"
# ----------
# python -c "import pkg_resources; print(list(pkg_resources.iter_entry_points(group='airflow.plugins')))"
# [EntryPoint.parse('starship = astronomer.starship.main:StarshipPlugin'),
# EntryPoint.parse('starship_aeroscope = astronomer.aeroscope.plugins:AeroscopePlugin'),
# EntryPoint.parse('stellar_runtime = stellar.runtime.plugin:StellarRuntimePlugin'),
# EntryPoint.parse('OpenLineagePlugin = openlineage.airflow.plugin:OpenLineagePlugin'),
# EntryPoint.parse('astronomer_runtime = astronomer.runtime.plugin:AstronomerRuntimePlugin'),
# EntryPoint.parse('astronomer_analytics_plugin = astronomer_analytics_plugin.analytics_plugin:AstronomerPlugin'),
# EntryPoint.parse('astronomer_version_check = astronomer.airflow.version_check.plugin:AstronomerVersionCheckPlugin'),
# EntryPoint.parse('hive = airflow.providers.apache.hive.plugins.hive:HivePlugin')]
# ----------
# cat /usr/local/lib/python3.9/site-packages/astronomer_starship-1.0.0.dist-info/direct_url.json
# {"dir_info": {"editable": true}, "url": "file:///usr/local/airflow/starship"}
# ----------
# cat /usr/local/lib/python3.9/site-packages/astronomer_starship-1.0.0.dist-info/entry_points.txt
# [airflow.plugins]
# starship=astronomer.starship.main:StarshipPlugin
# starship_aeroscope=astronomer.aeroscope.plugins:AeroscopePlugin
#
# [console_scripts]
# orion=orion.__main__:migrate
# ----------
