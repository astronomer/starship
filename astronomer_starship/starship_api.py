from airflow import __version__

[major, _] = __version__.split(".", maxsplit=1)

if int(major) == 2:
    from astronomer_starship.v2.starship_api import StarshipAPIPlugin  # noqa
elif int(major) == 3:
    from astronomer_starship.v3.starship_api import StarshipAPIPlugin  # noqa
