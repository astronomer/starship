from astronomer_starship.compat import AIRFLOW_V_2


if AIRFLOW_V_2:
    from astronomer_starship.af2.starship import StarshipPlugin
    from astronomer_starship.af2.starship_api import StarshipAPIPlugin
else:
    from airflow import __version__

    raise RuntimeError(f"No Starship plugins available for Airflow {__version__}")

__all__ = [
    "StarshipPlugin",
    "StarshipAPIPlugin",
]
