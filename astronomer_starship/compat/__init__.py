"""Compatibility layer for different major Airflow versions.

All imports should go through this module and will be resolved to the corresponding Airflow version.
"""

from typing import Tuple


def _get_base_airflow_version_tuple() -> Tuple[int, int, int]:
    from airflow import __version__
    from packaging.version import Version

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


class AirflowVersionError(ImportError):
    """An ImportError raised when no compatible Starship plugins are found for the installed Airflow version."""

    def __init__(self):
        from airflow import __version__

        msg = f"`No Starship plugins available for Airflow {__version__}`."
        super().__init__(msg)


AIRFLOW_VERSION_TUPLE = _get_base_airflow_version_tuple()
AIRFLOW_V_2 = AIRFLOW_VERSION_TUPLE >= (2, 0, 0) and AIRFLOW_VERSION_TUPLE < (3, 0, 0)
AIRFLOW_V_3 = AIRFLOW_VERSION_TUPLE >= (3, 0, 0) and AIRFLOW_VERSION_TUPLE < (4, 0, 0)

if AIRFLOW_V_2:
    from astronomer_starship._af2.starship import StarshipPlugin
    from astronomer_starship._af2.starship_api import StarshipApi, StarshipAPIPlugin
elif AIRFLOW_V_3:
    from astronomer_starship._af3.starship import StarshipPlugin
    from astronomer_starship._af3.starship_api import StarshipApi, StarshipAPIPlugin
else:
    raise AirflowVersionError()

__all__ = [
    "StarshipApi",
    "StarshipAPIPlugin",
    "StarshipPlugin",
]
