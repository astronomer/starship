def _get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from packaging.version import Version

    from airflow import __version__

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_VERSION_TUPLE = _get_base_airflow_version_tuple()
AIRFLOW_V_2 = AIRFLOW_VERSION_TUPLE >= (2, 0, 0) and AIRFLOW_VERSION_TUPLE < (3, 0, 0)
