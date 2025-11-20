import logging
from typing import TYPE_CHECKING

from astronomer_starship.common import BaseStarshipAirflow

if TYPE_CHECKING:
    from typing import Dict, Union

    from astronomer_starship.common import AttrDesc


logger = logging.getLogger(__name__)


class StarshipAirflow(BaseStarshipAirflow):
    """Base class for Airflow 3.x compatibility layers.

    This class should not implement version-specific logic; that should be done in subclasses.
    """


class StarshipAirflow30(StarshipAirflow):
    """Airflow 3.0 compatibility layer."""

    @classmethod
    def pool_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "name": {
                "attr": "pool",
                "methods": [("POST", True), ("DELETE", True)],
                "test_value": "test_name",
            },
            "slots": {"attr": "slots", "methods": [("POST", True)], "test_value": 1},
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "test_description",
            },
            "include_deferred": {
                "attr": "include_deferred",
                "methods": [("POST", True)],
                "test_value": True,
            },
        }

    @classmethod
    def variable_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "key": {
                "attr": "key",
                "methods": [("POST", True), ("DELETE", True)],
                "test_value": "key",
            },
            "val": {"attr": "val", "methods": [("POST", True)], "test_value": "val"},
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "description",
            },
        }

    @classmethod
    def connection_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "conn_id": {
                "attr": "conn_id",
                "methods": [("POST", True), ("DELETE", True)],
                "test_value": "conn_id",
            },
            "conn_type": {
                "attr": "conn_type",
                "methods": [("POST", True)],
                "test_value": "conn_type",
            },
            "host": {
                "attr": "host",
                "methods": [("POST", False)],
                "test_value": "host",
            },
            "port": {
                "attr": "port",
                "methods": [("POST", False)],
                "test_value": 1234,
            },
            "schema": {
                "attr": "schema",
                "methods": [("POST", False)],
                "test_value": "schema",
            },
            "login": {
                "attr": "login",
                "methods": [("POST", False)],
                "test_value": "login",
            },
            "password": {  # pragma: allowlist secret
                "attr": "password",  # pragma: allowlist secret
                "methods": [("POST", False)],
                "test_value": "password",  # pragma: allowlist secret
            },
            "extra": {
                "attr": "extra",
                "methods": [("POST", False)],
                "test_value": "extra",
            },
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "description",
            },
        }


class StarshipAirflow31(StarshipAirflow30):
    """Airflow 3.1 compatibility layer."""

    @classmethod
    def pool_attrs(cls) -> "Dict[str, AttrDesc]":
        attrs = super().pool_attrs()
        # TODO: add support for Teams? (added in 3.1)
        # attrs["team_id"] = {
        #     "attr": "team_id",
        #     "methods": [("POST", True)],
        #     "test_value": 123,
        # }
        return attrs

    @classmethod
    def variable_attrs(cls) -> "Dict[str, AttrDesc]":
        attrs = super().variable_attrs()
        # TODO: add support for Teams? (added in 3.1)
        # attrs["team_id"] = {
        #     "attr": "team_id",
        #     "methods": [("POST", True)],
        #     "test_value": 123,
        # }
        return attrs

    @classmethod
    def connection_attrs(cls) -> "Dict[str, AttrDesc]":
        attrs = super().connection_attrs()
        # TODO: add support for Teams? (added in 3.1)
        # attrs["team_id"] = {
        #     "attr": "team_id",
        #     "methods": [("POST", True)],
        #     "test_value": 123,
        # }
        return attrs


class StarshipCompatabilityLayer:
    """StarshipCompatabilityLayer is a factory class that returns the correct StarshipAirflow class for a version

    - 3.0 https://github.com/apache/airflow/tree/3.0.0/airflow-core/src/airflow/models
    - 3.1 https://github.com/apache/airflow/tree/3.1.0/airflow-core/src/airflow/models
    """

    def __new__(cls, airflow_version: "Union[str, None]" = None) -> StarshipAirflow:
        from airflow import __version__
        from packaging.version import Version

        if airflow_version is None:
            airflow_version = __version__

        version = Version(airflow_version)
        major, minor = version.major, version.minor

        if major == 3:
            if minor == 0:
                return StarshipAirflow30()
            elif minor == 1:
                return StarshipAirflow31()

        raise RuntimeError(f"Unsupported Airflow Version: {airflow_version}")
