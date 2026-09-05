"""Airflow 3 implementation of StarshipLocalHook: HTTP against the source Starship API.

Airflow 3 disallows direct DB access from workers, so operators read source
metadata through the source instance's own /api/starship/* endpoints. Expects
an Airflow HTTP connection (default id: ``starship_source``).
"""

from airflow.sdk import BaseHook

from astronomer_starship.providers.starship.hooks.starship import (
    STARSHIP_SOURCE_CONN_ID,
    StarshipHttpHook,
)


def _missing_conn_error(conn_id: str) -> RuntimeError:
    return RuntimeError(
        f"Starship source connection '{conn_id}' is not configured. "
        f"On Airflow 3, direct database access from workers is disallowed, so the "
        f"migration operators fetch source metadata via HTTP through the Starship "
        f"API. Create an Airflow HTTP connection with id '{conn_id}' whose host "
        f"points at the source Airflow's base URL (e.g. https://<source>/) and "
        f"whose password is a valid API token for that instance."
    )


class StarshipLocalHook(StarshipHttpHook):
    """Read source Airflow metadata via HTTP on Airflow 3.

    Inherits all ``get_*`` methods and ``set_dag_is_paused`` from
    :class:`StarshipHttpHook`. Overrides the remaining ``set_*`` methods to
    preserve the read-only semantics of the Airflow 2 LocalHook: mutating
    source state is not part of Starship's migration flow.
    """

    def __init__(self, http_conn_id: str = STARSHIP_SOURCE_CONN_ID, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        # AirflowNotFoundException moved between airflow.exceptions and
        # airflow.sdk.exceptions across AF3 minor releases; catch either.
        not_found: tuple = ()
        try:
            from airflow.exceptions import AirflowNotFoundException as _Core

            not_found += (_Core,)
        except ImportError:
            pass
        try:
            from airflow.sdk.exceptions import AirflowNotFoundException as _Sdk

            not_found += (_Sdk,)
        except ImportError:
            pass

        try:
            BaseHook.get_connection(self.http_conn_id)
        except not_found as e:
            raise _missing_conn_error(self.http_conn_id) from e

    def set_variable(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def set_pool(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def set_connection(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def set_dag_runs(self, dag_runs: list):
        raise RuntimeError("Setting local data is not supported")

    def set_task_instances(self, task_instances: list):
        raise RuntimeError("Setting local data is not supported")
