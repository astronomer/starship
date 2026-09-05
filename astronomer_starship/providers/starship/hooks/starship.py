"""
Hooks for interacting with Starship migrations
"""

from abc import ABC, abstractmethod
from typing import List

from airflow.providers.http.hooks.http import HttpHook

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3
from astronomer_starship.compat.starship_compatability import StarshipCompatabilityLayer

if AIRFLOW_V_3:
    from airflow.sdk import BaseHook
elif AIRFLOW_V_2:
    from airflow.hooks.base import BaseHook
else:
    raise RuntimeError("Unsupported Airflow version")

POOLS_ROUTE = "/api/starship/pools"
CONNECTIONS_ROUTE = "/api/starship/connections"
VARIABLES_ROUTE = "/api/starship/variables"
DAGS_ROUTE = "/api/starship/dags"
DAG_RUNS_ROUTE = "/api/starship/dag_runs"
TASK_INSTANCES_ROUTE = "/api/starship/task_instances"


class StarshipHook(ABC):
    @abstractmethod
    def get_variables(self):
        pass

    @abstractmethod
    def set_variable(self, **kwargs):
        pass

    @abstractmethod
    def get_pools(self):
        pass

    @abstractmethod
    def set_pool(self, **kwargs):
        pass

    @abstractmethod
    def get_connections(self):
        pass

    @abstractmethod
    def set_connection(self, **kwargs):
        pass

    @abstractmethod
    def get_dags(self):
        pass

    @abstractmethod
    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        pass

    @abstractmethod
    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        pass

    @abstractmethod
    def set_dag_runs(self, dag_runs: list):
        pass

    @abstractmethod
    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        pass

    @abstractmethod
    def set_task_instances(self, task_instances: list):
        pass


STARSHIP_SOURCE_CONN_ID = "starship_source"


def _local_hook_af3_conn_missing_error(conn_id: str) -> RuntimeError:
    return RuntimeError(
        f"Starship source connection '{conn_id}' is not configured. "
        f"On Airflow 3, direct database access from workers is disallowed, so the "
        f"migration operators fetch source metadata via HTTP through the Starship "
        f"API. Create an Airflow HTTP connection with id '{conn_id}' whose host "
        f"points at the source Airflow's base URL (e.g. https://<source>/) and "
        f"whose password is a valid API token for that instance."
    )


if AIRFLOW_V_3:

    class StarshipLocalHook(HttpHook, StarshipHook):
        """Read source Airflow metadata via HTTP on Airflow 3.

        Airflow 3 disallows direct DB access from workers, so the operator-based
        migration flow reaches the source instance through its own Starship HTTP
        API instead. Expects an Airflow connection (default id: ``starship_source``)
        whose host is the source Airflow base URL and whose password is an API
        token.

        Preserves the read-only semantics of the Airflow 2 LocalHook: the
        ``set_*`` methods that mutate source state (except ``set_dag_is_paused``,
        which is used to pause the source DAG during migration) raise
        ``RuntimeError``.
        """

        def __init__(self, http_conn_id: str = STARSHIP_SOURCE_CONN_ID, **kwargs):
            super().__init__(http_conn_id=http_conn_id, **kwargs)
            # Airflow 3 splits AirflowNotFoundException between airflow.exceptions
            # (task-side) and airflow.sdk.exceptions (SDK); catch either.
            not_found_exc: tuple = ()
            try:
                from airflow.exceptions import AirflowNotFoundException as _CoreNotFound

                not_found_exc += (_CoreNotFound,)
            except ImportError:
                pass
            try:
                from airflow.sdk.exceptions import AirflowNotFoundException as _SdkNotFound

                not_found_exc += (_SdkNotFound,)
            except ImportError:
                pass

            try:
                BaseHook.get_connection(self.http_conn_id)
            except not_found_exc as e:
                raise _local_hook_af3_conn_missing_error(self.http_conn_id) from e

        def get_variables(self):
            conn = self.get_conn()
            res = conn.get(self.url_from_endpoint(VARIABLES_ROUTE))
            res.raise_for_status()
            return res.json()

        def set_variable(self, **kwargs):
            raise RuntimeError("Setting local data is not supported")

        def get_pools(self):
            conn = self.get_conn()
            res = conn.get(self.url_from_endpoint(POOLS_ROUTE))
            res.raise_for_status()
            return res.json()

        def set_pool(self, **kwargs):
            raise RuntimeError("Setting local data is not supported")

        # noinspection PyMethodOverriding
        def get_connections(self):
            conn = self.get_conn()
            res = conn.get(self.url_from_endpoint(CONNECTIONS_ROUTE))
            res.raise_for_status()
            return res.json()

        def set_connection(self, **kwargs):
            raise RuntimeError("Setting local data is not supported")

        def get_dags(self) -> dict:
            conn = self.get_conn()
            res = conn.get(self.url_from_endpoint(DAGS_ROUTE))
            res.raise_for_status()
            return res.json()

        def set_dag_is_paused(self, dag_id: str, is_paused: bool):
            conn = self.get_conn()
            res = conn.patch(
                self.url_from_endpoint(DAGS_ROUTE),
                json={"dag_id": dag_id, "is_paused": is_paused},
            )
            res.raise_for_status()
            return res.json()

        def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
            conn = self.get_conn()
            res = conn.get(
                self.url_from_endpoint(DAG_RUNS_ROUTE),
                params={"dag_id": dag_id, "limit": limit, "offset": offset},
            )
            res.raise_for_status()
            return res.json()

        def set_dag_runs(self, dag_runs: list):
            raise RuntimeError("Setting local data is not supported")

        def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
            conn = self.get_conn()
            res = conn.get(
                self.url_from_endpoint(TASK_INSTANCES_ROUTE),
                params={"dag_id": dag_id, "limit": limit, "offset": offset},
            )
            res.raise_for_status()
            return res.json()

        def set_task_instances(self, task_instances: list):
            raise RuntimeError("Setting local data is not supported")

else:

    class StarshipLocalHook(BaseHook, StarshipHook):
        """Hook to retrieve local Airflow data, which can then be sent to the Target Starship instance."""

        def get_variables(self):
            """
            Get all variables from the local Airflow instance.
            """
            return StarshipCompatabilityLayer().get_variables()

        def set_variable(self, **kwargs):
            raise RuntimeError("Setting local data is not supported")

        def get_pools(self):
            """
            Get all pools from the local Airflow instance.
            """
            return StarshipCompatabilityLayer().get_pools()

        def set_pool(self, **kwargs):
            raise RuntimeError("Setting local data is not supported")

        # noinspection PyMethodOverriding
        def get_connections(self):
            """
            Get all connections from the local Airflow instance.
            """
            return StarshipCompatabilityLayer().get_connections()

        def set_connection(self, **kwargs):
            raise RuntimeError("Setting local data is not supported")

        def get_dags(self) -> dict:
            """
            Get all DAGs from the local Airflow instance.
            """
            return StarshipCompatabilityLayer().get_dags()

        def set_dag_is_paused(self, dag_id: str, is_paused: bool):
            """
            Set the paused status of a DAG in the local Airflow instance.
            """
            return StarshipCompatabilityLayer().set_dag_is_paused(dag_id, is_paused)

        def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
            """
            Get DAG runs from the local Airflow instance.
            """
            return StarshipCompatabilityLayer().get_dag_runs(dag_id, offset=offset, limit=limit)

        def set_dag_runs(self, dag_runs: list):
            raise RuntimeError("Setting local data is not supported")

        def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
            """
            Get task instances from the local Airflow instance.
            """
            return StarshipCompatabilityLayer().get_task_instances(dag_id, offset=offset, limit=limit)

        def set_task_instances(self, task_instances: list):
            raise RuntimeError("Setting local data is not supported")


class StarshipHttpHook(HttpHook, StarshipHook):
    def get_variables(self):
        """
        Get all variables from the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(VARIABLES_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_variable(self, **kwargs):
        """
        Set a variable in the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(VARIABLES_ROUTE)
        res = conn.post(url, json=kwargs)
        res.raise_for_status()
        return res.json()

    def get_pools(self):
        """
        Get all pools from the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(POOLS_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_pool(self, **kwargs):
        """
        Set a pool in the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(POOLS_ROUTE)
        res = conn.post(url, json=kwargs)
        res.raise_for_status()
        return res.json()

    # noinspection PyMethodOverriding
    def get_connections(self):
        """
        Get all connections from the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(CONNECTIONS_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_connection(self, **kwargs):
        """
        Set a connection in the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(CONNECTIONS_ROUTE)
        res = conn.post(url, json=kwargs)
        res.raise_for_status()
        return res.json()

    def get_dags(self) -> dict:
        """
        Get all DAGs from the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(DAGS_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        """
        Set the paused status of a DAG in the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(DAGS_ROUTE)
        res = conn.patch(url, json={"dag_id": dag_id, "is_paused": is_paused})
        res.raise_for_status()
        return res.json()

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        """
        Get DAG runs from the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(DAG_RUNS_ROUTE)
        res = conn.get(url, params={"dag_id": dag_id, "limit": limit})
        res.raise_for_status()
        return res.json()

    def set_dag_runs(self, dag_runs: List[dict]) -> dict:
        """
        Set DAG runs in the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(DAG_RUNS_ROUTE)
        res = conn.post(url, json={"dag_runs": dag_runs})
        res.raise_for_status()
        return res.json()

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        """
        Get task instances from the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(TASK_INSTANCES_ROUTE)
        res = conn.get(url, params={"dag_id": dag_id, "limit": limit})
        res.raise_for_status()
        return res.json()

    def set_task_instances(self, task_instances: list[dict]) -> dict:
        """
        Set task instances in the Target Starship instance.
        """
        conn = self.get_conn()
        url = self.url_from_endpoint(TASK_INSTANCES_ROUTE)
        res = conn.post(url, json={"task_instances": task_instances})
        res.raise_for_status()
        return res.json()
