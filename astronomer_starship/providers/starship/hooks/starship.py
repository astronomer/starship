"""
Hooks for interacting with Starship migrations
"""

from abc import ABC, abstractmethod
from typing import List, Optional

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
TASK_INSTANCE_HISTORY_ROUTE = "/api/starship/task_instance_history"

# Headers that Airflow's HttpHook legitimately forwards from connection extras.
# Anything else in `extra` (e.g. auth-factory hints like `impersonation_chain`
# or `region_name`) must not leak onto the HTTP session as a header.
_STANDARD_HTTP_HEADERS = frozenset(
    [
        "accept",
        "accept-encoding",
        "accept-language",
        "authorization",
        "cache-control",
        "connection",
        "content-type",
        "cookie",
        "host",
        "origin",
        "pragma",
        "referer",
        "user-agent",
    ]
)


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
    def get_dags(self, dag_id: Optional[str] = None) -> list:
        """Get all DAGs, or a single-element list when ``dag_id`` is provided."""
        pass

    def get_dag(self, dag_id: str) -> Optional[dict]:
        """Get a single DAG by ID, or ``None`` if it doesn't exist.
        a thin filter on top of ``get_dags(dag_id=...)``.
        """
        dags = self.get_dags(dag_id=dag_id)
        return dags[0] if dags else None

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

    @abstractmethod
    def get_task_instance_history(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        pass

    @abstractmethod
    def set_task_instance_history(self, task_instances: list):
        pass


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

    def get_dags(self, dag_id: Optional[str] = None) -> list:
        """Get all DAGs from the local Airflow instance, or a single one.

        When ``dag_id`` is provided the compat layer issues a targeted query
        instead of paying the full N+1 cost of the unfiltered fetch.
        """
        return StarshipCompatabilityLayer().get_dags(dag_id=dag_id)

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
        """Write DAG runs to the local Airflow database."""
        return StarshipCompatabilityLayer().set_dag_runs(dag_runs)

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        """
        Get task instances from the local Airflow instance.
        """
        return StarshipCompatabilityLayer().get_task_instances(dag_id, offset=offset, limit=limit)

    def set_task_instances(self, task_instances: list):
        """Write task instances to the local Airflow database."""
        return StarshipCompatabilityLayer().set_task_instances(task_instances)

    def get_task_instance_history(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        """Get task instance history from the local Airflow instance."""
        return StarshipCompatabilityLayer().get_task_instance_history(dag_id, offset=offset, limit=limit)

    def set_task_instance_history(self, task_instances: list):
        """Write task instance history to the local Airflow database."""
        return StarshipCompatabilityLayer().set_task_instance_history(task_instances=task_instances)


class StarshipHttpHook(HttpHook, StarshipHook):
    def get_conn(self, headers=None):
        """Drop non-HTTP-header keys from connection ``extra`` before use.

        Airflow's ``HttpHook`` copies the entire ``extra`` JSON onto the
        session as headers. Starship uses ``extra`` to stash auth-factory
        hints (e.g. ``impersonation_chain``, ``region_name``,
        ``environment_name``), and those must not be sent as HTTP headers.

        Also rebinds ``session.auth`` for bearer-token-in-password
        connections (Astro, OSS-bearer) because Airflow's ``HttpHook``
        instantiates ``auth_type()`` with **zero args** when the
        connection has no ``login``, losing the token.
        """
        session = super().get_conn(headers=headers)

        extra_keys = [k for k in session.headers if k.lower() not in _STANDARD_HTTP_HEADERS]
        for key in extra_keys:
            session.headers.pop(key, None)

        if getattr(self, "_auth_type", None) and getattr(self, "http_conn_id", None):
            try:
                conn = self.get_connection(self.http_conn_id)
            except Exception:
                return session
            # Upstream HttpHook only passes creds when conn.login is set;
            # for token-in-password connections that means our auth class
            # is called with no args. Rebind explicitly with the token.
            if not conn.login and conn.password:
                session.auth = self._auth_type(password=conn.password)

        return session

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

    def get_dags(self, dag_id: Optional[str] = None) -> list:
        """Get all DAGs from the Target Starship instance, or a single one.

        The remote ``/api/starship/dags`` endpoint doesn't accept a server-side
        filter today, so we fetch the full list once (cached on the hook
        instance) and filter client-side. Repeated single-DAG lookups during
        a wave still cost just one HTTP roundtrip.
        """
        if not hasattr(self, "_all_dags_cache"):
            conn = self.get_conn()
            url = self.url_from_endpoint(DAGS_ROUTE)
            res = conn.get(url)
            res.raise_for_status()
            self._all_dags_cache = res.json()
        if dag_id is None:
            return self._all_dags_cache
        return [d for d in self._all_dags_cache if d["dag_id"] == dag_id]

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

    def get_task_instance_history(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        """Get task instance history from the Target Starship instance."""
        conn = self.get_conn()
        url = self.url_from_endpoint(TASK_INSTANCE_HISTORY_ROUTE)
        res = conn.get(url, params={"dag_id": dag_id, "limit": limit})
        res.raise_for_status()
        return res.json()

    def set_task_instance_history(self, task_instances: List[dict]) -> dict:
        """Set task instance history in the Target Starship instance."""
        conn = self.get_conn()
        url = self.url_from_endpoint(TASK_INSTANCE_HISTORY_ROUTE)
        res = conn.post(url, json={"task_instances": task_instances})
        res.raise_for_status()
        return res.json()
