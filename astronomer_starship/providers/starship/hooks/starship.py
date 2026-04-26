"""
Hooks for interacting with Starship migrations
"""

import json
import logging
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
    def get_dags(self):
        pass

    @abstractmethod
    def get_dag(self, dag_id: str) -> dict:
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

    def get_dag(self, dag_id: str) -> dict:
        """Get a single DAG's metadata from the local Airflow instance."""
        from airflow.models import DagModel

        compat = StarshipCompatabilityLayer()
        try:
            fields = [
                getattr(DagModel, attr_desc["attr"])
                for attr_desc in compat.dag_attrs().values()
                if attr_desc["attr"] is not None
            ]
            dag_result = compat.session.query(*fields).filter(DagModel.dag_id == dag_id).one()
            record = {
                attr: (
                    compat._get_tags(dag_result.dag_id)
                    if attr == "tags"
                    else (
                        compat._get_dag_run_count(dag_result.dag_id)
                        if attr == "dag_run_count"
                        else getattr(dag_result, attr_desc["attr"], None)
                    )
                )
                for attr, attr_desc in compat.dag_attrs().items()
            }
            return json.loads(json.dumps(record, default=str))
        except Exception:
            logging.exception("get_dag() failed for DAG %s", dag_id)
            raise

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
            # is called with no args. Rebind explicitly with both fields.
            if not conn.login and conn.password:
                session.auth = self._auth_type(conn.login, conn.password)

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

    def get_dag(self, dag_id: str) -> dict:
        """Get a single DAG by ID from the Target Starship instance."""
        if not hasattr(self, "_dag_cache"):
            self._dag_cache = {d["dag_id"]: d for d in self.get_dags()}
        if dag_id not in self._dag_cache:
            raise ValueError(f"DAG '{dag_id}' not found in remote.")
        return self._dag_cache[dag_id]

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
