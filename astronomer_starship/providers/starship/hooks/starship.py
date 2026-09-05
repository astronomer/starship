"""Hooks for interacting with Starship migrations."""

from abc import ABC, abstractmethod
from typing import List

from airflow.providers.http.hooks.http import HttpHook

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3

POOLS_ROUTE = "/api/starship/pools"
CONNECTIONS_ROUTE = "/api/starship/connections"
VARIABLES_ROUTE = "/api/starship/variables"
DAGS_ROUTE = "/api/starship/dags"
DAG_RUNS_ROUTE = "/api/starship/dag_runs"
TASK_INSTANCES_ROUTE = "/api/starship/task_instances"

# Default Airflow connection id the AF3 StarshipLocalHook reads from.
STARSHIP_SOURCE_CONN_ID = "starship_source"


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


# Dispatch StarshipLocalHook to the version-specific implementation. Kept at
# the bottom because both submodules import from this file.
if AIRFLOW_V_3:
    from astronomer_starship.providers.starship.hooks._local_af3 import StarshipLocalHook
elif AIRFLOW_V_2:
    from astronomer_starship.providers.starship.hooks._local_af2 import StarshipLocalHook
else:
    raise RuntimeError("Unsupported Airflow version")


__all__ = [
    "STARSHIP_SOURCE_CONN_ID",
    "StarshipHook",
    "StarshipHttpHook",
    "StarshipLocalHook",
]
