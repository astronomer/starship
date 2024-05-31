from abc import ABC, abstractmethod

from typing import List

from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook

from astronomer_starship.starship_api import starship_compat

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


class StarshipLocalHook(BaseHook, StarshipHook):
    def get_variables(self):
        return starship_compat.get_variables()

    def set_variable(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def get_pools(self):
        return starship_compat.get_pools()

    def set_pool(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    # noinspection PyMethodOverriding
    def get_connections(self):
        return starship_compat.get_connections()

    def set_connection(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def get_dags(self) -> dict:
        return starship_compat.get_dags()

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        return starship_compat.set_dag_is_paused(dag_id, is_paused)

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        return starship_compat.get_dag_runs(dag_id, offset=offset, limit=limit)

    def set_dag_runs(self, dag_runs: list):
        raise RuntimeError("Setting local data is not supported")

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        return starship_compat.get_task_instances(dag_id, offset=offset, limit=limit)

    def set_task_instances(self, task_instances: list):
        raise RuntimeError("Setting local data is not supported")


class StarshipHttpHook(HttpHook, StarshipHook):
    def get_variables(self):
        conn = self.get_conn()
        url = self.url_from_endpoint(VARIABLES_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_variable(self, **kwargs):
        conn = self.get_conn()
        url = self.url_from_endpoint(VARIABLES_ROUTE)
        res = conn.post(url, json=kwargs)
        res.raise_for_status()
        return res.json()

    def get_pools(self):
        conn = self.get_conn()
        url = self.url_from_endpoint(POOLS_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_pool(self, **kwargs):
        conn = self.get_conn()
        url = self.url_from_endpoint(POOLS_ROUTE)
        res = conn.post(url, json=kwargs)
        res.raise_for_status()
        return res.json()

    # noinspection PyMethodOverriding
    def get_connections(self):
        conn = self.get_conn()
        url = self.url_from_endpoint(CONNECTIONS_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_connection(self, **kwargs):
        conn = self.get_conn()
        url = self.url_from_endpoint(CONNECTIONS_ROUTE)
        res = conn.post(url, json=kwargs)
        res.raise_for_status()
        return res.json()

    def get_dags(self) -> dict:
        conn = self.get_conn()
        url = self.url_from_endpoint(DAGS_ROUTE)
        res = conn.get(url)
        res.raise_for_status()
        return res.json()

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        conn = self.get_conn()
        url = self.url_from_endpoint(DAGS_ROUTE)
        res = conn.patch(url, json={"dag_id": dag_id, "is_paused": is_paused})
        res.raise_for_status()
        return res.json()

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        conn = self.get_conn()
        url = self.url_from_endpoint(DAG_RUNS_ROUTE)
        res = conn.get(url, params={"dag_id": dag_id, "limit": limit})
        res.raise_for_status()
        return res.json()

    def set_dag_runs(self, dag_runs: List[dict]) -> dict:
        conn = self.get_conn()
        url = self.url_from_endpoint(DAG_RUNS_ROUTE)
        res = conn.post(url, json={"dag_runs": dag_runs})
        res.raise_for_status()
        return res.json()

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        conn = self.get_conn()
        url = self.url_from_endpoint(TASK_INSTANCES_ROUTE)
        res = conn.get(url, params={"dag_id": dag_id, "limit": limit})
        res.raise_for_status()
        return res.json()

    def set_task_instances(self, task_instances: list[dict]) -> dict:
        conn = self.get_conn()
        url = self.url_from_endpoint(TASK_INSTANCES_ROUTE)
        res = conn.post(url, json={"task_instances": task_instances})
        res.raise_for_status()
        return res.json()
