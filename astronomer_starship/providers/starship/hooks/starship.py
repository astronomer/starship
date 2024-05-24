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

    def set_connection(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        return starship_compat.set_dag_is_paused(dag_id, is_paused)

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        return starship_compat.get_dag_runs(dag_id, limit)

    def set_dag_runs(self, dag_runs: list):
        raise RuntimeError("Setting local data is not supported")

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        return starship_compat.get_task_instances(dag_id, offset, limit)

    def set_task_instances(self, task_instances: list):
        raise RuntimeError("Setting local data is not supported")

    def get_dags(self) -> dict:
        return starship_compat.get_dags()


class StarshipHttpHook(HttpHook, StarshipHook):
    conn_name_attr = "http_conn_id"
    default_conn_name = "starship_default"
    conn_type = "http"
    hook_name = "HTTP"

    def get_variables(self):
        return (
            self.get_conn()
            .get(self.get_connection(self.http_conn_id).url / VARIABLES_ROUTE)
            .json()
        )

    def set_variable(self, **kwargs):
        return (
            self.get_conn()
            .post(
                self.get_connection(self.http_conn_id).url / VARIABLES_ROUTE,
                json=kwargs,
            )
            .json()
        )

    def get_pools(self):
        return (
            self.get_conn()
            .get(self.get_connection(self.http_conn_id).url / POOLS_ROUTE)
            .json()
        )

    def set_pool(self, **kwargs):
        return (
            self.get_conn()
            .post(self.get_connection(self.http_conn_id).url / POOLS_ROUTE, json=kwargs)
            .json()
        )

    def set_connection(self, **kwargs):
        return (
            self.get_conn()
            .post(
                self.get_connection(self.http_conn_id).url / CONNECTIONS_ROUTE,
                json=kwargs,
            )
            .json()
        )

    def get_dags(self) -> dict:
        return (
            self.get_conn()
            .get(self.get_connection(self.http_conn_id).url / DAGS_ROUTE)
            .json()
        )

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        return (
            self.get_conn()
            .get(
                self.get_connection(self.http_conn_id).url / DAG_RUNS_ROUTE,
                params={"dag_id": dag_id, "limit": limit},
            )
            .json()
        )

    def set_dag_runs(self, dag_runs: List[dict]) -> dict:
        return (
            self.get_conn()
            .post(
                self.get_connection(self.http_conn_id).url / DAG_RUNS_ROUTE,
                json={"dag_runs": dag_runs},
            )
            .json()
        )

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        return (
            self.get_conn()
            .get(
                self.get_connection(self.http_conn_id).url / TASK_INSTANCES_ROUTE,
                params={"dag_id": dag_id, "limit": limit},
            )
            .json()
        )

    def set_task_instances(self, task_instances: list[dict]) -> dict:
        return (
            self.get_conn()
            .post(
                self.get_connection(self.http_conn_id).url / TASK_INSTANCES_ROUTE,
                json={"task_instances": task_instances},
            )
            .json()
        )

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        return (
            self.get_conn()
            .patch(
                self.get_connection(self.http_conn_id).url / DAGS_ROUTE,
                json={"dag_id": dag_id, "is_paused": is_paused},
            )
            .json()
        )
