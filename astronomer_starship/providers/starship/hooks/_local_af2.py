"""Airflow 2 implementation of StarshipLocalHook: direct DB access via the compat layer."""

from airflow.hooks.base import BaseHook

from astronomer_starship.compat.starship_compatability import StarshipCompatabilityLayer
from astronomer_starship.providers.starship.hooks.starship import StarshipHook


class StarshipLocalHook(BaseHook, StarshipHook):
    """Hook to retrieve local Airflow data, which can then be sent to the Target Starship instance."""

    def get_variables(self):
        return StarshipCompatabilityLayer().get_variables()

    def set_variable(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def get_pools(self):
        return StarshipCompatabilityLayer().get_pools()

    def set_pool(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    # noinspection PyMethodOverriding
    def get_connections(self):
        return StarshipCompatabilityLayer().get_connections()

    def set_connection(self, **kwargs):
        raise RuntimeError("Setting local data is not supported")

    def get_dags(self) -> dict:
        return StarshipCompatabilityLayer().get_dags()

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        return StarshipCompatabilityLayer().set_dag_is_paused(dag_id, is_paused)

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        return StarshipCompatabilityLayer().get_dag_runs(dag_id, offset=offset, limit=limit)

    def set_dag_runs(self, dag_runs: list):
        raise RuntimeError("Setting local data is not supported")

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        return StarshipCompatabilityLayer().get_task_instances(dag_id, offset=offset, limit=limit)

    def set_task_instances(self, task_instances: list):
        raise RuntimeError("Setting local data is not supported")
