import json
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional

import requests
from airflow.models import Connection, Pool, Variable
from cachetools.func import ttl_cache
from deprecated import deprecated
from requests import Response

from astronomer.starship.services import local_airflow_client

remote_dags: Dict[str, Dict[str, Any]] = {}
dag_fetch_time: datetime = datetime(1970, 1, 1)


def conn_to_json(connection: Connection) -> dict:
    return {
        "connection_id": connection.conn_id,
        "conn_type": connection.conn_type,
        "host": connection.host,
        "login": connection.login,
        "schema": connection.schema,
        "port": connection.port,
        "password": connection.password or "",
        "extra": connection.extra,
    }


@ttl_cache(ttl=1)
def get_connections(deployment_url: str, token: str) -> List[Any]:
    r = requests.get(
        f"{deployment_url}/api/v1/connections",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()["connections"]


def delete_connection(
    deployment_url: str, token: str, connection: Connection
) -> Response:
    r = requests.delete(
        f"{deployment_url}/api/v1/connections/{connection.conn_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r


def do_test_connection(
    deployment_url: str, token: str, connection: Connection
) -> Response:
    r = requests.post(
        f"{deployment_url}/api/v1/connections/test",
        headers={"Authorization": f"Bearer {token}"},
        json=conn_to_json(connection),
    )
    r.raise_for_status()
    return r


def create_connection(deployment_url, token, connection) -> Response:
    r = requests.post(
        f"{deployment_url}/api/v1/connections",
        headers={"Authorization": f"Bearer {token}"},
        json=conn_to_json(connection),
    )
    r.raise_for_status()
    return r


def delete_pool(deployment_url, token, pool) -> Response:
    r = requests.delete(
        f"{deployment_url}/api/v1/pools/{pool.pool}",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r


def get_pools(deployment_url: str, token: str):
    r = requests.get(
        f"{deployment_url}/api/v1/pools",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()["pools"]


def create_pool(deployment_url, token, pool: Pool) -> Response:
    r = requests.post(
        f"{deployment_url}/api/v1/pools",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": pool.pool, "slots": pool.slots, "description": pool.description},
    )
    r.raise_for_status()
    return r


def is_pool_migrated(deployment_url: str, token: str, pool_name: str):
    remote_pools = get_pools(deployment_url, token)
    return pool_name in (remote_pool.get("name", "") for remote_pool in remote_pools)


def get_variables(deployment_url: str, token: str):
    r = requests.get(
        f"{deployment_url}/api/v1/variables",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()["variables"]


def delete_variable(deployment_url: str, token: str, variable: Variable):
    r = requests.delete(
        f"{deployment_url}/api/v1/variables/{variable.key}",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r


def is_variable_migrated(deployment_url: str, token: str, variable: str):
    return variable in (v["key"] for v in get_variables(deployment_url, token))


def create_variable(deployment_url, token: str, variable: Variable):
    r = requests.post(
        f"{deployment_url}/api/v1/variables",
        headers={"Authorization": f"Bearer {token}"},
        json={"key": variable.key, "value": variable.val},
    )
    r.raise_for_status()
    return r


@deprecated(reason="unused, doesn't work in astro")
def get_config(deployment_url: str, token: str):
    r = requests.get(
        f"{deployment_url}/api/v1/config",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()


def set_dag_is_paused(dag_id, is_paused, deployment_url, token):
    r = requests.patch(
        f"{deployment_url}/api/v1/dags?dag_id_pattern={dag_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_paused": is_paused},
    )
    r.raise_for_status()
    return r


def _get_remote_dags(deployment_url: str, token: str) -> Response:
    r = requests.get(
        f"{deployment_url}/api/v1/dags",
        headers={"Authorization": f"Bearer {token}"},
    )
    return r


def _get_remote_dag(dag_id: str, deployment_url: str, token: str) -> Response:
    r = requests.get(
        f"{deployment_url}/api/v1/dags/{dag_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return r


def get_dag(
    dag_id: str,
    deployment_url: str,
    token: str,
    ttl: timedelta = timedelta(seconds=60),
    skip_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    global remote_dags
    global dag_fetch_time

    if dag_fetch_time + ttl < datetime.now():
        r = _get_remote_dags(deployment_url, token)
        if not r.ok:
            remote_dags = {}
            dag_fetch_time = datetime.now()
            r.raise_for_status()
        else:
            # reset the cache - reset the ttl timer
            remote_dags = {dag["dag_id"]: dag for dag in r.json().get("dags", [])}
            dag_fetch_time = datetime.now()
        return remote_dags.get(dag_id)
    elif skip_cache:
        r = _get_remote_dag(dag_id, deployment_url, token)
        r.raise_for_status()
        dag = r.json()
        remote_dags[dag_id] = dag
        # we don't reset the cache time - because all other entries are still on the clock
        return dag
    else:
        return remote_dags.get(dag_id)


def get_dag_runs(dag_id, deployment_url, token) -> Response:
    r = requests.get(
        f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns?limit=1",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r


def migrate_dag(dag: str, deployment_url: str, token: str):
    result = local_airflow_client.migrate(table_name="dag_run", dag_id=dag)
    r = requests.post(
        f"{deployment_url}/astromigration/dag_history/receive",
        data=json.dumps(result),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    r.raise_for_status()
    return r.ok
