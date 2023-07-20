import json
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional, Callable

import requests
from airflow.models import Connection, Pool, Variable
from requests import Response

from astronomer.starship.services import local_airflow_client

# Caching
remote_data: Dict[str, Dict[str, Dict[str, Any]]] = {}
remote_data_fetch_time: Dict[str, datetime] = {}


def get_all_with_cache(
    data_type: str,
    get_remote_data_fn: Callable[[str, str], Response],
    deployment_url: str,
    key_id: str,
    results_key_id: str,
    token: str,
    ttl: timedelta = timedelta(seconds=60),
    skip_cache: bool = False,
) -> Dict[str, Dict[str, Any]]:
    last_fetched = remote_data_fetch_time.get(data_type, datetime(1970, 1, 1))
    if skip_cache:
        r = get_remote_data_fn(deployment_url, token)
        if not r.ok:
            r.raise_for_status()
        return {datum[key_id]: datum for datum in r.json().get(results_key_id, [])}
    elif last_fetched + ttl < datetime.now():
        r = get_remote_data_fn(deployment_url, token)
        if not r or not r.ok:
            # set the cache as empty
            remote_data[data_type] = {}
            remote_data_fetch_time[data_type] = datetime.now()
            if not r.ok:
                r.raise_for_status()
        else:
            # reset the cache - reset the ttl timer
            remote_data[data_type] = {
                datum[key_id]: datum for datum in r.json().get(results_key_id, [])
            }
            remote_data_fetch_time[data_type] = datetime.now()
        return remote_data[data_type]
    else:
        return remote_data[data_type]


# Utility Methods
def get_extras(deployment_url: str, token: str) -> Dict[str, Any]:
    """Use Bearer auth if astro else admin:admin if local"""
    return (
        {
            "headers": {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }
        }
        if "localhost" not in deployment_url
        else {
            "headers": {"Content-Type": "application/json"},
            "auth": ("admin", "admin"),
        }
    )


# Connections
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


def _get_remote_connections(deployment_url: str, token: str) -> Response:
    return requests.get(
        f"{deployment_url}/api/v1/connections",
        **get_extras(deployment_url, token),
    )


def get_connections(
    deployment_url: str,
    token: str,
    ttl: timedelta = timedelta(seconds=60),
    skip_cache: bool = False,
) -> List[Dict[str, Any]]:
    if not deployment_url:
        return []

    return list(
        get_all_with_cache(
            data_type="connections",
            get_remote_data_fn=_get_remote_connections,
            key_id="connection_id",
            results_key_id="connections",
            deployment_url=deployment_url,
            token=token,
            ttl=ttl,
            skip_cache=skip_cache,
        ).values()
    )


def delete_connection(
    deployment_url: str, token: str, connection: Connection
) -> Optional[Dict[str, Any]]:
    if not deployment_url:
        return {}
    r = requests.delete(
        f"{deployment_url}/api/v1/connections/{connection.conn_id}",
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.json() if r.content else None


def do_test_connection(
    deployment_url: str, token: str, connection: Connection
) -> Dict[str, Any]:
    if not deployment_url:
        return {}
    r = requests.post(
        f"{deployment_url}/api/v1/connections/test",
        **get_extras(deployment_url, token),
        json=conn_to_json(connection),
    )
    r.raise_for_status()
    return r.json()


def create_connection(deployment_url, token, connection) -> Dict[str, Any]:
    if not deployment_url:
        return {}
    r = requests.post(
        f"{deployment_url}/api/v1/connections",
        **get_extras(deployment_url, token),
        json=conn_to_json(connection),
    )
    r.raise_for_status()
    return r.json()


# Pools
def delete_pool(deployment_url, token, pool) -> Optional[Dict[str, Any]]:
    if not deployment_url:
        return {}
    r = requests.delete(
        f"{deployment_url}/api/v1/pools/{pool.pool}",
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.json() if r.content else None


def _get_remote_pools(deployment_url: str, token: str) -> Response:
    return requests.get(
        f"{deployment_url}/api/v1/pools",
        **get_extras(deployment_url, token),
    )


def get_pools(
    deployment_url: str,
    token: str,
    ttl: timedelta = timedelta(seconds=60),
    skip_cache: bool = False,
) -> List[Dict[str, Any]]:
    if not deployment_url:
        return []

    return list(
        get_all_with_cache(
            data_type="pools",
            get_remote_data_fn=_get_remote_pools,
            key_id="name",
            results_key_id="pools",
            deployment_url=deployment_url,
            token=token,
            ttl=ttl,
            skip_cache=skip_cache,
        ).values()
    )


def create_pool(deployment_url, token, pool: Pool) -> Dict[str, Any]:
    if not deployment_url:
        return {}
    r = requests.post(
        f"{deployment_url}/api/v1/pools",
        **get_extras(deployment_url, token),
        json={"name": pool.pool, "slots": pool.slots, "description": pool.description},
    )
    if not r.ok and r.status_code == 400 and "Unknown field" in r.text:
        r = requests.post(
            f"{deployment_url}/api/v1/pools",
            **get_extras(deployment_url, token),
            json={"name": pool.pool, "slots": pool.slots},
        )
    r.raise_for_status()
    return r.json()


def is_pool_migrated(deployment_url: str, token: str, pool_name: str):
    return any(
        pool_name == remote_pool.get("name")
        for remote_pool in get_pools(deployment_url, token)
    )


# Variables
def _get_remote_variables(deployment_url: str, token: str) -> Response:
    return requests.get(
        f"{deployment_url}/api/v1/variables", **get_extras(deployment_url, token)
    )


def get_variables(
    deployment_url: str,
    token: str,
    ttl: timedelta = timedelta(seconds=60),
    skip_cache: bool = False,
) -> List[Dict[str, Any]]:
    if not deployment_url:
        return []

    return list(
        get_all_with_cache(
            data_type="variables",
            get_remote_data_fn=_get_remote_variables,
            key_id="key",
            results_key_id="variables",
            deployment_url=deployment_url,
            token=token,
            ttl=ttl,
            skip_cache=skip_cache,
        ).values()
    )


def delete_variable(
    deployment_url: str, token: str, variable: Variable
) -> Optional[Dict[str, Any]]:
    if not deployment_url:
        return {}
    r = requests.delete(
        f"{deployment_url}/api/v1/variables/{variable.key}",
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.json() if r.content else None


def is_variable_migrated(deployment_url: str, token: str, variable: str):
    return any(variable == v["key"] for v in get_variables(deployment_url, token))


def create_variable(deployment_url, token: str, variable: Variable) -> Dict[str, Any]:
    if not deployment_url:
        return {}
    r = requests.post(
        f"{deployment_url}/api/v1/variables",
        **get_extras(deployment_url, token),
        json={"key": variable.key, "value": variable.val},
    )
    r.raise_for_status()
    return r.json()


# DAGs
def set_dag_is_paused(dag_id, is_paused, deployment_url, token) -> Dict[str, Any]:
    if not deployment_url:
        return {}
    r = requests.patch(
        f"{deployment_url}/api/v1/dags/{dag_id}",
        **get_extras(deployment_url, token),
        json={"is_paused": is_paused},
    )
    r.raise_for_status()
    return r.json()


def _get_remote_dags(deployment_url: str, token: str) -> Optional[Response]:
    if not deployment_url:
        return None
    r = requests.get(
        f"{deployment_url}/api/v1/dags", **get_extras(deployment_url, token)
    )
    return r


def _get_remote_dag(dag_id: str, deployment_url: str, token: str) -> Response:
    """DEPRECATED - Generic cache method uses _get_remote_dags instead"""
    r = requests.get(
        f"{deployment_url}/api/v1/dags/{dag_id}", **get_extras(deployment_url, token)
    )
    return r


def get_dag_runs(dag_id, deployment_url, token) -> Dict[str, Any]:
    if not deployment_url:
        return {}
    r = requests.get(
        f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns?limit=1",
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.json()


def get_dag(
    dag_id: str,
    deployment_url: str,
    token: str,
    ttl: timedelta = timedelta(seconds=60),
    skip_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    if skip_cache:
        r = _get_remote_dag(dag_id, deployment_url, token)
        r.raise_for_status()
        dag = r.json()
        if "dags" not in remote_data:
            remote_data["dags"] = {}
        remote_data["dags"][dag_id] = dag
        # we don't reset the cache time - because all other entries are still on the clock
        return dag
    else:
        return get_all_with_cache(
            data_type="dags",
            get_remote_data_fn=_get_remote_dags,
            key_id="dag_id",
            results_key_id="dags",
            deployment_url=deployment_url,
            token=token,
            ttl=ttl,
            skip_cache=False,
        ).get(dag_id)


def delete_dag(
    deployment_url: str, token: str, dag_id: str
) -> Optional[Dict[str, Any]]:
    if not deployment_url:
        raise RuntimeError("Deployment URL was not given")
    r = requests.delete(
        f"{deployment_url}/api/v1/dags/{dag_id}",
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.json() if r.content else None


def migrate_dag(deployment_url: str, token: str, dag_id: str) -> bool:
    if not deployment_url:
        return False
    data = local_airflow_client.get_dag_runs_and_task_instances(dag_id=dag_id)
    r = requests.post(
        f"{deployment_url}/astromigration/dag_history/receive",
        data=json.dumps(data, default=str),
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.ok


# Task Instances
def get_task_instances(
    dag_id: str, dag_run_id: str, deployment_url: str, token: str
) -> List[Dict[str, Any]]:
    if not deployment_url:
        return []
    r = requests.get(
        f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        **get_extras(deployment_url, token),
    )
    r.raise_for_status()
    return r.json().get("task_instances", [])
