import requests
from cachetools.func import ttl_cache
from flask import session


@ttl_cache(ttl=1)
def get_connections(deployment_url: str, token: str):
    resp = requests.get(
        f"{deployment_url}/api/v1/connections",
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp.json()["connections"]


def get_pools(deployment_url: str, token: str):
    resp = requests.get(
        f"{deployment_url}/api/v1/pools",
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp.json()["pools"]


def get_variables(deployment_url: str, token: str):
    resp = requests.get(
        f"{deployment_url}/api/v1/variables",
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp.json()["variables"]


def create_variable(deployment_url, local_vars, variable):
    r = requests.post(
        f"{deployment_url}/api/v1/variables",
        headers={"Authorization": f"Bearer {session.get('token')}"},
        json={"key": variable, "value": local_vars[variable].val},
    )
    r.raise_for_status()


def get_config(deployment_url: str, token: str):
    resp = requests.get(
        f"{deployment_url}/api/v1/config",
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp.json()


def set_dag_is_paused(dag_id, is_paused, deployment_url, token):
    return requests.patch(
        f"{deployment_url}/api/v1/dags?dag_id_pattern={dag_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_paused": is_paused},
    )


def get_dag(dag_id, deployment_url, token):
    resp = requests.get(
        f"{deployment_url}/api/v1/dags/{dag_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp


def test_connection(conn_id, deployment_url, local_connections):
    rv = requests.post(
        f"{deployment_url}/api/v1/connections/test",
        headers={"Authorization": f"Bearer {session.get('token')}"},
        json={
            "connection_id": local_connections[conn_id].conn_id,
            "conn_type": local_connections[conn_id].conn_type,
            "host": local_connections[conn_id].host,
            "login": local_connections[conn_id].login,
            "schema": local_connections[conn_id].schema,
            "port": local_connections[conn_id].port,
            "password": local_connections[conn_id].password or "",
            "extra": local_connections[conn_id].extra,
        },
    )
    return rv


def create_connection(conn_id, deployment_url, local_connections):
    r = requests.post(
        f"{deployment_url}/api/v1/connections",
        headers={"Authorization": f"Bearer {session.get('token')}"},
        json={
            "connection_id": local_connections[conn_id].conn_id,
            "conn_type": local_connections[conn_id].conn_type,
            "host": local_connections[conn_id].host,
            "login": local_connections[conn_id].login,
            "schema": local_connections[conn_id].schema,
            "port": local_connections[conn_id].port,
            "password": local_connections[conn_id].password or "",
            "extra": local_connections[conn_id].extra,
        },
    )
    r.raise_for_status()
