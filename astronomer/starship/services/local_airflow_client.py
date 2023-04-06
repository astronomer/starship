from typing import List

from airflow import DAG
from airflow.models import DagModel
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
from deprecated import deprecated


@provide_session
def get_connections(session: Session) -> List['Connection']:
    from airflow.models import Connection

    connections = session.query(Connection).order_by(Connection.conn_id).all()
    return connections


@provide_session
def get_pools(session: Session) -> List['Pool']:
    from airflow.models import Pool

    pools = session.query(Pool).all()
    return pools


def get_pool(pool_name):
    return [p for p in get_pools() if p.pool == pool_name][0]


@provide_session
def get_variables(session: Session):
    from airflow.models import Variable

    variables = session.query(Variable).order_by(Variable.key).all()
    return variables


def get_variable(variable: str):
    return [v for v in get_variables() if v.key == variable][0]


def get_dags():
    from airflow.models import DagBag

    dags = DagBag().dags
    return dags


def get_dag(dag_id: str) -> DAG:
    return get_dags()[dag_id]


def set_dag_is_paused(dag_id, is_paused):
    DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)


@deprecated
class LocalAirflowClient:
    pass
