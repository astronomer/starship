from typing import List

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


@provide_session
def get_variables(session: Session):
    from airflow.models import Variable

    vars = session.query(Variable).order_by(Variable.key).all()
    return vars


def get_dags():
    from airflow.models import DagBag

    dags = DagBag().dags
    return dags


def set_dag_is_paused(dag_id, is_paused):
    DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)


@deprecated
class LocalAirflowClient:
    pass
