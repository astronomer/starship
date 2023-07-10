import itertools
import logging
from typing import List, Dict, Any

from airflow import DAG
from airflow.models import DagModel, TaskInstance, DagRun, Base
from airflow.utils.session import provide_session
from cachetools.func import ttl_cache
from sqlalchemy.orm import Session
from airflow.models import Connection
from airflow.models import Pool
from airflow.models import DagBag
from airflow.models import Variable


@provide_session
def get_connections(session: Session) -> List["Connection"]:
    connections = session.query(Connection).order_by(Connection.conn_id).all()
    return connections


@provide_session
def get_pools(session: Session) -> List["Pool"]:
    pools = session.query(Pool).all()
    return pools


def get_pool(pool_name):
    return [p for p in get_pools() if p.pool == pool_name][0]


@provide_session
def get_variables(session: Session):
    variables = session.query(Variable).order_by(Variable.key).all()
    return variables


def get_variable(variable: str):
    return [v for v in get_variables() if v.key == variable][0]


@ttl_cache(ttl=60)
def get_dags():
    dags = DagBag().dags
    return dags


def get_dag(dag_id: str) -> DAG:
    return get_dags()[dag_id]


def set_dag_is_paused(dag_id, is_paused):
    DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)


@provide_session
def receive_dag(session: Session, data: list = None):
    if data is None:
        logging.warning(f"Received no data! data {data}")
        return

    for datum in data:
        table_name = datum.pop("table")
        logging.debug("Removing keys that could cause issues...")
        for k in ["conf", "id"]:
            if k in datum:
                del datum[k]

        if table_name not in ["task_instance", "dag_run"]:
            logging.warning(
                f"Received unexpected record! table_name {table_name}, data {datum} - skipping!"
            )
        else:
            logging.debug(f"Adding {datum}")
            model = {"task_instance": TaskInstance, "dag_run": DagRun}[table_name]
            # Reconstruct DagRun/TaskInstance, skipping __init__
            ti = model.__new__(model, **datum)
            # Call Base as though __init__ had been called
            super(Base, ti).__init__()
            # Add it
            session.add(ti)

    session.commit()


@provide_session
def get_dag_runs_and_task_instances(
    session: Session, dag_id: str
) -> List[Dict[str, Any]]:
    def _as_json_with_table(_row):
        _r = {col.name: getattr(_row, col.name) for col in _row.__table__.columns}
        _r["table"] = str(_row.__table__)
        return _r

    dag_runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id)
        .order_by(DagRun.start_date)
        .limit(5)
    )
    logging.debug(
        f"Recursing through dag_runs: {dag_runs}, and flattening with task_instance relations"
    )
    return list(
        itertools.chain(
            *[
                (
                    _as_json_with_table(dag_run),
                    *[_as_json_with_table(ti) for ti in dag_run.task_instances],
                )
                for dag_run in dag_runs
            ]
        )
    )
