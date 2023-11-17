import itertools
import logging
import pickle
from typing import List, Dict, Any

from sqlalchemy.dialects.postgresql import insert

from airflow import DAG
from airflow.models import DagModel, DagRun
from airflow.utils.session import provide_session
from cachetools.func import ttl_cache
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import NoSuchTableError
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
    return DagBag(read_dags_from_db=True).dags


def get_dag(dag_id: str) -> DAG:
    return get_dags()[dag_id]


def set_dag_is_paused(dag_id, is_paused):
    DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)


@provide_session
def receive_dag(session: Session, data: list = None):
    if data is None:
        logging.warning(f"Received no data! data {data}")
        return

    engine = session.get_bind()
    metadata_obj = MetaData(bind=engine)

    task_instances = [datum for datum in data if datum["table"] == "task_instance"]
    dag_runs = [datum for datum in data if datum["table"] == "dag_run"]

    others = [
        datum for datum in data if datum["table"] not in ("task_instance", "dag_run")
    ]
    if len(others):
        logging.warning(f"Received unexpected records! {others} - skipping!")

    for data_list, table_name in (
        (dag_runs, "dag_run"),
        (task_instances, "task_instance"),
    ):
        if not data_list:
            continue
        logging.debug("Removing keys that could cause issues...")
        for datum in data_list:
            # Dropping conf and executor_config because they are pickle objects
            # I can't figure out how to send them
            for k in ["conf", "id", "table", "conf", "executor_config"]:
                if k in datum:
                    if k == "executor_config":
                        datum[k] = pickle.dumps({})
                    else:
                        del datum[k]

        try:
            table = Table(f"airflow.{table_name}", metadata_obj, autoload_with=engine)
        except NoSuchTableError:
            table = Table(table_name, metadata_obj, autoload_with=engine)

        with engine.begin() as txn:
            txn.execute(insert(table).on_conflict_do_nothing(), data_list)


@provide_session
def get_dag_runs_and_task_instances(
    session: Session, dag_id: str
) -> List[Dict[str, Any]]:
    def _as_json_with_table(_row):
        _r = {col.name: getattr(_row, col.name) for col in _row.__table__.columns}
        try:
            _r["table"] = str(_row.__table__)
        except AttributeError:
            _r["table"] = str(_row.__tablename__)
        return _r

    dag_runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id)
        .order_by(DagRun.start_date)
        .limit(5)
        .all()
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
