import itertools
import logging
from typing import List, Dict, Any

import sqlalchemy
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

    # logging.debug(f"Adding {datum}")
    # model = {"task_instance": TaskInstance, "dag_run": DagRun}[table_name]
    # # Reconstruct DagRun/TaskInstance, skipping __init__
    # ti = model.__new__(model, **datum)
    # # Call Base as though __init__ had been called
    # super(Base, ti).__init__()
    # # Add it
    # session.add(ti)
    #      ERROR - Class 'airflow.models.dagrun.DagRun' is mapped, but this instance lacks instrumentation.
    #      This occurs when the instance is created before sqlalchemy.orm.mapper(airflow.models.dagrun.DagRun)
    #      was called.
    #  Traceback (most recent call last):
    #    File "/usr/local/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 2016, in add
    #      state = attributes.instance_state(instance)
    #  AttributeError: 'DagRun' object has no attribute '_sa_instance_state'
    #
    #  The above exception was the direct cause of the following exception:
    #
    #  Traceback (most recent call last):
    #    File "/usr/local/airflow/starship/astronomer/starship/main.py", line 74, in receive_dag_history
    #      local_airflow_client.receive_dag(data=data)
    #    File "/usr/local/lib/python3.9/site-packages/airflow/utils/session.py", line 70, in wrapper
    #      return func(*args, session=session, **kwargs)
    #    File "/usr/local/airflow/starship/astronomer/starship/services/local_airflow_client.py",
    #    line 81, in receive_dag
    #      session.add(ti)
    #    File "/usr/local/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 2018, in add
    #      util.raise_(
    #    File "/usr/local/lib/python3.9/site-packages/sqlalchemy/util/compat.py", line 182, in raise_
    #      raise exception
    #  sqlalchemy.orm.exc.UnmappedInstanceError: Class 'airflow.models.dagrun.DagRun' is mapped,
    #  but this instance lacks instrumentation.  This occurs when the instance is created before
    #  sqlalchemy.orm.mapper(airflow.models.dagrun.DagRun) was called.
    #  [2023-07-10 23:37:50,256] {_internal.py:113} INFO - 127.0.0.1 - - [10/Jul/2023 23:37:50]
    #  "POST /astromigration/dag_history/receive HTTP/1

    engine = session.get_bind()
    metadata_obj = MetaData(bind=engine)

    task_instances = [datum for datum in data if datum["table"] == "task_instance"]
    dag_runs = [datum for datum in data if datum["table"] == "dag_run"]

    others = [
        datum for datum in data if datum["table"] not in ("task_instance", "dag_run")
    ]
    if any(others):
        logging.warning(f"Received unexpected records! {others} - skipping!")

    for data_list, table_name in (
        (task_instances, "task_instance"),
        (dag_runs, "dag_run"),
    ):
        if not data_list:
            continue
        logging.debug("Removing keys that could cause issues...")
        for datum in data_list:
            for k in ["conf", "id"]:
                if k in datum:
                    del datum[k]

        try:
            table = Table(f"airflow.{table_name}", metadata_obj, autoload_with=engine)
        except NoSuchTableError:
            table = Table(table_name, metadata_obj, autoload_with=engine)

        engine.execute(
            sqlalchemy.dialects.postgresql.insert(table).on_conflict_do_nothing(),
            data_list,
        )
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
