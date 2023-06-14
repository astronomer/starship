import datetime
import logging
from typing import List

import sqlalchemy
from airflow import DAG, settings
from airflow.models import DagModel
from airflow.utils.session import provide_session
from cachetools.func import ttl_cache
from deprecated import deprecated
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import Session
from airflow.models import Connection
from airflow.models import Pool


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
    from airflow.models import Variable

    variables = session.query(Variable).order_by(Variable.key).all()
    return variables


def get_variable(variable: str):
    return [v for v in get_variables() if v.key == variable][0]


@ttl_cache(ttl=60)
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


@provide_session
def migrate(session: Session, table_name: str, dag_id: str):
    logging.info("Creating source SQL Connections ..")
    source_engine = session.get_bind()
    source_metadata_obj = MetaData(bind=source_engine)
    source_table = get_table(source_metadata_obj, source_engine, table_name)

    # noinspection SqlInjection
    logging.info(f"Fetching data for {table_name} from source...")
    source_result = source_engine.execution_options(stream_results=True).execute(
        f"SELECT * FROM {source_table.name} where dag_id='{dag_id}' order by start_date desc limit 5"
    )
    rtn = []
    for result in source_result.fetchall():
        result = dict(result._asdict())
        for key, value in result.items():
            if isinstance(value, memoryview):
                result[key] = value.tobytes().decode("iso-8859-1")
            if isinstance(value, datetime.datetime):
                result[key] = f"{value.strftime('%Y-%m-%d %H:%M:%S.%f+00')}"
        result["table"] = table_name
        rtn.append(result)
        source_table = get_table(source_metadata_obj, source_engine, "task_instance")
        sub_result = source_engine.execution_options(stream_results=True).execute(
            f"SELECT * FROM {source_table.name} where dag_id='{dag_id}' and run_id='{result['run_id']}'"
        )
        for ti_result in sub_result.fetchall():
            ti_result_new = dict(ti_result._asdict())
            ti_result = dict(ti_result._asdict())
            ti_result["table"] = "task_instance"
            for ti_key, ti_value in ti_result_new.items():
                if isinstance(ti_value, memoryview):
                    del ti_result[ti_key]
                if isinstance(ti_value, datetime.datetime):
                    ti_result[
                        ti_key
                    ] = f"{ti_value.strftime('%Y-%m-%d %H:%M:%S.%f+00')}"
            rtn.append(ti_result)

    return rtn


def receive_dag(data: list = None):
    if data is None:
        data = []
    dest_session = settings.Session()
    dest_engine = dest_session.get_bind()
    dest_metadata_obj = MetaData(bind=dest_engine)
    for datum in data:
        try:
            del datum["conf"]
            del datum["id"]
        except Exception:
            logging.info(
                'tried to deleting something that doesn"t exist, don"t worry it still doesn"t exist'
            )
        table_name = datum.pop("table")
        dest_table = get_table(dest_metadata_obj, dest_engine, table_name)
        dest_engine.execute(
            sqlalchemy.dialects.postgresql.insert(dest_table).on_conflict_do_nothing(),
            [datum],
        )
        dest_session.commit()


def get_table(_metadata_obj, _engine, _table_name):
    try:
        return Table(f"airflow.{_table_name}", _metadata_obj, autoload_with=_engine)
    except NoSuchTableError:
        return Table(_table_name, _metadata_obj, autoload_with=_engine)
