import json

import sqlalchemy
from airflow import settings
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.exc import NoSuchTableError
import logging
import os
from flask import jsonify
import urllib.parse
import requests
import datetime


def get_table(_metadata_obj, _engine, _table_name):
    try:
        return Table(f'airflow.{_table_name}', _metadata_obj, autoload_with=_engine)
    except NoSuchTableError:
        return Table(_table_name, _metadata_obj, autoload_with=_engine)
def migrate(table_name: str, batch_size: int = 100000, dag_id:str=''):

        logging.info(f"Creating source SQL Connections ..")
        # sql_alchemy_conn = os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"]
        # conn_url = f"{sql_alchemy_conn}/postgres"
        # source_engine = create_engine(conn_url, echo=False)
        session = settings.Session()
        source_engine = session.get_bind()
        source_metadata_obj = MetaData(bind=source_engine)
        source_table = get_table(source_metadata_obj, source_engine, table_name)


        # noinspection SqlInjection
        logging.info(f"Fetching data for {table_name} from source...")
        source_result = source_engine.execution_options(stream_results=True) \
            .execute(f"SELECT * FROM {source_table.name} where dag_id='{dag_id}' limit 1")
        rtn=[]
        for result in source_result.fetchall():
            result=dict(result._asdict())
            for key,value in result.items():
                if isinstance(value,memoryview):
                    result[key]=value.tobytes().decode('iso-8859-1')
                if isinstance(value,datetime.datetime):
                    result[key]=f"{value.strftime('%Y-%m-%d %H:%M:%S.%f+00')}"
            result["table"]=table_name
            rtn.append(result)

        return rtn
    # except Exception as e:
    #     return str(e)

def migrate_dag(dag:str,deployment_url:str,deployment:str,csfr:dict,token:str):
    result=[]
    for table in  [
            "dag_run",
            "task_instance",
            # "task_reschedule",
            # "trigger",
            # "task_fail",
            # "xcom",
            # "rendered_task_instance_fields",
            # "sensor_instance",
            # "sla_miss"
                   ]:
        result.append(migrate(table_name=table,dag_id=dag))
    headers={
     'Content-Type':'application/json',
        "Authorization": f"Bearer {token}"
      }
    logging.info(f"curl --location {deployment_url}/astromigration/daghistory/receive/{deployment}/astro/{dag}/send --header {json.dumps(headers)} --data '{json.dumps(result[0][0])}'" )
    requests.post(f"{deployment_url}/astromigration/daghistory/receive/{deployment}/astro/{dag}/send",data=json.dumps(result[0][0]),headers=headers)

    return str(type(result[0][0]))


def receive_dag(dag:str=None,deployment:str=None,dest:str=None,action:str=None,data:dict={}):
    try:
        # data=data.to_dict()
        # del data['csrf_token']
        #TODO: deleting this unitl i figure out how to handle bools & bytes
        del data['external_trigger']
        del data['conf']
        del data['id']
        table_name=data.pop('table')
        # sql_alchemy_conn = os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"]
        # conn_url = f"{sql_alchemy_conn}/postgres"
        # dest_session = settings.Session()
        # dest_engine = create_engine(conn_url, echo=False)

        dest_session = settings.Session()
        # dest_session = session.e

        dest_engine = dest_session.get_bind()
        dest_metadata_obj = MetaData(bind=dest_engine)
        dest_table = get_table(dest_metadata_obj, dest_engine, table_name)
        dest_engine.execute(
            sqlalchemy.dialects.postgresql.insert(dest_table).on_conflict_do_nothing(),
            # ### ^POTENTIALLY CHANGE HERE^ ###
            [data]
        )
        dest_session.commit()
    except Exception as e:
        return str(e)
    # urllib.parse.urlencode(data)
    # return urllib.parse.urlencode(data)

