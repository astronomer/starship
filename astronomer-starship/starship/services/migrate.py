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
        session = settings.Session()
        source_engine = session.get_bind()
        source_metadata_obj = MetaData(bind=source_engine)
        source_table = get_table(source_metadata_obj, source_engine, table_name)


        # noinspection SqlInjection
        logging.info(f"Fetching data for {table_name} from source...")
        source_result = source_engine.execution_options(stream_results=True) \
            .execute(f"SELECT * FROM {source_table.name} where dag_id='{dag_id}' order by start_date desc limit 5")
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
            source_table = get_table(source_metadata_obj, source_engine, "task_instance")
            sub_result=source_engine.execution_options(stream_results=True) \
            .execute(f"SELECT * FROM {source_table.name} where dag_id='{dag_id}' and run_id='{result['run_id']}'")
            for ti_result in sub_result.fetchall():
                ti_result_new=dict(ti_result._asdict())
                ti_result=dict(ti_result._asdict())
                ti_result["table"]='task_instance'
                for ti_key,ti_value in ti_result_new.items():
                    if isinstance(ti_value, memoryview):
                        del ti_result[ti_key]
                    if isinstance(ti_value, datetime.datetime):
                        ti_result[ti_key] = f"{ti_value.strftime('%Y-%m-%d %H:%M:%S.%f+00')}"
                rtn.append(ti_result)


        return rtn

def migrate_dag(dag:str,deployment_url:str,deployment:str,csfr:dict,token:str):
    result=[]
    for table in  [
            "dag_run"
                   ]:
        result.append(migrate(table_name=table,dag_id=dag))
    headers={
     'Content-Type':'application/json',
        "Authorization": f"Bearer {token}"
      }

    # deployment_url="http://host.docker.internal:8081"
    logging.info(f"curl --location {deployment_url}/astromigration/daghistory/receive/{deployment}/astro/{dag}/send --header {json.dumps(headers)} --data '{json.dumps(result[0])}'" )
    requests.post(f"{deployment_url}/astromigration/daghistory/receive/{deployment}/astro/{dag}/send",data=json.dumps(result[0]),headers=headers)



    return str(type(result[0][0]))


def receive_dag(dag:str=None,deployment:str=None,dest:str=None,action:str=None,data:list={}):
    try:
        dest_session = settings.Session()
        dest_engine = dest_session.get_bind()
        dest_metadata_obj = MetaData(bind=dest_engine)
        for datum in data:
            try:
                # del datum['external_trigger']
                del datum['conf']
                del datum['id']
            except:
                logging.info('tried to deleting something that doesn"t exist, don"t worry it still doesn"t exist')
            table_name=datum.pop('table')
            dest_table = get_table(dest_metadata_obj, dest_engine, table_name)
            dest_engine.execute(
                sqlalchemy.dialects.postgresql.insert(dest_table).on_conflict_do_nothing(),
                [datum]
            )
            dest_session.commit()
    except Exception as e:
        return str(e)


