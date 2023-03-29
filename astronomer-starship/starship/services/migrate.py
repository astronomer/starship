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


def get_table(_metadata_obj, _engine, _table_name):
    try:
        return Table(f'airflow.{_table_name}', _metadata_obj, autoload_with=_engine)
    except NoSuchTableError:
        return Table(_table_name, _metadata_obj, autoload_with=_engine)
def migrate(table_name: str, batch_size: int = 100000, dag_id:str=''):
    try:
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
        for result in source_result:
            rtn.append(result._mapping)

        return rtn
    except Exception as e:
        return str(e)

def migrate_dag(dag:str,deployment_url:str,deployment:str,csfr:dict):
    result=[]
    for table in  [
        "dag_run",
        #         "task_instance",
        #         "task_reschedule",
        #         "trigger",
        #         "task_fail",
        #         "xcom",
        #         "rendered_task_instance_fields",
        #         "sensor_instance",
        #         "sla_miss"
                   ]:
        result.append(migrate(table_name=table,dag_id=dag))
    data = urllib.parse.urlencode(result[0][0])
    data+='&'+urllib.parse.urlencode(csfr)
    # headers={
    # # 'User-Agent':' Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0',
    # 'Accept':'*/*' ,
    # 'Accept-Language':'en-US,en;q=0.5',
    # 'Accept-Encoding':'gzip, deflate, br' ,
    # # 'HX-Request':' true',
    # # 'HX-Target':' dag-example_dag_basic',
    # # 'HX-Current-URL':' http://localhost:8080/astromigration/',
    # 'Content-Type':'application/x-www-form-urlencoded',
    # # 'Origin':' http://localhost:8080',
    # # 'DNT':' 1',
    # 'Connection':'keep-alive',
    # # 'Referer':' http://localhost:8080/astromigration/',
    # # 'Cookie':' session=efe9be01-b45a-4450-aa58-07ad2b87e1b4.FgYSPmVAL1T-ONa5jvy1_yubNmc',
    # 'Sec-Fetch-Dest':'empty',
    # 'Sec-Fetch-Mode':'cors',
    # 'Sec-Fetch-Site':'same-origin',
    # }
    # requests.post(f"http://localhost:8080/astromigration/daghistory/receive/clfr61wko539379c0fn377567g/astro/{dag}/send",data=data,headers=headers)
    # return str(type(result[0][0]))
    return data


def receive_dag(dag:str=None,deployment:str=None,dest:str=None,action:str=None,data:dict={}):
    data=data.to_dict()
    del data['csrf_token']
    #TODO: deleting this unitl i figure out how to handle bools & bytes
    del data['external_trigger']
    del data['conf']
    table_name='dag_run'
    # sql_alchemy_conn = os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"]
    # conn_url = f"{sql_alchemy_conn}/postgres"
    # dest_session = settings.Session()
    # dest_engine = create_engine(conn_url, echo=False)

    dest_session = settings.Session()
    dest_engine = dest_session.get_bind()
    dest_metadata_obj = MetaData(bind=dest_engine)
    dest_table = get_table(dest_metadata_obj, dest_engine, table_name)
    dest_engine.execute(
        sqlalchemy.dialects.postgresql.insert(dest_table).on_conflict_do_nothing(),
        # ### ^POTENTIALLY CHANGE HERE^ ###
        [data]
    )
    dest_session.commit()

    # urllib.parse.urlencode(data)
    # return urllib.parse.urlencode(data)

