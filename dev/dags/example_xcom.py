from datetime import datetime

from airflow.decorators import dag, task
from flask import ctx


@task
def example_task():
    from airflow.io.path import ObjectStoragePath
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    ctx["ti"].xcom_push(
        key="example_str",
        value="This is an example string value for XCom.",
    )

    ctx["ti"].xcom_push(
        key="example_int",
        value=42,
    )

    ctx["ti"].xcom_push(
        key="example_none",
        value=None,
    )

    return ObjectStoragePath(
        "s3://my-bucket/example/object.txt", conn_id="example_conn"
    )


@dag(
    schedule="@hourly",
    start_date=datetime(2025, 6, 1),
    catchup=False,
)
def example_xcom():
    example_task()


example_xcom()
