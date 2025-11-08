from datetime import datetime

from airflow.decorators import dag, task


@task
def run():
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

    return ObjectStoragePath("s3://my-bucket/example/object.txt", conn_id="example_conn")


@dag(
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["xcom"],
)
def example_xcom():
    """A DAG with XCom usage."""
    run()


example_xcom()
