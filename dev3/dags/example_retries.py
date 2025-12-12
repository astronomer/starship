import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.sdk import dag, get_current_context, task

logger = logging.getLogger(__name__)


@task
def run():
    ctx = get_current_context()
    ti = ctx["ti"]

    if ti.try_number < 3:
        raise AirflowException("Simulated task failure to demonstrate retries.")

    logger.info("Task succeeded on attempt %s", ti.try_number)


@dag(
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["retries"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=1),
    },
    render_template_as_native_obj=True,
)
def example_retries():
    """A DAG with a task which retries 2 times until success."""

    run()


example_retries()
