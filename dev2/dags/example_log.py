import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Param

logger = logging.getLogger(__name__)


@task
def run(num_lines: int):
    for i in range(num_lines):
        logger.info("Log line %s", i)


@dag(
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    params={
        "num_lines": Param(1000, type="number", description="How many lines of log to generate."),
    },
    tags=["log"],
    render_template_as_native_obj=True,
)
def example_log():
    """A DAG with a task which generates a large log file."""

    run(num_lines="{{ params.num_lines }}")


example_log()
