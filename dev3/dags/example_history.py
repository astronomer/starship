from datetime import datetime, timedelta, timezone

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


def days_ago(n: int = 0, hour=0, minute=0, second=0, microsecond=0) -> datetime:
    today = datetime.now(tz=timezone.utc).replace(hour=hour, minute=minute, second=second, microsecond=microsecond)
    return today - timedelta(days=n)


@dag(
    schedule="@daily",
    start_date=days_ago(365),
    catchup=True,
    tags=["history"],
)
def example_history():
    """A DAG with a long history of tasks."""

    BashOperator(
        task_id="print_run_id",
        bash_command="echo 'run_id={{ run_id }}'",
    )


example_history()
