from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


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
