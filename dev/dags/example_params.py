from datetime import datetime

from airflow.decorators import dag
from airflow.models import Param
from airflow.operators.bash import BashOperator


@dag(
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    params={
        "example_param": Param("default_value", type="string", description="An example parameter"),
    },
    tags=["params"],
)
def example_params():
    """A DAG which uses params."""

    BashOperator(
        task_id="run",
        bash_command="echo 'example_param={{ params.example_param }}'",
    )


example_params()
