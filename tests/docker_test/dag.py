from datetime import datetime
from airflow import DAG

try:
    from airflow.operators.bash import BashOperator
except ImportError:
    from airflow.providers.standard.operators.bash import BashOperator

try:
    with DAG(
        dag_id="dag_0",
        tags=["foo", "bar"],
        is_paused_upon_creation=True,
        default_args={"owner": "baz"},
        schedule_interval="@once",
        start_date=datetime(1970, 1, 1),
    ) as dag:
        BashOperator(task_id="operator_0", bash_command="echo hi")
except TypeError:
    with DAG(
        dag_id="dag_0",
        tags=["foo", "bar"],
        is_paused_upon_creation=True,
        default_args={"owner": "baz"},
        schedule="@once",
        start_date=datetime(1970, 1, 1),
    ) as dag:
        BashOperator(task_id="operator_0", bash_command="echo hi")
