from datetime import datetime

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3

if AIRFLOW_V_2:
    from airflow import DAG
    from airflow.operators.bash import BashOperator

    with DAG(
        dag_id="dag_0",
        tags=["foo", "bar"],
        is_paused_upon_creation=True,
        default_args={"owner": "baz"},
        schedule_interval="@once",
        start_date=datetime(1970, 1, 1),
    ) as dag:
        BashOperator(task_id="operator_0", bash_command="echo hi")
elif AIRFLOW_V_3:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG

    with DAG(
        dag_id="dag_0",
        tags=["foo", "bar"],
        is_paused_upon_creation=True,
        default_args={"owner": "baz"},
        schedule="@once",
        start_date=datetime(1970, 1, 1),
    ) as dag:
        BashOperator(task_id="operator_0", bash_command="echo hi")
