from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Param


@task(map_index_template="{{ params.name }}")
def example_task(name: str, rendered: str):
    from airflow.operators.python import get_current_context

    get_current_context()["params"]["name"] = name
    msg = f"Hello {name}, with example_param={rendered}!"
    print(msg)
    return msg


@dag(
    schedule="@hourly",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    params={
        "example_param": Param(
            "default_value",
            type="string",
            description="An example parameter",
        )
    },
)
def example_mapped():
    example_task.partial(
        rendered="{{ params.example_param }}",
    ).expand(
        name=["Alice", "Bob", "Charlie"],
    )


example_mapped()
