from datetime import datetime

from airflow.decorators import dag, task


@task(map_index_template="{{ params.name }}")
def run(name: str):
    from airflow.operators.python import get_current_context

    get_current_context()["params"]["name"] = name
    print(f"Hello {name}!")


@dag(
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["mapped"],
)
def example_mapped():
    """A DAG with mapped tasks."""

    run.expand(
        name=["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
    )


example_mapped()
