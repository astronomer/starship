"""
## This is an extremely long DAG description designed specifically to test UI display limits

This DAG was created for the sole purpose of testing how the Airflow UI handles
extremely long text values in various fields such as the DAG name, description,
owner, and other metadata fields. It is important to verify that the UI can
gracefully handle overflow, truncation, tooltips, and other display mechanisms
when dealing with unusually long strings that might be encountered in
production environments where users may not always follow naming conventions.

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore
eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt
in culpa qui officia deserunt mollit anim id est laborum.

This description continues with more text to really push the limits of the UI
rendering capabilities. The goal is to see if scrollbars appear, if text gets
truncated with ellipsis, or if the layout breaks in any unexpected way.
"""

from airflow.decorators import dag, task
from pendulum import datetime


VERY_LONG_OWNER_NAME = (
    "John Jacob Jingleheimer Schmidt Senior Junior III Esquire PhD MBA "
    "Chief Executive Officer President Vice President Director Manager "
    "Supervisor Team Lead Principal Staff Senior Distinguished Fellow "
    "Architect Engineer Developer Administrator Consultant Specialist"
)


@dag(
    dag_id=(
        "this_is_an_extremely_long_dag_name_designed_to_test_how_the_airflow_"
        "ui_handles_very_long_dag_identifiers_which_should_never_be_used_in_"
        "production_but_we_are_testing_edge_cases_to_ensure_proper_truncation_"
        "and_overflow_handling_in_all_ui_views_249_char"
    ),  # Exactly 249 characters
    start_date=datetime(2024, 1, 1),
    # Using a complex cron schedule to test schedule display
    schedule="0 0 1,15 1,2,3,4,5,6,7,8,9,10,11,12 1,2,3,4,5",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": VERY_LONG_OWNER_NAME,
        "retries": 3,
    },
    tags=[
        "test",
        "ui-display-test",
        "very-long-tag-name-that-should-test-tag-overflow-behavior",
        "another-extremely-long-tag-that-nobody-would-ever-use-in-real-life",
        "edge-case",
        "stress-test",
        "overflow-test",
        "truncation-test",
    ],
    description=(
        "This is an incredibly verbose and excessively long description that "
        "is meant to test how the Airflow scheduler and web UI handle DAGs "
        "with extremely lengthy description strings that would normally never "
        "be written by any reasonable developer but might occur accidentally "
        "or through automated DAG generation tools or configuration systems "
        "that do not impose reasonable length limits on their output fields."
    ),
)
def this_is_an_extremely_long_dag_name_that_tests_ui():
    """
    DAG function with a long name to test function name display.
    """

    @task
    def task_with_a_very_long_name_that_is_designed_to_test_task_name_overflow_and_truncation_behavior_in_the_graph_view_and_grid_view():
        """
        A simple task with an extremely long name.

        :return: A confirmation message.
        :rtype: str
        """
        return "Task completed successfully!"

    @task
    def another_long_task_name_for_additional_ui_stress_testing_purposes_to_verify_consistent_behavior_across_multiple_tasks(
        message: str,
    ):
        """
        A second task that depends on the first.

        :param message: Message from upstream task.
        :type message: str
        """
        print(f"Received: {message}")

    result = task_with_a_very_long_name_that_is_designed_to_test_task_name_overflow_and_truncation_behavior_in_the_graph_view_and_grid_view()
    another_long_task_name_for_additional_ui_stress_testing_purposes_to_verify_consistent_behavior_across_multiple_tasks(
        result
    )


this_is_an_extremely_long_dag_name_that_tests_ui()
