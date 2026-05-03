"""Starship cutover wave — template DAG.

Copy this file into your project's ``dags/`` folder and edit the settings at
the top of the file. Each resolved DAG becomes its own mapped task, so
Airflow handles parallelism, per-DAG retries, and failure isolation natively.

The source Airflow credential is taken from the Airflow Connection named
``SOURCE_CONN_ID`` below — by default this is ``starship_source``, the
connection written by the Starship Setup page. If you prefer to drive the
wave entirely from Airflow (no UI), create that Connection manually. The
auth factory dispatches on Airflow's standard ``conn_type``:

- Astro / OSS bearer — ``Conn Type``: HTTP, ``Host``: source base URL
  (e.g. ``https://<hash>.astronomer.run/<deployment-slug>``),
  ``Password``: bearer token.
- OSS HTTP Basic — ``Conn Type``: HTTP, ``Host`` as above, ``Login`` +
  ``Password`` set (no token).
- GCC (Cloud Composer) — ``Conn Type``: Google Cloud, ``Host``: Composer
  Airflow URL. Uses Application Default Credentials. Optional
  ``Extra``: ``{"impersonation_chain": ["sa@..."]}``.
- MWAA — ``Conn Type``: Amazon Web Services, ``Host``: MWAA web URL.
  ``Extra``: ``{"region_name": "us-west-2", "environment_name": "my-env"}``.
"""

from datetime import datetime
from typing import List

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3

if AIRFLOW_V_3:
    from airflow.sdk import DAG, task
elif AIRFLOW_V_2:
    from airflow import DAG
    from airflow.decorators import task
else:
    raise RuntimeError("Unsupported Airflow version")

from astronomer_starship.providers.starship.auth.factory import resolve_source_hook
from astronomer_starship.providers.starship.cutover import resolve_dag_patterns
from astronomer_starship.providers.starship.hooks.starship import StarshipLocalHook
from astronomer_starship.providers.starship.operators.starship import (
    StarshipCutoverMigrationOperator,
)

# ---------------------------------------------------------------------------
# Wave configuration — edit these for your migration.
# ---------------------------------------------------------------------------

#: Airflow Connection id for the source Airflow. Matches the Starship UI default.
SOURCE_CONN_ID = "starship_source"

#: fnmatch patterns of DAGs to include. Empty list = all DAGs present on both
#: source and local (big-bang mode).
INCLUDE_PATTERNS: List[str] = []

#: Maximum DAG runs to fetch per DAG. Task instances scale with this.
DAG_RUN_LIMIT = 500

#: Pause each DAG on the source after migrating it.
PAUSE_DAG_IN_SOURCE = True

#: Unpause each DAG on this (target) Airflow after migrating it.
UNPAUSE_DAG_IN_TARGET = False


with DAG(
    dag_id="starship_cutover_wave_template",
    schedule=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
    tags=["starship", "cutover", "migration"],
    default_args={"owner": "Astronomer"},
    doc_md=__doc__,
) as dag:

    @task
    def resolve_targets() -> List[str]:
        """Cross-reference INCLUDE_PATTERNS against source + local DAGs."""
        source_hook = resolve_source_hook(SOURCE_CONN_ID)
        local_hook = StarshipLocalHook()
        dag_ids = resolve_dag_patterns(
            source_hook=source_hook,
            patterns=INCLUDE_PATTERNS,
            local_hook=local_hook,
        )
        if not dag_ids:
            raise RuntimeError(
                "No DAGs matched — check INCLUDE_PATTERNS and that the DAGs exist on both source and target."
            )
        return dag_ids

    StarshipCutoverMigrationOperator.partial(
        task_id="migrate",
        source_conn_id=SOURCE_CONN_ID,
        dag_run_limit=DAG_RUN_LIMIT,
        pause_dag_in_source=PAUSE_DAG_IN_SOURCE,
        unpause_dag_in_target=UNPAUSE_DAG_IN_TARGET,
        map_index_template="{{ task.target_dag_id }}",
    ).expand(target_dag_id=resolve_targets())
