"""
Core migration service for plugin-based cutover.

Provides:
- State management via Airflow Variables (survives restarts, works cross-pod)
- DAG pattern resolution with fnmatch wildcards
- Per-DAG migration logic
- Rollback: delete migrated data from destination, reverse pause/unpause
- Abort: stop a running migration via threading.Event
- Wait-for-running: defer DAGs with active runs in source, retry later
- Background thread orchestrator with parallel execution
"""

import fnmatch
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

from airflow.models import DagModel, DagRun, TaskInstance, Variable
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy import select
from sqlalchemy.orm.session import Session

from astronomer_starship.cutover.auth import CutoverHttpHook, get_source_hook
from astronomer_starship.providers.starship.hooks.starship import (
    StarshipLocalHook,
)

SYSTEM_DAGS_TO_EXCLUDE = {
    "airflow_monitoring",
}

MIGRATION_DAG_PREFIXES = ("starship_airflow_migration_dag",)

logger = logging.getLogger(__name__)

VARIABLE_KEY = "starship_cutover_state"

# Lock to prevent concurrent state writes from racing
_state_lock = threading.Lock()

# Active abort events keyed by migration_id
_abort_events: Dict[str, threading.Event] = {}

# In-memory step labels: {migration_id: {dag_id: step_label}}
# Used to avoid writing to the Variable on every step change. Only terminal
# states (completed, failed, etc.) are persisted to the Variable.
_step_labels: Dict[str, Dict[str, str]] = {}
_step_labels_lock = threading.Lock()

# Non-terminal DAG run states in Airflow
RUNNING_STATES = {"running", "queued", "up_for_retry", "up_for_reschedule"}


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_state() -> dict:
    """Read and parse the migration state from the Airflow Variable."""
    try:
        raw = Variable.get(VARIABLE_KEY, default_var=None)
        if raw is None:
            return {"migrations": []}
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupt migration state variable -- resetting.")
        return {"migrations": []}


def save_state(state: dict) -> None:
    """Serialize and write the migration state to the Airflow Variable."""
    Variable.set(VARIABLE_KEY, json.dumps(state, default=str))


def create_migration(
    migration_type: str,
    config: dict,
    dag_ids: List[str],
) -> str:
    """Create a new migration entry and return its ID."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    migration_id = f"{migration_type}_{ts}_{os.urandom(3).hex()}"

    migration = {
        "id": migration_id,
        "type": migration_type,
        "status": "running",
        "abort_requested": False,
        "started_at": _now_iso(),
        "completed_at": None,
        "config": config,
        "dags": {
            dag_id: {
                "status": "pending",
                "step": None,
                "dag_runs_migrated": 0,
                "task_instances_migrated": 0,
                "latest_data_interval_end": None,
                "source_paused_by_us": False,
                "dest_unpaused_by_us": False,
                "error": None,
            }
            for dag_id in dag_ids
        },
    }

    with _state_lock:
        state = get_state()
        state["migrations"].append(migration)
        save_state(state)

    return migration_id


def get_migration(migration_id: str) -> Optional[dict]:
    """Return a specific migration entry or None."""
    state = get_state()
    for m in state["migrations"]:
        if m["id"] == migration_id:
            return m
    return None


def set_step_label(migration_id: str, dag_id: str, label: str) -> None:
    """Update the in-memory step label for a DAG (no DB write)."""
    with _step_labels_lock:
        _step_labels.setdefault(migration_id, {})[dag_id] = label


def get_step_label(migration_id: str, dag_id: str) -> Optional[str]:
    """Read the in-memory step label for a DAG."""
    with _step_labels_lock:
        return _step_labels.get(migration_id, {}).get(dag_id)


def cleanup_step_labels(migration_id: str) -> None:
    """Remove all in-memory step labels for a migration."""
    with _step_labels_lock:
        _step_labels.pop(migration_id, None)


def update_dag_status(
    migration_id: str,
    dag_id: str,
    status: str,
    **details,
) -> None:
    """Update per-DAG state within a migration.

    For non-terminal updates (status='running' with only a step change),
    use set_step_label() instead to avoid lock contention on the Variable.
    """
    with _state_lock:
        state = get_state()
        for m in state["migrations"]:
            if m["id"] == migration_id:
                if dag_id in m["dags"]:
                    m["dags"][dag_id]["status"] = status
                    for k, v in details.items():
                        m["dags"][dag_id][k] = v
                break
        save_state(state)


def update_migration_status(migration_id: str, status: str) -> None:
    """Update the overall migration status."""
    with _state_lock:
        state = get_state()
        for m in state["migrations"]:
            if m["id"] == migration_id:
                m["status"] = status
                if status in ("completed", "failed", "rolled_back", "aborted"):
                    m["completed_at"] = _now_iso()
                break
        save_state(state)


# ---------------------------------------------------------------------------
# Abort mechanism
# ---------------------------------------------------------------------------


def request_abort(migration_id: str) -> None:
    """Signal a running migration to stop.

    Persists the flag in the Airflow Variable so it works across Gunicorn
    workers.  Also sets the in-memory event if this happens to be the same
    process running the migration thread.
    """
    # Persist to Variable (works cross-process)
    with _state_lock:
        state = get_state()
        for m in state["migrations"]:
            if m["id"] == migration_id:
                m["abort_requested"] = True
                break
        save_state(state)

    # Also set in-memory event if available (fast path, same process)
    event = _abort_events.get(migration_id)
    if event:
        event.set()

    logger.info("Abort requested for migration %s.", migration_id)


_abort_check_cache: Dict[str, float] = {}
_ABORT_CHECK_TTL = 5  # seconds between DB checks


def _is_aborted(migration_id: str) -> bool:
    """Check if abort has been requested for this migration.

    Checks the in-memory event first (fast, same-process). Falls back to
    the persisted Variable state (works cross-process / cross-worker) but
    rate-limits DB reads to once per _ABORT_CHECK_TTL seconds.
    """
    # Fast path: in-memory event
    event = _abort_events.get(migration_id)
    if event is not None and event.is_set():
        return True

    # Slow path: check persisted state, but rate-limit DB reads
    now = time.monotonic()
    last_check = _abort_check_cache.get(migration_id, 0)
    if now - last_check < _ABORT_CHECK_TTL:
        return False

    _abort_check_cache[migration_id] = now
    try:
        m = get_migration(migration_id)
    except Exception:
        logger.debug("Failed to read abort state for %s", migration_id, exc_info=True)
        return False
    if m and m.get("abort_requested"):
        # Sync in-memory event so subsequent checks are fast
        if event is not None:
            event.set()
        return True

    return False


def _cleanup_abort_event(migration_id: str) -> None:
    """Remove the abort event after migration finishes."""
    _abort_events.pop(migration_id, None)
    _abort_check_cache.pop(migration_id, None)


# ---------------------------------------------------------------------------
# DAG pattern resolution
# ---------------------------------------------------------------------------


def resolve_dag_patterns(
    source_hook: CutoverHttpHook,
    patterns: List[str],
    local_hook: Optional[StarshipLocalHook] = None,
) -> List[str]:
    """Resolve fnmatch patterns against source DAGs, cross-ref with local.

    Args:
        source_hook: CutoverHttpHook connected to source.
        patterns: List of patterns (e.g. ["etl_*", "reporting_daily"]).
            Empty list means "all DAGs" (big-bang mode).
        local_hook: StarshipLocalHook for cross-referencing. Created if None.

    Returns:
        Sorted list of resolved dag_ids present in both source and destination.
    """
    if local_hook is None:
        local_hook = StarshipLocalHook()

    all_source_dags = source_hook.get_dags()
    source_dag_ids = [d["dag_id"] for d in all_source_dags]
    local_dag_ids = {d["dag_id"] for d in local_hook.get_dags()}

    # Filter by patterns (empty = all)
    if patterns:
        matched = set()
        for pattern in patterns:
            pattern = pattern.strip()
            if not pattern:
                continue
            for dag_id in source_dag_ids:
                if fnmatch.fnmatch(dag_id, pattern):
                    matched.add(dag_id)
        candidates = list(matched)
    else:
        candidates = list(source_dag_ids)

    # Exclude system and migration DAGs
    filtered = [d for d in candidates if d not in SYSTEM_DAGS_TO_EXCLUDE and not d.startswith(MIGRATION_DAG_PREFIXES)]

    # Cross-reference with local
    resolved = sorted(d for d in filtered if d in local_dag_ids)
    skipped = sorted(d for d in filtered if d not in local_dag_ids)

    if skipped:
        logger.warning(
            "Skipping %d DAGs not deployed locally: %s",
            len(skipped),
            skipped[:20],
        )

    logger.info(
        "Resolved %d DAGs from %d pattern(s) (%d skipped -- not local).",
        len(resolved),
        len(patterns),
        len(skipped),
    )
    return resolved


# ---------------------------------------------------------------------------
# Wait-for-running check
# ---------------------------------------------------------------------------


def _dag_has_active_runs(source_hook: CutoverHttpHook, dag_id: str) -> bool:
    """Check if the source DAG has any active (non-terminal) runs."""
    try:
        dag_runs = source_hook.get_dag_runs(dag_id=dag_id, limit=100)
        for dr in dag_runs.get("dag_runs", []):
            if dr.get("state") in RUNNING_STATES:
                return True
    except Exception:
        logger.warning(
            "Could not check active runs for %s -- assuming active (conservative).",
            dag_id,
            exc_info=True,
        )
        return True
    return False


# ---------------------------------------------------------------------------
# Per-DAG migration logic
# ---------------------------------------------------------------------------


class _AbortedError(Exception):
    """Raised when abort is detected mid-migration."""


def migrate_dag(  # noqa: C901
    dag_id: str,
    source_hook: CutoverHttpHook,
    dest_hook: StarshipLocalHook,
    dag_run_limit: int = 500,
    wait_for_scheduler: bool = False,
    on_step=None,
    migration_id: Optional[str] = None,
) -> dict:
    """Migrate a single DAG's history from source to destination.

    Args:
        on_step: Optional callback(step_label) called at each phase for UI updates.
        migration_id: If provided, abort is checked before each source API call.

    Returns a result dict with counts and latest_data_interval_end.
    Does NOT handle pause/unpause -- caller does that.

    Raises _AbortedError if abort is detected before touching source.
    """

    def _step(label):
        if on_step:
            on_step(label)

    def _check_abort():
        """Raise _AbortedError if abort was requested (before source interaction)."""
        if migration_id and _is_aborted(migration_id):
            raise _AbortedError(f"Abort requested for {dag_id}")

    # --- Pre-migration checks ---
    _check_abort()
    _step("Pre-checks")
    logger.info("[%s] Running pre-migration checks...", dag_id)

    try:
        source_dag = source_hook.get_dag(dag_id=dag_id)
        logger.info("[%s] Source DAG found -- %s.", dag_id, "paused" if source_dag.get("is_paused") else "active")
    except Exception as e:
        raise RuntimeError(f"DAG '{dag_id}' not found or inaccessible in source.") from e

    from sqlalchemy.exc import NoResultFound

    try:
        dest_dag = dest_hook.get_dag(dag_id=dag_id)
    except NoResultFound as e:
        raise RuntimeError(f"DAG '{dag_id}' not found in destination. Deploy it before migrating.") from e

    if not dest_dag["is_paused"]:
        raise RuntimeError(f"DAG '{dag_id}' is active in destination. Pause it before migrating.")

    if dest_dag["dag_run_count"] != 0:
        raise RuntimeError(f"DAG '{dag_id}' already has runs in destination. Clear them first.")

    logger.info("[%s] Pre-checks passed (dest paused, no existing runs).", dag_id)

    # --- Fetch dag_runs ---
    _check_abort()
    _step("Fetching DAG runs")
    logger.info("[%s] Fetching up to %d DAG runs from source...", dag_id, dag_run_limit)
    dag_runs_resp = source_hook.get_dag_runs(dag_id=dag_id, limit=dag_run_limit)
    dag_runs = dag_runs_resp.get("dag_runs", [])

    if not dag_runs:
        logger.info("[%s] No DAG runs found in source -- skipping.", dag_id)
        return {
            "dag_runs_migrated": 0,
            "task_instances_migrated": 0,
            "latest_data_interval_end": None,
        }

    logger.info("[%s] Fetched %d DAG runs.", dag_id, len(dag_runs))

    # --- Fetch task instances ---
    # Use a much higher limit for TIs: each DAG run can have many tasks.
    ti_limit = dag_run_limit * 200
    _check_abort()
    _step("Fetching task instances")
    logger.info("[%s] Fetching task instances from source...", dag_id)
    ti_resp = source_hook.get_task_instances(dag_id=dag_id, limit=ti_limit)
    task_instances = ti_resp.get("task_instances", [])
    logger.info("[%s] Fetched %d task instances.", dag_id, len(task_instances))

    # --- Write dag_runs ---
    _step("Writing DAG runs")
    logger.info("[%s] Writing %d DAG runs to destination...", dag_id, len(dag_runs))
    dest_hook.set_dag_runs(dag_runs=dag_runs)

    _intervals = [dr["data_interval_end"] for dr in dag_runs if dr.get("data_interval_end")]
    latest_data_interval_end = max(_intervals) if _intervals else None
    logger.info("[%s] Latest data_interval_end: %s", dag_id, latest_data_interval_end)

    # --- Write task instances ---
    if task_instances:
        _step("Writing task instances")
        logger.info("[%s] Writing %d task instances to destination...", dag_id, len(task_instances))
        dest_hook.set_task_instances(task_instances=task_instances)

    # --- Migrate task instance history (AF 2.10+) ---
    # NOTE: No _check_abort() here -- we've already written dag_runs and TIs
    # to destination. Stopping now would leave partial data. Let it finish
    # the remaining writes so rollback has consistent state.
    ti_history_count = 0
    _step("Fetching TI history")
    try:
        logger.info("[%s] Fetching task instance history from source...", dag_id)
        history = source_hook.get_task_instance_history(dag_id=dag_id, limit=ti_limit)
        history_records = history.get("task_instances", [])
        if history_records:
            _step("Writing TI history")
            logger.info("[%s] Writing %d TI history records to destination...", dag_id, len(history_records))
            dest_hook.set_task_instance_history(task_instances=history_records)
            ti_history_count = len(history_records)
        else:
            logger.info("[%s] No TI history records found.", dag_id)
    except Exception:
        logger.info("[%s] TI history not available (pre-AF 2.10). Skipping.", dag_id)

    # --- Optionally wait for scheduler to update next_dagrun ---
    if wait_for_scheduler:
        _step("Waiting for scheduler")
        logger.info("[%s] Waiting for scheduler to sync next_dagrun...", dag_id)
        _wait_for_scheduler_sync(dag_id, latest_data_interval_end)

    logger.info("[%s] Migration data transfer complete.", dag_id)

    return {
        "dag_runs_migrated": len(dag_runs),
        "task_instances_migrated": len(task_instances),
        "task_instance_history_migrated": ti_history_count,
        "latest_data_interval_end": latest_data_interval_end,
    }


@provide_session
def _get_next_dagrun(dag_id: str, session: Session = NEW_SESSION):
    """Quick session-scoped query -- session is released when function returns."""
    return session.scalar(select(DagModel.next_dagrun).where(DagModel.dag_id == dag_id))


@provide_session
def _get_schedule_interval(dag_id: str, session: Session = NEW_SESSION):
    """Quick session-scoped query for schedule interval."""
    return session.scalar(select(DagModel.schedule_interval).where(DagModel.dag_id == dag_id))


def _wait_for_scheduler_sync(
    dag_id: str,
    latest_data_interval_end: str,
    timeout: int = 60,
) -> None:
    """Wait for the scheduler to update next_dagrun past the migrated runs.

    Uses session-per-poll to avoid holding DB connections open for the
    entire polling duration.
    """
    if latest_data_interval_end is None:
        return

    target_dt = datetime.fromisoformat(str(latest_data_interval_end))
    if target_dt.tzinfo is None:
        target_dt = target_dt.replace(tzinfo=timezone.utc)

    if _get_schedule_interval(dag_id) is None:
        return

    start = datetime.now(timezone.utc)
    while (datetime.now(timezone.utc) - start).total_seconds() < timeout:
        next_dagrun = _get_next_dagrun(dag_id)
        if next_dagrun is None or next_dagrun >= target_dt:
            return
        time.sleep(5)

    logger.warning(
        "Scheduler sync timeout for %s after %ds -- continuing.",
        dag_id,
        timeout,
    )


def pause_unpause_dag(
    dag_id: str,
    source_hook: CutoverHttpHook,
    dest_hook: StarshipLocalHook,
    pause_source: bool,
    unpause_dest: bool,
) -> dict:
    """Pause in source and/or unpause in destination. Returns what was done."""
    result = {"source_paused_by_us": False, "dest_unpaused_by_us": False}

    if pause_source:
        source_hook.set_dag_is_paused(dag_id=dag_id, is_paused=True)
        result["source_paused_by_us"] = True
        logger.info("Paused DAG %s in source.", dag_id)

    if unpause_dest:
        dest_hook.set_dag_is_paused(dag_id=dag_id, is_paused=False)
        result["dest_unpaused_by_us"] = True
        logger.info("Unpaused DAG %s in destination.", dag_id)

    return result


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


@provide_session
def delete_migrated_dag_data(
    dag_id: str,
    latest_data_interval_end: str,
    session: Session = NEW_SESSION,
) -> int:
    """Delete dag_runs, task_instances, and TI history migrated for a DAG."""
    if not latest_data_interval_end:
        return 0

    target_dt = datetime.fromisoformat(str(latest_data_interval_end))

    # Get run_ids to delete
    runs_to_delete = (
        session.query(DagRun.run_id)
        .filter(
            DagRun.dag_id == dag_id,
            DagRun.data_interval_end <= target_dt,
        )
        .all()
    )
    run_ids = [r[0] for r in runs_to_delete]

    if not run_ids:
        return 0

    chunk_size = 500

    # Delete task instances first (no cascade in Airflow)
    for i in range(0, len(run_ids), chunk_size):
        chunk = run_ids[i : i + chunk_size]
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id.in_(chunk),
        ).delete(synchronize_session=False)

    # Delete task instance history (AF 2.10+)
    try:
        from airflow.models import TaskInstanceHistory

        for i in range(0, len(run_ids), chunk_size):
            chunk = run_ids[i : i + chunk_size]
            session.query(TaskInstanceHistory).filter(
                TaskInstanceHistory.dag_id == dag_id,
                TaskInstanceHistory.run_id.in_(chunk),
            ).delete(synchronize_session=False)
    except ImportError:
        pass

    # Delete dag_runs
    deleted = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == dag_id,
            DagRun.data_interval_end <= target_dt,
        )
        .delete(synchronize_session=False)
    )

    session.commit()
    logger.info("Rolled back %d dag_runs for %s (+ TIs).", deleted, dag_id)
    return deleted


@provide_session
def purge_dag_metadata(
    dag_id: str,
    session: Session = NEW_SESSION,
) -> int:
    """Delete ALL dag_runs, task_instances, and TI history for a DAG.

    Unlike delete_migrated_dag_data, this has no date filter -- it removes
    everything. Intended as a force-cleanup escape hatch.
    """
    run_ids = [r[0] for r in session.query(DagRun.run_id).filter(DagRun.dag_id == dag_id).all()]

    if not run_ids:
        return 0

    chunk_size = 500

    for i in range(0, len(run_ids), chunk_size):
        chunk = run_ids[i : i + chunk_size]
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id.in_(chunk),
        ).delete(synchronize_session=False)

    try:
        from airflow.models import TaskInstanceHistory

        for i in range(0, len(run_ids), chunk_size):
            chunk = run_ids[i : i + chunk_size]
            session.query(TaskInstanceHistory).filter(
                TaskInstanceHistory.dag_id == dag_id,
                TaskInstanceHistory.run_id.in_(chunk),
            ).delete(synchronize_session=False)
    except ImportError:
        pass

    deleted = session.query(DagRun).filter(DagRun.dag_id == dag_id).delete(synchronize_session=False)

    session.commit()
    logger.info("Purged ALL metadata for %s: %d dag_runs (+ TIs).", dag_id, deleted)
    return deleted


@provide_session
def purge_all_instance_dag_metadata(session: Session = NEW_SESSION) -> dict:
    """Purge ALL dag_runs, task_instances, and TI history for every DAG in the instance.

    Returns a dict with 'purged' and 'errors' counts.
    """
    dag_ids = [r[0] for r in session.query(DagRun.dag_id).distinct().all()]
    purged = 0
    errors = 0
    for dag_id in dag_ids:
        try:
            purge_dag_metadata(dag_id)
            purged += 1
        except Exception:
            logger.exception("Purge failed for DAG %s.", dag_id)
            errors += 1
    return {"purged": purged, "errors": errors}


def rollback_dag(  # noqa: C901
    migration_id: str,
    dag_id: str,
    source_hook: Optional[CutoverHttpHook] = None,
    dest_hook: Optional[StarshipLocalHook] = None,
) -> None:
    """Roll back a single DAG within a migration.

    Deletes migrated data from destination and reverses pause/unpause.
    """
    migration = get_migration(migration_id)
    if not migration:
        raise ValueError(f"Migration '{migration_id}' not found.")

    dag_state = migration["dags"].get(dag_id)
    if not dag_state:
        raise ValueError(f"DAG '{dag_id}' not in migration '{migration_id}'.")

    if dag_state["status"] == "rolled_back":
        raise ValueError(f"DAG '{dag_id}' is already rolled back.")

    config = migration["config"]

    if source_hook is None:
        source_hook = get_source_hook(config["source_conn_id"])
    if dest_hook is None:
        dest_hook = StarshipLocalHook()

    # Delete migrated data from destination
    if dag_state["latest_data_interval_end"]:
        delete_migrated_dag_data(
            dag_id=dag_id,
            latest_data_interval_end=dag_state["latest_data_interval_end"],
        )

    # Reverse pause/unpause
    if dag_state.get("source_paused_by_us"):
        try:
            source_hook.set_dag_is_paused(dag_id=dag_id, is_paused=False)
            logger.info("Unpaused DAG %s in source (rollback).", dag_id)
        except Exception:
            logger.exception("Failed to unpause %s in source during rollback.", dag_id)

    if dag_state.get("dest_unpaused_by_us"):
        try:
            dest_hook.set_dag_is_paused(dag_id=dag_id, is_paused=True)
            logger.info("Re-paused DAG %s in destination (rollback).", dag_id)
        except Exception:
            logger.exception("Failed to re-pause %s in dest during rollback.", dag_id)

    update_dag_status(migration_id, dag_id, "rolled_back")


def rollback_migration(migration_id: str) -> None:
    """Roll back an entire migration -- all completed/failed DAGs."""
    migration = get_migration(migration_id)
    if not migration:
        raise ValueError(f"Migration '{migration_id}' not found.")

    config = migration["config"]
    source_hook = get_source_hook(config["source_conn_id"])
    dest_hook = StarshipLocalHook()

    rollback_failures = 0
    for dag_id, dag_state in migration["dags"].items():
        if dag_state["status"] in ("completed", "failed"):
            try:
                rollback_dag(
                    migration_id=migration_id,
                    dag_id=dag_id,
                    source_hook=source_hook,
                    dest_hook=dest_hook,
                )
            except Exception:
                logger.exception("Failed to rollback DAG %s.", dag_id)
                update_dag_status(
                    migration_id,
                    dag_id,
                    "failed",
                    error="Rollback failed: see logs",
                )
                rollback_failures += 1

    if rollback_failures:
        update_migration_status(migration_id, "failed")
        logger.warning(
            "Migration %s rollback partially failed: %d DAG(s) could not be rolled back.",
            migration_id,
            rollback_failures,
        )
    else:
        update_migration_status(migration_id, "rolled_back")


# ---------------------------------------------------------------------------
# Background thread orchestrator
# ---------------------------------------------------------------------------


def migrate_single_dag(
    migration_id: str,
    dag_id: str,
    config: dict,
) -> str:
    """Migrate a single DAG end-to-end. Designed to run inside a thread pool.

    Each call creates its own hooks so HTTP sessions and DB connections
    are not shared across threads.

    Returns the DAG's final status string ("completed", "failed", "deferred", "aborted").
    """
    # Check abort before starting
    if _is_aborted(migration_id):
        logger.info("[%s] Abort detected -- skipping.", dag_id)
        update_dag_status(migration_id, dag_id, "aborted")
        return "aborted"

    logger.info("[%s] Starting migration...", dag_id)
    source_hook = get_source_hook(config["source_conn_id"])
    dest_hook = StarshipLocalHook()

    dag_run_limit = config.get("dag_run_limit", 500)
    pause_source = config.get("pause_in_source", True)
    unpause_dest = config.get("unpause_in_destination", False)
    wait_for_scheduler = config.get("wait_for_scheduler", False)
    wait_for_running = config.get("wait_for_running", False)

    # Check if DAG has active runs in source
    if wait_for_running:
        logger.info("[%s] Checking for active runs in source...", dag_id)
        if _dag_has_active_runs(source_hook, dag_id):
            logger.info("[%s] Has active runs in source -- deferring.", dag_id)
            update_dag_status(
                migration_id,
                dag_id,
                "deferred",
                step="Deferred -- active runs",
                error="Active runs in source -- will retry",
            )
            return "deferred"

    # Step callback: uses in-memory tracking to avoid DB writes on every step
    def _on_step(label):
        set_step_label(migration_id, dag_id, label)

    update_dag_status(migration_id, dag_id, "running", step="Starting")

    try:
        result = migrate_dag(
            dag_id=dag_id,
            source_hook=source_hook,
            dest_hook=dest_hook,
            dag_run_limit=dag_run_limit,
            wait_for_scheduler=wait_for_scheduler,
            on_step=_on_step,
            migration_id=migration_id,
        )

        # Check abort after migration (before pause/unpause)
        if _is_aborted(migration_id):
            logger.info("[%s] Abort detected after data transfer -- skipping pause/unpause.", dag_id)
            update_dag_status(
                migration_id,
                dag_id,
                "completed",
                step="Done (abort before pause)",
                dag_runs_migrated=result["dag_runs_migrated"],
                task_instances_migrated=result["task_instances_migrated"],
                latest_data_interval_end=result["latest_data_interval_end"],
                error="Data migrated but abort requested before pause/unpause",
            )
            return "completed"

        # Pause/unpause
        set_step_label(migration_id, dag_id, "Pausing/Unpausing")
        logger.info("[%s] Handling pause/unpause...", dag_id)
        pause_result = pause_unpause_dag(
            dag_id=dag_id,
            source_hook=source_hook,
            dest_hook=dest_hook,
            pause_source=pause_source,
            unpause_dest=unpause_dest,
        )

        update_dag_status(
            migration_id,
            dag_id,
            "completed",
            step="Done",
            dag_runs_migrated=result["dag_runs_migrated"],
            task_instances_migrated=result["task_instances_migrated"],
            latest_data_interval_end=result["latest_data_interval_end"],
            source_paused_by_us=pause_result["source_paused_by_us"],
            dest_unpaused_by_us=pause_result["dest_unpaused_by_us"],
            error=None,
        )
        logger.info(
            "[%s] Migration complete: %d runs, %d TIs.",
            dag_id,
            result["dag_runs_migrated"],
            result["task_instances_migrated"],
        )
        return "completed"

    except _AbortedError:
        logger.info("[%s] Aborted before touching source.", dag_id)
        update_dag_status(
            migration_id,
            dag_id,
            "aborted",
            step="Aborted",
        )
        return "aborted"

    except Exception as e:
        logger.exception("[%s] Migration FAILED: %s", dag_id, e)
        update_dag_status(
            migration_id,
            dag_id,
            "failed",
            step="Failed",
            error=str(e)[:500],
        )
        return "failed"


def _run_dag_batch(
    migration_id: str,
    dag_ids: List[str],
    config: dict,
) -> None:
    """Run a batch of DAGs (sequential or parallel based on config)."""
    parallel_workers = config.get("parallel_workers", 1)
    logger.info(
        "[%s] Running batch of %d DAGs (%d workers).",
        migration_id,
        len(dag_ids),
        parallel_workers,
    )

    if parallel_workers <= 1:
        for dag_id in dag_ids:
            if _is_aborted(migration_id):
                # Mark remaining as aborted
                for remaining in dag_ids[dag_ids.index(dag_id) :]:
                    m = get_migration(migration_id)
                    if m and m["dags"].get(remaining, {}).get("status") == "pending":
                        update_dag_status(migration_id, remaining, "aborted")
                break
            migrate_single_dag(migration_id, dag_id, config)
    else:
        with ThreadPoolExecutor(
            max_workers=parallel_workers,
            thread_name_prefix=f"migration-{migration_id}",
        ) as executor:
            futures = {executor.submit(migrate_single_dag, migration_id, dag_id, config): dag_id for dag_id in dag_ids}
            for future in as_completed(futures):
                dag_id = futures[future]
                try:
                    future.result()
                except Exception:
                    logger.exception("Unexpected error in worker for DAG %s.", dag_id)
                # If aborted, cancel remaining futures
                if _is_aborted(migration_id):
                    for f in futures:
                        f.cancel()
                    break


def _get_dag_status_from_state(migration_id: str, dag_id: str) -> str:
    """Get a single DAG's status from the migration state."""
    m = get_migration(migration_id)
    if m and dag_id in m["dags"]:
        return m["dags"][dag_id]["status"]
    return "unknown"


def run_migration(migration_id: str, dag_ids: List[str], config: dict) -> None:  # noqa: C901
    """Run a full migration. Called from the background thread.

    Migrates DAGs in parallel using a thread pool. Each worker gets its own
    hooks (HTTP sessions / DB connections are not shared across threads).
    Supports abort, wait-for-running with retry, and optional scheduler sync.
    """
    try:
        # --- Main pass ---
        _run_dag_batch(migration_id, dag_ids, config)

        # --- Retry deferred DAGs (wait-for-running) ---
        if config.get("wait_for_running") and not _is_aborted(migration_id):
            retry_interval = config.get("retry_interval", 120)
            max_retries = config.get("max_retries", 3)

            for attempt in range(1, max_retries + 1):
                deferred = [d for d in dag_ids if _get_dag_status_from_state(migration_id, d) == "deferred"]
                if not deferred:
                    break

                logger.info(
                    "Retry round %d/%d: %d deferred DAGs, waiting %ds...",
                    attempt,
                    max_retries,
                    len(deferred),
                    retry_interval,
                )

                # Wait with abort check
                for _ in range(retry_interval // 5):
                    if _is_aborted(migration_id):
                        break
                    time.sleep(5)

                if _is_aborted(migration_id):
                    break

                # Reset deferred DAGs to pending for retry
                for dag_id in deferred:
                    update_dag_status(migration_id, dag_id, "pending")

                _run_dag_batch(migration_id, deferred, config)

            # Mark any remaining deferred DAGs as skipped
            for dag_id in dag_ids:
                if _get_dag_status_from_state(migration_id, dag_id) == "deferred":
                    update_dag_status(
                        migration_id,
                        dag_id,
                        "skipped",
                        error=f"Still running in source after {max_retries} retries",
                    )

        # --- Determine final status ---
        if _is_aborted(migration_id):
            # Mark any still-pending DAGs as aborted
            for dag_id in dag_ids:
                status = _get_dag_status_from_state(migration_id, dag_id)
                if status in ("pending", "deferred"):
                    update_dag_status(migration_id, dag_id, "aborted")
            update_migration_status(migration_id, "aborted")
        else:
            migration = get_migration(migration_id)
            dag_statuses = [migration["dags"][d]["status"] for d in dag_ids]
            if all(s == "failed" for s in dag_statuses):
                final_status = "failed"
            else:
                final_status = "completed"
            update_migration_status(migration_id, final_status)

        logger.info(
            "Migration %s finished with status: %s",
            migration_id,
            get_migration(migration_id)["status"],
        )
    finally:
        _cleanup_abort_event(migration_id)
        cleanup_step_labels(migration_id)


def start_migration_thread(
    migration_id: str,
    dag_ids: List[str],
    config: dict,
) -> threading.Thread:
    """Start a migration in a background daemon thread."""
    # Create abort event before starting thread
    _abort_events[migration_id] = threading.Event()

    t = threading.Thread(
        target=run_migration,
        args=(migration_id, dag_ids, config),
        daemon=True,
        name=f"migration-{migration_id}",
    )
    t.start()
    logger.info("Started migration thread for %s (%d DAGs).", migration_id, len(dag_ids))
    return t
