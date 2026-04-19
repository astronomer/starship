"""Cutover wave engine.

Orchestrates wave-based metadata migration from a source Airflow (remote)
into the local Airflow:

- State management via an Airflow Variable (survives restarts, shared across
  Gunicorn workers).
- DAG pattern resolution with ``fnmatch`` wildcards (cross-referenced
  against DAGs deployed locally).
- Per-DAG migration via :func:`migrate_dag_history` (unified operator engine).
- Rollback: delete migrated data from the local Airflow DB and reverse the
  pause/unpause that the wave applied.
- Abort: signal a running wave to stop; persisted in the Variable so it
  works across workers.
- Wait-for-running: defer DAGs with active runs in source, retry later.
- Background thread orchestrator with parallel execution.

The engine is consumed by two surfaces:

1. The REST API exposed to the React UI (``/api/starship/cutover/*``).
2. A template DAG (``providers/starship/example_dags/cutover_template.py``)
   for users who prefer to drive waves from Airflow itself.
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

from astronomer_starship.providers.starship.auth import resolve_source_hook
from astronomer_starship.providers.starship.hooks.starship import (
    StarshipHttpHook,
    StarshipLocalHook,
)
from astronomer_starship.providers.starship.operators.starship import (
    AbortedError,
    migrate_dag_history,
    pause_unpause_dag,
)

logger = logging.getLogger(__name__)


SYSTEM_DAGS_TO_EXCLUDE = {"airflow_monitoring"}
MIGRATION_DAG_PREFIXES = ("starship_airflow_migration_dag",)

VARIABLE_KEY = "starship_cutover_state"

# Non-terminal DAG run states used by the wait-for-running check.
RUNNING_STATES = {"running", "queued", "up_for_retry", "up_for_reschedule"}

_DEFAULT_SOURCE_CONN_ID = "starship_source"


# ---------------------------------------------------------------------------
# State management (Airflow Variable-backed)
# ---------------------------------------------------------------------------

_state_lock = threading.Lock()
_abort_events: Dict[str, threading.Event] = {}
_step_labels: Dict[str, Dict[str, str]] = {}
_step_labels_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_state() -> dict:
    """Read and parse the wave state from the Airflow Variable."""
    try:
        raw = Variable.get(VARIABLE_KEY, default_var=None)
        if raw is None:
            return {"migrations": []}
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupt cutover state variable -- resetting.")
        return {"migrations": []}


def save_state(state: dict) -> None:
    Variable.set(VARIABLE_KEY, json.dumps(state, default=str))


def create_migration(
    migration_type: str,
    config: dict,
    dag_ids: List[str],
) -> str:
    """Create a new wave entry and return its ID."""
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
                "target_unpaused_by_us": False,
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
    with _step_labels_lock:
        return _step_labels.get(migration_id, {}).get(dag_id)


def cleanup_step_labels(migration_id: str) -> None:
    with _step_labels_lock:
        _step_labels.pop(migration_id, None)


def update_dag_status(
    migration_id: str,
    dag_id: str,
    status: str,
    **details,
) -> None:
    """Update per-DAG state within a wave. For step-only changes use
    :func:`set_step_label` instead — this takes the Variable lock."""
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
# Abort
# ---------------------------------------------------------------------------

_abort_check_cache: Dict[str, float] = {}
_ABORT_CHECK_TTL = 5  # seconds between cross-worker DB reads


def request_abort(migration_id: str) -> None:
    """Signal a running wave to stop.

    Persists the flag in the Variable (works cross-worker). Also sets the
    in-memory event if this process is the one running the wave.
    """
    with _state_lock:
        state = get_state()
        for m in state["migrations"]:
            if m["id"] == migration_id:
                m["abort_requested"] = True
                break
        save_state(state)

    event = _abort_events.get(migration_id)
    if event:
        event.set()

    logger.info("Abort requested for wave %s.", migration_id)


def _is_aborted(migration_id: str) -> bool:
    """Fast in-memory check, falling back to rate-limited Variable read."""
    event = _abort_events.get(migration_id)
    if event is not None and event.is_set():
        return True

    now = time.monotonic()
    if now - _abort_check_cache.get(migration_id, 0) < _ABORT_CHECK_TTL:
        return False

    _abort_check_cache[migration_id] = now
    try:
        m = get_migration(migration_id)
    except Exception:
        logger.debug("Failed to read abort state for %s", migration_id, exc_info=True)
        return False
    if m and m.get("abort_requested"):
        if event is not None:
            event.set()
        return True
    return False


def _cleanup_abort_event(migration_id: str) -> None:
    _abort_events.pop(migration_id, None)
    _abort_check_cache.pop(migration_id, None)


# ---------------------------------------------------------------------------
# DAG resolution
# ---------------------------------------------------------------------------


def resolve_dag_patterns(
    source_hook: StarshipHttpHook,
    patterns: List[str],
    local_hook: Optional[StarshipLocalHook] = None,
) -> List[str]:
    """Resolve fnmatch patterns against source DAGs, cross-ref with local.

    ``patterns`` is a list of fnmatch strings (e.g. ``["etl_*", "reporting_daily"]``).
    Empty list means "all source DAGs" (big-bang mode). Returns a sorted list
    of dag_ids present in both source and local.
    """
    if local_hook is None:
        local_hook = StarshipLocalHook()

    source_dag_ids = [d["dag_id"] for d in source_hook.get_dags()]
    local_dag_ids = {d["dag_id"] for d in local_hook.get_dags()}

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

    filtered = [d for d in candidates if d not in SYSTEM_DAGS_TO_EXCLUDE and not d.startswith(MIGRATION_DAG_PREFIXES)]

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
# Wait-for-running / scheduler sync
# ---------------------------------------------------------------------------


def _dag_has_active_runs(source_hook: StarshipHttpHook, dag_id: str) -> bool:
    try:
        dag_runs = source_hook.get_dag_runs(dag_id=dag_id, limit=100)
        for dr in dag_runs.get("dag_runs", []):
            if dr.get("state") in RUNNING_STATES:
                return True
    except Exception:
        # Conservative: if we can't tell, assume active to avoid racing writes.
        logger.warning(
            "Could not check active runs for %s -- assuming active (conservative).",
            dag_id,
            exc_info=True,
        )
        return True
    return False


@provide_session
def _get_next_dagrun(dag_id: str, session: Session = NEW_SESSION):
    return session.scalar(select(DagModel.next_dagrun).where(DagModel.dag_id == dag_id))


@provide_session
def _get_schedule_interval(dag_id: str, session: Session = NEW_SESSION):
    return session.scalar(select(DagModel.schedule_interval).where(DagModel.dag_id == dag_id))


def _wait_for_scheduler_sync(dag_id: str, latest_data_interval_end: str, timeout: int = 60) -> None:
    """Wait until the scheduler has advanced next_dagrun past the migrated runs.

    Uses one short session per poll so we don't hold a DB connection open
    for the full poll loop.
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

    logger.warning("Scheduler sync timeout for %s after %ds -- continuing.", dag_id, timeout)


# ---------------------------------------------------------------------------
# Rollback / purge (destructive; local DB only)
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

    run_ids = [
        r[0]
        for r in session.query(DagRun.run_id)
        .filter(DagRun.dag_id == dag_id, DagRun.data_interval_end <= target_dt)
        .all()
    ]
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

    deleted = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id, DagRun.data_interval_end <= target_dt)
        .delete(synchronize_session=False)
    )
    session.commit()
    logger.info("Rolled back %d dag_runs for %s (+ TIs).", deleted, dag_id)
    return deleted


@provide_session
def purge_dag_metadata(dag_id: str, session: Session = NEW_SESSION) -> int:
    """Delete ALL dag_runs, task_instances, and TI history for a DAG.

    No date filter — force-cleanup escape hatch.
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
    """Purge ALL dag_runs, task_instances, and TI history for every DAG in the instance."""
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


def rollback_dag(
    migration_id: str,
    dag_id: str,
    source_hook: Optional[StarshipHttpHook] = None,
    target_hook: Optional[StarshipLocalHook] = None,
) -> None:
    """Roll back a single DAG within a wave: delete migrated data, reverse pause/unpause."""
    migration = get_migration(migration_id)
    if not migration:
        raise ValueError(f"Wave '{migration_id}' not found.")

    dag_state = migration["dags"].get(dag_id)
    if not dag_state:
        raise ValueError(f"DAG '{dag_id}' not in wave '{migration_id}'.")
    if dag_state["status"] == "rolled_back":
        raise ValueError(f"DAG '{dag_id}' is already rolled back.")

    config = migration["config"]
    if source_hook is None:
        source_hook = resolve_source_hook(config.get("source_conn_id", _DEFAULT_SOURCE_CONN_ID))
    if target_hook is None:
        target_hook = StarshipLocalHook()

    if dag_state["latest_data_interval_end"]:
        delete_migrated_dag_data(
            dag_id=dag_id,
            latest_data_interval_end=dag_state["latest_data_interval_end"],
        )

    if dag_state.get("source_paused_by_us"):
        try:
            source_hook.set_dag_is_paused(dag_id=dag_id, is_paused=False)
            logger.info("Unpaused DAG %s in source (rollback).", dag_id)
        except Exception:
            logger.exception("Failed to unpause %s in source during rollback.", dag_id)

    if dag_state.get("target_unpaused_by_us"):
        try:
            target_hook.set_dag_is_paused(dag_id=dag_id, is_paused=True)
            logger.info("Re-paused DAG %s in target (rollback).", dag_id)
        except Exception:
            logger.exception("Failed to re-pause %s in target during rollback.", dag_id)

    update_dag_status(migration_id, dag_id, "rolled_back")


def rollback_migration(migration_id: str) -> None:
    """Roll back a whole wave — all completed/failed DAGs."""
    migration = get_migration(migration_id)
    if not migration:
        raise ValueError(f"Wave '{migration_id}' not found.")

    config = migration["config"]
    source_hook = resolve_source_hook(config.get("source_conn_id", _DEFAULT_SOURCE_CONN_ID))
    target_hook = StarshipLocalHook()

    rollback_failures = 0
    for dag_id, dag_state in migration["dags"].items():
        if dag_state["status"] in ("completed", "failed"):
            try:
                rollback_dag(
                    migration_id=migration_id,
                    dag_id=dag_id,
                    source_hook=source_hook,
                    target_hook=target_hook,
                )
            except Exception:
                logger.exception("Failed to rollback DAG %s.", dag_id)
                update_dag_status(migration_id, dag_id, "failed", error="Rollback failed: see logs")
                rollback_failures += 1

    if rollback_failures:
        update_migration_status(migration_id, "failed")
        logger.warning(
            "Wave %s rollback partially failed: %d DAG(s) could not be rolled back.",
            migration_id,
            rollback_failures,
        )
    else:
        update_migration_status(migration_id, "rolled_back")


# ---------------------------------------------------------------------------
# Per-DAG worker and batch orchestrator
# ---------------------------------------------------------------------------


def migrate_single_dag(migration_id: str, dag_id: str, config: dict) -> str:  # noqa: C901
    """Migrate one DAG end-to-end inside a thread.

    Each call creates its own hooks so HTTP sessions and DB connections
    are not shared across threads.

    Returns the DAG's final status string: ``completed`` / ``failed`` /
    ``deferred`` / ``aborted``.
    """
    if _is_aborted(migration_id):
        logger.info("[%s] Abort detected -- skipping.", dag_id)
        update_dag_status(migration_id, dag_id, "aborted")
        return "aborted"

    source_conn_id = config.get("source_conn_id", _DEFAULT_SOURCE_CONN_ID)
    dag_run_limit = config.get("dag_run_limit", 500)
    pause_source = config.get("pause_in_source", True)
    unpause_target = config.get("unpause_in_target", config.get("unpause_in_destination", False))
    wait_for_scheduler = config.get("wait_for_scheduler", False)
    wait_for_running = config.get("wait_for_running", False)

    source_hook = resolve_source_hook(source_conn_id)
    target_hook = StarshipLocalHook()

    if wait_for_running and _dag_has_active_runs(source_hook, dag_id):
        logger.info("[%s] Has active runs in source -- deferring.", dag_id)
        update_dag_status(
            migration_id,
            dag_id,
            "deferred",
            step="Deferred -- active runs",
            error="Active runs in source -- will retry",
        )
        return "deferred"

    def _on_step(label: str) -> None:
        # In-memory only -- avoids a Variable write on every step change.
        set_step_label(migration_id, dag_id, label)

    def _check_abort() -> None:
        if _is_aborted(migration_id):
            raise AbortedError(f"Abort requested for {dag_id}")

    update_dag_status(migration_id, dag_id, "running", step="Starting")

    try:
        # Split the migration into data transfer + pause/unpause so we can
        # land data even if abort comes in before the pause step.
        result = migrate_dag_history(
            source_hook=source_hook,
            target_hook=target_hook,
            target_dag_id=dag_id,
            dag_run_limit=dag_run_limit,
            pause_dag_in_source=False,
            unpause_dag_in_target=False,
            pre_checks=True,
            migrate_ti_history=True,
            on_step=_on_step,
            check_abort=_check_abort,
        )

        if wait_for_scheduler:
            _on_step("Waiting for scheduler")
            _wait_for_scheduler_sync(dag_id, result.get("latest_data_interval_end"))

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

        set_step_label(migration_id, dag_id, "Pausing/Unpausing")
        pause_result = pause_unpause_dag(
            dag_id=dag_id,
            source_hook=source_hook,
            target_hook=target_hook,
            pause_in_source=pause_source,
            unpause_in_target=unpause_target,
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
            target_unpaused_by_us=pause_result["target_unpaused_by_us"],
            error=None,
        )
        logger.info(
            "[%s] Migration complete: %d runs, %d TIs.",
            dag_id,
            result["dag_runs_migrated"],
            result["task_instances_migrated"],
        )
        return "completed"

    except AbortedError:
        logger.info("[%s] Aborted before touching source.", dag_id)
        update_dag_status(migration_id, dag_id, "aborted", step="Aborted")
        return "aborted"
    except Exception as e:
        logger.exception("[%s] Migration FAILED: %s", dag_id, e)
        update_dag_status(migration_id, dag_id, "failed", step="Failed", error=str(e)[:500])
        return "failed"


def _run_dag_batch(migration_id: str, dag_ids: List[str], config: dict) -> None:
    parallel_workers = config.get("parallel_workers", 1)
    logger.info("[%s] Running batch of %d DAGs (%d workers).", migration_id, len(dag_ids), parallel_workers)

    if parallel_workers <= 1:
        for dag_id in dag_ids:
            if _is_aborted(migration_id):
                for remaining in dag_ids[dag_ids.index(dag_id) :]:
                    m = get_migration(migration_id)
                    if m and m["dags"].get(remaining, {}).get("status") == "pending":
                        update_dag_status(migration_id, remaining, "aborted")
                break
            migrate_single_dag(migration_id, dag_id, config)
        return

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
            if _is_aborted(migration_id):
                for f in futures:
                    f.cancel()
                break


def _dag_status(migration_id: str, dag_id: str) -> str:
    m = get_migration(migration_id)
    if m and dag_id in m["dags"]:
        return m["dags"][dag_id]["status"]
    return "unknown"


def run_migration(migration_id: str, dag_ids: List[str], config: dict) -> None:  # noqa: C901
    """Run a full wave: main pass, wait-for-running retry loop, final status."""
    try:
        _run_dag_batch(migration_id, dag_ids, config)

        if config.get("wait_for_running") and not _is_aborted(migration_id):
            retry_interval = config.get("retry_interval", 120)
            max_retries = config.get("max_retries", 3)

            for attempt in range(1, max_retries + 1):
                deferred = [d for d in dag_ids if _dag_status(migration_id, d) == "deferred"]
                if not deferred:
                    break

                logger.info(
                    "Retry round %d/%d: %d deferred DAGs, waiting %ds...",
                    attempt,
                    max_retries,
                    len(deferred),
                    retry_interval,
                )
                # Poll for abort while sleeping.
                for _ in range(retry_interval // 5):
                    if _is_aborted(migration_id):
                        break
                    time.sleep(5)
                if _is_aborted(migration_id):
                    break

                for dag_id in deferred:
                    update_dag_status(migration_id, dag_id, "pending")
                _run_dag_batch(migration_id, deferred, config)

            # Anything still deferred after max_retries becomes skipped.
            for dag_id in dag_ids:
                if _dag_status(migration_id, dag_id) == "deferred":
                    update_dag_status(
                        migration_id,
                        dag_id,
                        "skipped",
                        error=f"Still running in source after {max_retries} retries",
                    )

        if _is_aborted(migration_id):
            for dag_id in dag_ids:
                if _dag_status(migration_id, dag_id) in ("pending", "deferred"):
                    update_dag_status(migration_id, dag_id, "aborted")
            update_migration_status(migration_id, "aborted")
        else:
            migration = get_migration(migration_id)
            dag_statuses = [migration["dags"][d]["status"] for d in dag_ids]
            final_status = "failed" if all(s == "failed" for s in dag_statuses) else "completed"
            update_migration_status(migration_id, final_status)

        logger.info(
            "Wave %s finished with status: %s",
            migration_id,
            get_migration(migration_id)["status"],
        )
    finally:
        _cleanup_abort_event(migration_id)
        cleanup_step_labels(migration_id)


def start_migration_thread(migration_id: str, dag_ids: List[str], config: dict) -> threading.Thread:
    """Start a wave in a background daemon thread."""
    _abort_events[migration_id] = threading.Event()
    t = threading.Thread(
        target=run_migration,
        args=(migration_id, dag_ids, config),
        daemon=True,
        name=f"migration-{migration_id}",
    )
    t.start()
    logger.info("Started wave thread for %s (%d DAGs).", migration_id, len(dag_ids))
    return t
