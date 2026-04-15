"""
Airflow plugin view for Starship Cutover.

Provides a multi-page admin UI for:
- Dashboard: migration history and action buttons
- Big-bang cutover: single-button migration of all source DAGs
- Incremental cutover: form with DAG patterns (fnmatch wildcards)
- Status: per-DAG progress with auto-refresh, rollback buttons

All views are restricted to the Admin role via Flask-AppBuilder RBAC.
"""

import fnmatch
import logging
import os
import threading

from flask import flash, redirect, request, url_for
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose, has_access

from astronomer_starship.cutover.auth import get_source_hook
from astronomer_starship.cutover.service import (
    _abort_events,
    _cleanup_abort_event,
    _is_aborted,
    _run_dag_batch,
    create_migration,
    delete_migrated_dag_data,
    get_migration,
    get_state,
    get_step_label,
    purge_all_instance_dag_metadata,
    purge_dag_metadata,
    request_abort,
    resolve_dag_patterns,
    rollback_dag,
    rollback_migration,
    start_migration_thread,
    update_dag_status,
    update_migration_status,
)
from astronomer_starship.providers.starship.hooks.starship import (
    StarshipLocalHook,
)

# Maximum migrations shown on the dashboard
_DASHBOARD_LIMIT = 25

logger = logging.getLogger(__name__)


def _safe_int(value, default: int) -> int:
    """Parse an int from form input, falling back to default on bad input."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_common_config(form) -> dict:
    """Extract common migration config from a form submission."""
    conn_id = form.get("source_conn_id", "starship_default").strip()
    return {
        "source_conn_id": conn_id,
        "dag_run_limit": _safe_int(form.get("dag_run_limit"), 500),
        "parallel_workers": min(_safe_int(form.get("parallel_workers"), 4), 16),
        "pause_in_source": form.get("pause_in_source") == "on",
        "unpause_in_destination": form.get("unpause_in_destination") == "on",
        "wait_for_scheduler": form.get("wait_for_scheduler") == "on",
        "wait_for_running": form.get("wait_for_running") == "on",
        "retry_interval": _safe_int(form.get("retry_interval"), 120),
        "max_retries": _safe_int(form.get("max_retries"), 3),
    }


class CutoverView(AppBuilderBaseView):
    """Admin-only views for migration cutover management."""

    route_base = "/cutover"
    default_view = "index"

    template_folder = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "cutover",
        "templates",
    )

    # -------------------------------------------------------------------
    # Dashboard
    # -------------------------------------------------------------------

    @expose("/")
    @has_access
    def index(self):
        state = get_state()
        migrations = list(reversed(state.get("migrations", [])))
        return self.render_template(
            "cutover/index.html",
            migrations=migrations[:_DASHBOARD_LIMIT],
            total_migrations=len(migrations),
            dashboard_limit=_DASHBOARD_LIMIT,
        )

    # -------------------------------------------------------------------
    # Big-bang
    # -------------------------------------------------------------------

    @expose("/bigbang", methods=["GET"])
    @has_access
    def bigbang_form(self):
        return self.render_template("cutover/bigbang.html")

    @expose("/bigbang", methods=["POST"])
    @has_access
    def bigbang_start(self):
        config = _parse_common_config(request.form)
        exclude_patterns_raw = request.form.get("exclude_patterns", "").strip()
        exclude_patterns = [p.strip() for p in exclude_patterns_raw.splitlines() if p.strip()]

        try:
            source_hook = get_source_hook(config["source_conn_id"])
            local_hook = StarshipLocalHook()

            dag_ids = resolve_dag_patterns(
                source_hook=source_hook,
                patterns=[],
                local_hook=local_hook,
            )

            if exclude_patterns:
                excluded = set()
                for pat in exclude_patterns:
                    for d in dag_ids:
                        if fnmatch.fnmatch(d, pat):
                            excluded.add(d)
                dag_ids = [d for d in dag_ids if d not in excluded]

            if not dag_ids:
                flash("No DAGs found to migrate. Check source connection and filters.", "warning")
                return redirect(url_for("CutoverView.bigbang_form"))

        except Exception as e:
            flash(f"Failed to resolve DAGs: {e}", "danger")
            logger.exception("Big-bang DAG resolution failed.")
            return redirect(url_for("CutoverView.bigbang_form"))

        migration_id = create_migration("bigbang", config, dag_ids)
        start_migration_thread(migration_id, dag_ids, config)

        flash(
            f"Big-bang migration started: {len(dag_ids)} DAGs. ID: {migration_id}",
            "success",
        )
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    # -------------------------------------------------------------------
    # Incremental
    # -------------------------------------------------------------------

    @expose("/incremental", methods=["GET"])
    @has_access
    def incremental_form(self):
        return self.render_template("cutover/incremental.html")

    @expose("/incremental", methods=["POST"])
    @has_access
    def incremental_start(self):
        config = _parse_common_config(request.form)
        patterns_raw = request.form.get("dag_patterns", "").strip()
        patterns = [p.strip() for p in patterns_raw.splitlines() if p.strip()]

        if not patterns:
            flash("Please enter at least one DAG pattern.", "warning")
            return redirect(url_for("CutoverView.incremental_form"))

        try:
            source_hook = get_source_hook(config["source_conn_id"])
            local_hook = StarshipLocalHook()
            dag_ids = resolve_dag_patterns(
                source_hook=source_hook,
                patterns=patterns,
                local_hook=local_hook,
            )

            if not dag_ids:
                flash(
                    "No DAGs matched the given patterns. "
                    "Verify patterns and check that DAGs exist in both source and destination.",
                    "warning",
                )
                return redirect(url_for("CutoverView.incremental_form"))

        except Exception as e:
            flash(f"Failed to resolve DAGs: {e}", "danger")
            logger.exception("Incremental DAG resolution failed.")
            return redirect(url_for("CutoverView.incremental_form"))

        migration_id = create_migration("incremental", config, dag_ids)
        start_migration_thread(migration_id, dag_ids, config)

        flash(
            f"Incremental migration started: {len(dag_ids)} DAGs. ID: {migration_id}",
            "success",
        )
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    # -------------------------------------------------------------------
    # Status
    # -------------------------------------------------------------------

    @expose("/status/<migration_id>")
    @has_access
    def status(self, migration_id):
        migration = get_migration(migration_id)
        if not migration:
            flash(f"Migration '{migration_id}' not found.", "danger")
            return redirect(url_for("CutoverView.index"))

        # Overlay in-memory step labels onto persisted state for running DAGs
        for dag_id, dag_state in migration["dags"].items():
            if dag_state["status"] == "running":
                mem_step = get_step_label(migration_id, dag_id)
                if mem_step:
                    dag_state["step"] = mem_step

        # Detect stale "running" state (no progress for >15 min)
        stale_warning = False
        if migration["status"] == "running":
            from datetime import datetime, timezone

            started = datetime.fromisoformat(migration["started_at"])
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            running_dags = [d for d in migration["dags"].values() if d["status"] == "running"]
            terminal_dags = [
                d for d in migration["dags"].values() if d["status"] in ("completed", "failed", "aborted", "skipped")
            ]
            if elapsed > 900 and not running_dags and terminal_dags:
                stale_warning = True

        # Summary counts
        dag_statuses = [d["status"] for d in migration["dags"].values()]
        summary = {
            "total": len(dag_statuses),
            "completed": dag_statuses.count("completed"),
            "failed": dag_statuses.count("failed"),
            "pending": dag_statuses.count("pending"),
            "running": dag_statuses.count("running"),
            "rolled_back": dag_statuses.count("rolled_back"),
            "deferred": dag_statuses.count("deferred"),
            "aborted": dag_statuses.count("aborted"),
            "skipped": dag_statuses.count("skipped"),
        }

        return self.render_template(
            "cutover/status.html",
            migration=migration,
            summary=summary,
            stale_warning=stale_warning,
        )

    # -------------------------------------------------------------------
    # Rollback
    # -------------------------------------------------------------------

    @expose("/rollback/<migration_id>", methods=["POST"])
    @has_access
    def rollback_all(self, migration_id):
        try:
            rollback_migration(migration_id)
            flash(f"Migration '{migration_id}' rolled back.", "success")
        except Exception as e:
            flash(f"Rollback failed: {e}", "danger")
            logger.exception("Rollback failed for migration %s.", migration_id)
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    @expose("/rollback/<migration_id>/<dag_id>", methods=["POST"])
    @has_access
    def rollback_single(self, migration_id, dag_id):
        try:
            rollback_dag(migration_id=migration_id, dag_id=dag_id)
            flash(f"DAG '{dag_id}' rolled back.", "success")
        except Exception as e:
            flash(f"Rollback failed for '{dag_id}': {e}", "danger")
            logger.exception("Rollback failed for DAG %s in %s.", dag_id, migration_id)
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    # -------------------------------------------------------------------
    # Purge
    # -------------------------------------------------------------------

    @expose("/purge/<migration_id>/<dag_id>", methods=["POST"])
    @has_access
    def purge_single(self, migration_id, dag_id):
        try:
            deleted = purge_dag_metadata(dag_id)
            update_dag_status(
                migration_id,
                dag_id,
                "rolled_back",
                step="Purged",
                dag_runs_migrated=0,
                task_instances_migrated=0,
                latest_data_interval_end=None,
                error=None,
            )
            flash(f"Purged all metadata for DAG '{dag_id}' ({deleted} runs deleted).", "success")
        except Exception as e:
            flash(f"Purge failed for '{dag_id}': {e}", "danger")
            logger.exception("Purge failed for DAG %s in %s.", dag_id, migration_id)
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    @expose("/purge/<migration_id>", methods=["POST"])
    @has_access
    def purge_all(self, migration_id):
        migration = get_migration(migration_id)
        if not migration:
            flash(f"Migration '{migration_id}' not found.", "danger")
            return redirect(url_for("CutoverView.index"))

        purged = 0
        errors = 0
        for dag_id, dag_state in migration["dags"].items():
            if dag_state["status"] in ("running", "rolled_back"):
                continue
            try:
                purge_dag_metadata(dag_id)
                update_dag_status(
                    migration_id,
                    dag_id,
                    "rolled_back",
                    step="Purged",
                    dag_runs_migrated=0,
                    task_instances_migrated=0,
                    latest_data_interval_end=None,
                    error=None,
                )
                purged += 1
            except Exception as e:
                logger.exception("Purge failed for DAG %s.", dag_id)
                update_dag_status(
                    migration_id,
                    dag_id,
                    "failed",
                    error=f"Purge failed: {e}"[:500],
                )
                errors += 1

        if errors:
            flash(f"Purged {purged} DAGs, {errors} failed. Check logs.", "warning")
        else:
            flash(f"Purged all metadata for {purged} DAGs.", "success")

        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    # -------------------------------------------------------------------
    # Purge all DAGs (dashboard-level)
    # -------------------------------------------------------------------

    @expose("/purge-all-dags", methods=["POST"])
    @has_access
    def purge_all_dags(self):
        try:
            result = purge_all_instance_dag_metadata()
        except Exception as e:
            flash(f"Purge failed: {e}", "danger")
            logger.exception("Global purge failed.")
            return redirect(url_for("CutoverView.index"))

        purged, errors = result["purged"], result["errors"]
        if errors:
            flash(f"Purged {purged} DAGs, {errors} failed. Check logs.", "warning")
        elif purged == 0:
            flash("No DAG metadata to purge.", "info")
        else:
            flash(f"Purged all metadata for {purged} DAGs.", "success")

        return redirect(url_for("CutoverView.index"))

    # -------------------------------------------------------------------
    # Abort
    # -------------------------------------------------------------------

    @expose("/abort/<migration_id>", methods=["POST"])
    @has_access
    def abort(self, migration_id):
        try:
            request_abort(migration_id)
            flash(
                f"Abort requested for migration '{migration_id}'. "
                f"Running DAGs will finish their current operation, then stop.",
                "warning",
            )
        except Exception as e:
            flash(f"Abort failed: {e}", "danger")
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    # -------------------------------------------------------------------
    # Mark stale migration as failed (recovery action)
    # -------------------------------------------------------------------

    @expose("/mark-failed/<migration_id>", methods=["POST"])
    @has_access
    def mark_failed(self, migration_id):
        try:
            update_migration_status(migration_id, "failed")
            flash(
                f"Migration '{migration_id}' marked as failed. You can now rollback or retry.",
                "info",
            )
        except Exception as e:
            flash(f"Failed to update status: {e}", "danger")
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    # -------------------------------------------------------------------
    # Retry
    # -------------------------------------------------------------------

    @expose("/retry/<migration_id>/<dag_id>", methods=["POST"])
    @has_access
    def retry_single(self, migration_id, dag_id):
        migration = get_migration(migration_id)
        if not migration:
            flash(f"Migration '{migration_id}' not found.", "danger")
            return redirect(url_for("CutoverView.index"))

        dag_state = migration["dags"].get(dag_id)
        if not dag_state or dag_state["status"] not in ("failed", "skipped"):
            flash(f"DAG '{dag_id}' is not in a failed/skipped state.", "warning")
            return redirect(url_for("CutoverView.status", migration_id=migration_id))

        _start_retry(migration_id, [dag_id], migration["config"])
        flash(f"Retrying DAG '{dag_id}'...", "info")
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    @expose("/retry-failed/<migration_id>", methods=["POST"])
    @has_access
    def retry_all_failed(self, migration_id):
        migration = get_migration(migration_id)
        if not migration:
            flash(f"Migration '{migration_id}' not found.", "danger")
            return redirect(url_for("CutoverView.index"))

        dag_ids = [d for d, s in migration["dags"].items() if s["status"] == "failed"]
        if not dag_ids:
            flash("No failed DAGs to retry.", "warning")
            return redirect(url_for("CutoverView.status", migration_id=migration_id))

        _start_retry(migration_id, dag_ids, migration["config"])
        flash(f"Retrying {len(dag_ids)} failed DAGs...", "info")
        return redirect(url_for("CutoverView.status", migration_id=migration_id))

    @expose("/retry-skipped/<migration_id>", methods=["POST"])
    @has_access
    def retry_all_skipped(self, migration_id):
        migration = get_migration(migration_id)
        if not migration:
            flash(f"Migration '{migration_id}' not found.", "danger")
            return redirect(url_for("CutoverView.index"))

        dag_ids = [d for d, s in migration["dags"].items() if s["status"] == "skipped"]
        if not dag_ids:
            flash("No skipped DAGs to retry.", "warning")
            return redirect(url_for("CutoverView.status", migration_id=migration_id))

        _start_retry(migration_id, dag_ids, migration["config"])
        flash(f"Retrying {len(dag_ids)} skipped DAGs...", "info")
        return redirect(url_for("CutoverView.status", migration_id=migration_id))


def _start_retry(migration_id: str, dag_ids: list, config: dict) -> None:
    """Rollback partial data, reset DAG states, and launch a background retry thread."""
    migration = get_migration(migration_id)
    if migration:
        for dag_id in dag_ids:
            dag_state = migration["dags"].get(dag_id, {})
            if dag_state.get("latest_data_interval_end"):
                try:
                    delete_migrated_dag_data(
                        dag_id=dag_id,
                        latest_data_interval_end=dag_state["latest_data_interval_end"],
                    )
                except Exception:
                    logger.exception("Failed to clean up partial data for %s before retry.", dag_id)

    for dag_id in dag_ids:
        update_dag_status(
            migration_id,
            dag_id,
            "pending",
            step=None,
            error=None,
            dag_runs_migrated=0,
            task_instances_migrated=0,
            latest_data_interval_end=None,
            source_paused_by_us=False,
            dest_unpaused_by_us=False,
        )

    update_migration_status(migration_id, "running")

    _abort_events[migration_id] = threading.Event()

    t = threading.Thread(
        target=_retry_batch_worker,
        args=(migration_id, dag_ids, config),
        daemon=True,
        name=f"retry-{migration_id}",
    )
    t.start()


def _retry_batch_worker(
    migration_id: str,
    dag_ids: list,
    config: dict,
) -> None:
    """Background worker for retrying a batch of DAGs."""
    try:
        _run_dag_batch(migration_id, dag_ids, config)

        # Mark any DAGs left in pending (e.g. cancelled futures during abort) as aborted
        if _is_aborted(migration_id):
            for dag_id in dag_ids:
                migration = get_migration(migration_id)
                if migration and migration["dags"].get(dag_id, {}).get("status") == "pending":
                    update_dag_status(migration_id, dag_id, "aborted")

        migration = get_migration(migration_id)
        if migration:
            all_terminal = all(
                d["status"] in ("completed", "failed", "skipped", "aborted", "rolled_back")
                for d in migration["dags"].values()
            )
            if all_terminal:
                # Check only the retried DAGs to determine migration-level outcome
                retried_statuses = [migration["dags"][d]["status"] for d in dag_ids if d in migration["dags"]]
                has_failures = any(s in ("failed", "aborted") for s in retried_statuses)
                if _is_aborted(migration_id):
                    update_migration_status(migration_id, "aborted")
                elif has_failures:
                    update_migration_status(migration_id, "failed")
                else:
                    update_migration_status(migration_id, "completed")
    finally:
        _cleanup_abort_event(migration_id)
