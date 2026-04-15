"""Unit tests for the cutover feature.

Tests the service layer (state management, DAG pattern resolution),
auth factory (hook detection), and view routing. All Airflow/DB
dependencies are mocked so these run without a live environment.
"""

import threading
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Service: state management
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Reset module-level state between tests."""
    from astronomer_starship.cutover import service

    service._abort_events.clear()
    service._step_labels.clear()
    yield
    service._abort_events.clear()
    service._step_labels.clear()


@pytest.fixture()
def mock_variable():
    """Mock Airflow Variable.get / Variable.set as an in-memory dict."""
    store = {}
    with patch("astronomer_starship.cutover.service.Variable") as mock_var:
        mock_var.get = lambda key, default_var=None: store.get(key, default_var)
        mock_var.set = lambda key, value: store.__setitem__(key, value)
        yield store


def test_get_state_empty(mock_variable):
    from astronomer_starship.cutover.service import get_state

    state = get_state()
    assert state == {"migrations": []}


def test_get_state_corrupt(mock_variable):
    from astronomer_starship.cutover.service import get_state

    mock_variable["starship_cutover_state"] = "not json{"
    state = get_state()
    assert state == {"migrations": []}


def test_save_and_get_state(mock_variable):
    from astronomer_starship.cutover.service import get_state, save_state

    save_state({"migrations": [{"id": "test_123"}]})
    state = get_state()
    assert len(state["migrations"]) == 1
    assert state["migrations"][0]["id"] == "test_123"


def test_create_migration(mock_variable):
    from astronomer_starship.cutover.service import create_migration, get_migration

    mid = create_migration(
        migration_type="bigbang",
        config={"source_conn_id": "starship_default", "dag_run_limit": 500},
        dag_ids=["dag_a", "dag_b"],
    )

    assert mid.startswith("bigbang_")
    migration = get_migration(mid)
    assert migration is not None
    assert migration["status"] == "running"
    assert set(migration["dags"].keys()) == {"dag_a", "dag_b"}
    assert migration["dags"]["dag_a"]["status"] == "pending"


def test_update_dag_status(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        update_dag_status,
    )

    mid = create_migration("incremental", {"source_conn_id": "x"}, ["dag_1"])
    update_dag_status(mid, "dag_1", "completed", step="Done", dag_runs_migrated=42)

    migration = get_migration(mid)
    assert migration["dags"]["dag_1"]["status"] == "completed"
    assert migration["dags"]["dag_1"]["step"] == "Done"
    assert migration["dags"]["dag_1"]["dag_runs_migrated"] == 42


def test_update_migration_status(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        update_migration_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    update_migration_status(mid, "completed")

    migration = get_migration(mid)
    assert migration["status"] == "completed"
    assert migration["completed_at"] is not None


def test_get_migration_not_found(mock_variable):
    from astronomer_starship.cutover.service import get_migration

    assert get_migration("nonexistent_id") is None


# ---------------------------------------------------------------------------
# Service: step labels (in-memory)
# ---------------------------------------------------------------------------


def test_step_labels():
    from astronomer_starship.cutover.service import (
        cleanup_step_labels,
        get_step_label,
        set_step_label,
    )

    set_step_label("m1", "dag_a", "Fetching DAG runs")
    assert get_step_label("m1", "dag_a") == "Fetching DAG runs"
    assert get_step_label("m1", "dag_b") is None

    cleanup_step_labels("m1")
    assert get_step_label("m1", "dag_a") is None


# ---------------------------------------------------------------------------
# Service: abort mechanism
# ---------------------------------------------------------------------------


def test_abort_mechanism(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        _cleanup_abort_event,
        _is_aborted,
        create_migration,
        request_abort,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()

    assert not _is_aborted(mid)

    request_abort(mid)
    assert _is_aborted(mid)

    _cleanup_abort_event(mid)
    assert mid not in _abort_events


def test_abort_persisted_cross_worker(mock_variable):
    """Abort flag persisted in Variable works even without in-memory event."""
    from astronomer_starship.cutover.service import (
        _is_aborted,
        create_migration,
        get_migration,
        request_abort,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])

    # Simulate: web worker has no in-memory event (different process)
    # but request_abort persists to Variable
    request_abort(mid)

    # Verify persisted flag
    m = get_migration(mid)
    assert m["abort_requested"] is True

    # _is_aborted should detect it from the Variable even without in-memory event
    assert _is_aborted(mid)


def test_abort_nonexistent(mock_variable):
    from astronomer_starship.cutover.service import _is_aborted, request_abort

    # Should not raise (writes to Variable even if migration not found)
    request_abort("does_not_exist")
    assert not _is_aborted("does_not_exist")


# ---------------------------------------------------------------------------
# Service: DAG pattern resolution
# ---------------------------------------------------------------------------


def test_resolve_dag_patterns_all():
    """Empty patterns = big-bang mode = all DAGs in both source and local."""
    from astronomer_starship.cutover.service import resolve_dag_patterns

    source_hook = MagicMock()
    source_hook.get_dags.return_value = [
        {"dag_id": "etl_a"},
        {"dag_id": "etl_b"},
        {"dag_id": "reporting"},
    ]
    local_hook = MagicMock()
    local_hook.get_dags.return_value = [
        {"dag_id": "etl_a"},
        {"dag_id": "etl_b"},
    ]

    result = resolve_dag_patterns(source_hook, patterns=[], local_hook=local_hook)
    assert result == ["etl_a", "etl_b"]


def test_resolve_dag_patterns_with_wildcards():
    from astronomer_starship.cutover.service import resolve_dag_patterns

    source_hook = MagicMock()
    source_hook.get_dags.return_value = [
        {"dag_id": "etl_daily"},
        {"dag_id": "etl_weekly"},
        {"dag_id": "ml_train"},
        {"dag_id": "reporting"},
    ]
    local_hook = MagicMock()
    local_hook.get_dags.return_value = [
        {"dag_id": "etl_daily"},
        {"dag_id": "etl_weekly"},
        {"dag_id": "ml_train"},
        {"dag_id": "reporting"},
    ]

    result = resolve_dag_patterns(source_hook, patterns=["etl_*"], local_hook=local_hook)
    assert result == ["etl_daily", "etl_weekly"]


def test_resolve_dag_patterns_excludes_system_dags():
    from astronomer_starship.cutover.service import resolve_dag_patterns

    source_hook = MagicMock()
    source_hook.get_dags.return_value = [
        {"dag_id": "etl_a"},
        {"dag_id": "airflow_monitoring"},
        {"dag_id": "starship_airflow_migration_dag_v2"},
    ]
    local_hook = MagicMock()
    local_hook.get_dags.return_value = [
        {"dag_id": "etl_a"},
        {"dag_id": "airflow_monitoring"},
        {"dag_id": "starship_airflow_migration_dag_v2"},
    ]

    result = resolve_dag_patterns(source_hook, patterns=[], local_hook=local_hook)
    assert result == ["etl_a"]


def test_resolve_dag_patterns_cross_ref():
    """DAGs only in source (not deployed locally) are skipped."""
    from astronomer_starship.cutover.service import resolve_dag_patterns

    source_hook = MagicMock()
    source_hook.get_dags.return_value = [
        {"dag_id": "deployed"},
        {"dag_id": "not_deployed"},
    ]
    local_hook = MagicMock()
    local_hook.get_dags.return_value = [{"dag_id": "deployed"}]

    result = resolve_dag_patterns(source_hook, patterns=[], local_hook=local_hook)
    assert result == ["deployed"]


# ---------------------------------------------------------------------------
# Auth: get_source_hook factory
# ---------------------------------------------------------------------------


def test_get_source_hook_astro_bearer():
    """Connection with password -> AstroBearerAuth."""
    from astronomer_starship.cutover.auth import AstroBearerAuth, get_source_hook

    mock_conn = MagicMock()
    mock_conn.password = "test-token-value"  # pragma: allowlist secret
    mock_conn.extra_dejson = {}

    with patch("airflow.hooks.base.BaseHook") as mock_base:
        mock_base.get_connection.return_value = mock_conn
        hook = get_source_hook("test_conn")

    assert hook.auth_type is AstroBearerAuth


def test_get_source_hook_gcp_adc():
    """Connection without password and no impersonation -> ComposerV2BearerAuth."""
    from astronomer_starship.cutover.auth import ComposerV2BearerAuth, get_source_hook

    mock_conn = MagicMock()
    mock_conn.password = None
    mock_conn.extra_dejson = {}

    with patch("airflow.hooks.base.BaseHook") as mock_base:
        mock_base.get_connection.return_value = mock_conn
        hook = get_source_hook("test_conn")

    assert hook.auth_type is ComposerV2BearerAuth


def test_get_source_hook_impersonation():
    """Connection with impersonation_chain -> dynamically created auth class."""
    from astronomer_starship.cutover.auth import get_source_hook

    mock_conn = MagicMock()
    mock_conn.password = None
    mock_conn.extra_dejson = {
        "impersonation_chain": ["target-sa@project.iam.gserviceaccount.com"],
    }

    with patch("airflow.hooks.base.BaseHook") as mock_base:
        mock_base.get_connection.return_value = mock_conn
        hook = get_source_hook("test_conn")

    # Should not be the base classes — should be the dynamically created one
    assert hook.auth_type is not None
    assert hook.auth_type.__name__ == "ImpersonatedComposerAuth"


def test_cutover_http_hook_strips_headers():
    """CutoverHttpHook.get_conn() should strip non-standard headers."""
    from astronomer_starship.cutover.auth import CutoverHttpHook

    mock_session = MagicMock()
    mock_session.headers = {
        "Authorization": "Bearer xxx",
        "Content-Type": "application/json",
        "impersonation_chain": '["sa@project.iam.gserviceaccount.com"]',
        "extra_field": "should_be_removed",
    }

    with patch.object(
        CutoverHttpHook.__bases__[0],
        "get_conn",
        return_value=mock_session,
    ):
        hook = CutoverHttpHook.__new__(CutoverHttpHook)
        session = hook.get_conn()

    assert "Authorization" in session.headers
    assert "Content-Type" in session.headers
    assert "impersonation_chain" not in session.headers
    assert "extra_field" not in session.headers


# ---------------------------------------------------------------------------
# Service: purge_dag_metadata
# ---------------------------------------------------------------------------


def test_purge_dag_metadata_empty():
    """Purge on a DAG with no runs returns 0."""
    from astronomer_starship.cutover.service import purge_dag_metadata

    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.all.return_value = []

    with patch("astronomer_starship.cutover.service.NEW_SESSION", mock_session):
        result = purge_dag_metadata("empty_dag", session=mock_session)

    assert result == 0


# ---------------------------------------------------------------------------
# View helpers (_af2 — only available on Airflow 2.x)
# ---------------------------------------------------------------------------

try:
    from astronomer_starship._af2.cutover import _parse_common_config, _safe_int

    _has_af2 = True
except ImportError:
    _has_af2 = False

af2_only = pytest.mark.skipif(not _has_af2, reason="requires Airflow 2.x (_af2 module)")


@af2_only
def test_safe_int():
    assert _safe_int("42", 0) == 42
    assert _safe_int("bad", 10) == 10
    assert _safe_int(None, 5) == 5
    assert _safe_int("", 3) == 3


@af2_only
def test_parse_common_config():
    form = {
        "source_conn_id": "  my_conn  ",
        "dag_run_limit": "100",
        "parallel_workers": "8",
        "pause_in_source": "on",
        "unpause_in_destination": "on",
        "wait_for_scheduler": "on",
        "wait_for_running": "on",
        "retry_interval": "60",
        "max_retries": "5",
    }

    config = _parse_common_config(form)
    assert config["source_conn_id"] == "my_conn"
    assert config["dag_run_limit"] == 100
    assert config["parallel_workers"] == 8
    assert config["pause_in_source"] is True
    assert config["unpause_in_destination"] is True
    assert config["wait_for_scheduler"] is True
    assert config["wait_for_running"] is True
    assert config["retry_interval"] == 60
    assert config["max_retries"] == 5


@af2_only
def test_parse_common_config_defaults():
    config = _parse_common_config({})
    assert config["source_conn_id"] == "starship_default"
    assert config["dag_run_limit"] == 500
    assert config["parallel_workers"] == 4
    assert config["pause_in_source"] is False
    assert config["unpause_in_destination"] is False


# ---------------------------------------------------------------------------
# Service: migrate_dag (mocked hooks)
# ---------------------------------------------------------------------------


def test_migrate_dag_aborts_before_source(mock_variable):
    """migrate_dag raises _AbortedError when abort is set before source calls."""
    from astronomer_starship.cutover.service import (
        _abort_events,
        _AbortedError,
        create_migration,
        migrate_dag,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()
    _abort_events[mid].set()  # abort immediately

    source_hook = MagicMock()
    dest_hook = MagicMock()

    with pytest.raises(_AbortedError):
        migrate_dag("dag_1", source_hook, dest_hook, migration_id=mid)

    # Source should never have been called
    source_hook.get_dag.assert_not_called()
    source_hook.get_dag_runs.assert_not_called()


def test_migrate_dag_no_runs():
    """DAG with no runs in source returns zeros."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "empty_dag", "is_paused": True}
    source_hook.get_dag_runs.return_value = {"dag_runs": []}

    dest_hook = MagicMock()
    dest_hook.get_dag.return_value = {
        "dag_id": "empty_dag",
        "is_paused": True,
        "dag_run_count": 0,
    }

    result = migrate_dag("empty_dag", source_hook, dest_hook)
    assert result["dag_runs_migrated"] == 0
    assert result["task_instances_migrated"] == 0
    assert result["latest_data_interval_end"] is None


def test_migrate_dag_with_data():
    """DAG with runs migrates data to destination."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": False}
    source_hook.get_dag_runs.return_value = {
        "dag_runs": [
            {"dag_id": "dag_1", "run_id": "r1", "data_interval_end": "2024-01-01T00:00:00"},
            {"dag_id": "dag_1", "run_id": "r2", "data_interval_end": "2024-01-02T00:00:00"},
        ],
    }
    source_hook.get_task_instances.return_value = {
        "task_instances": [
            {"dag_id": "dag_1", "task_id": "t1", "run_id": "r1"},
            {"dag_id": "dag_1", "task_id": "t1", "run_id": "r2"},
            {"dag_id": "dag_1", "task_id": "t2", "run_id": "r1"},
        ],
    }
    source_hook.get_task_instance_history.side_effect = Exception("Not available")

    dest_hook = MagicMock()
    dest_hook.get_dag.return_value = {
        "dag_id": "dag_1",
        "is_paused": True,
        "dag_run_count": 0,
    }

    result = migrate_dag("dag_1", source_hook, dest_hook)
    assert result["dag_runs_migrated"] == 2
    assert result["task_instances_migrated"] == 3
    assert result["latest_data_interval_end"] == "2024-01-02T00:00:00"

    dest_hook.set_dag_runs.assert_called_once()
    dest_hook.set_task_instances.assert_called_once()


def test_migrate_dag_dest_not_paused():
    """Migration should fail if destination DAG is not paused."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True}

    dest_hook = MagicMock()
    dest_hook.get_dag.return_value = {
        "dag_id": "dag_1",
        "is_paused": False,
        "dag_run_count": 0,
    }

    with pytest.raises(RuntimeError, match="active in destination"):
        migrate_dag("dag_1", source_hook, dest_hook)


def test_migrate_dag_dest_has_runs():
    """Migration should fail if destination DAG already has runs."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True}

    dest_hook = MagicMock()
    dest_hook.get_dag.return_value = {
        "dag_id": "dag_1",
        "is_paused": True,
        "dag_run_count": 10,
    }

    with pytest.raises(RuntimeError, match="already has runs"):
        migrate_dag("dag_1", source_hook, dest_hook)


# ---------------------------------------------------------------------------
# Service: _dag_has_active_runs
# ---------------------------------------------------------------------------


def test_dag_has_active_runs_true():
    from astronomer_starship.cutover.service import _dag_has_active_runs

    source_hook = MagicMock()
    source_hook.get_dag_runs.return_value = {
        "dag_runs": [
            {"state": "success"},
            {"state": "running"},
        ],
    }
    assert _dag_has_active_runs(source_hook, "dag_1") is True


def test_dag_has_active_runs_false():
    from astronomer_starship.cutover.service import _dag_has_active_runs

    source_hook = MagicMock()
    source_hook.get_dag_runs.return_value = {
        "dag_runs": [
            {"state": "success"},
            {"state": "failed"},
        ],
    }
    assert _dag_has_active_runs(source_hook, "dag_1") is False


def test_dag_has_active_runs_empty():
    from astronomer_starship.cutover.service import _dag_has_active_runs

    source_hook = MagicMock()
    source_hook.get_dag_runs.return_value = {"dag_runs": []}
    assert _dag_has_active_runs(source_hook, "dag_1") is False


def test_dag_has_active_runs_exception():
    """On error, assume active (conservative)."""
    from astronomer_starship.cutover.service import _dag_has_active_runs

    source_hook = MagicMock()
    source_hook.get_dag_runs.side_effect = Exception("connection error")
    assert _dag_has_active_runs(source_hook, "dag_1") is True


def test_dag_has_active_runs_queued():
    from astronomer_starship.cutover.service import _dag_has_active_runs

    source_hook = MagicMock()
    source_hook.get_dag_runs.return_value = {
        "dag_runs": [{"state": "queued"}],
    }
    assert _dag_has_active_runs(source_hook, "dag_1") is True


# ---------------------------------------------------------------------------
# Service: pause_unpause_dag
# ---------------------------------------------------------------------------


def test_pause_unpause_both():
    from astronomer_starship.cutover.service import pause_unpause_dag

    source_hook = MagicMock()
    dest_hook = MagicMock()

    result = pause_unpause_dag("dag_1", source_hook, dest_hook, pause_source=True, unpause_dest=True)
    assert result["source_paused_by_us"] is True
    assert result["dest_unpaused_by_us"] is True
    source_hook.set_dag_is_paused.assert_called_once_with(dag_id="dag_1", is_paused=True)
    dest_hook.set_dag_is_paused.assert_called_once_with(dag_id="dag_1", is_paused=False)


def test_pause_unpause_neither():
    from astronomer_starship.cutover.service import pause_unpause_dag

    source_hook = MagicMock()
    dest_hook = MagicMock()

    result = pause_unpause_dag("dag_1", source_hook, dest_hook, pause_source=False, unpause_dest=False)
    assert result["source_paused_by_us"] is False
    assert result["dest_unpaused_by_us"] is False
    source_hook.set_dag_is_paused.assert_not_called()
    dest_hook.set_dag_is_paused.assert_not_called()


def test_pause_only_source():
    from astronomer_starship.cutover.service import pause_unpause_dag

    source_hook = MagicMock()
    dest_hook = MagicMock()

    result = pause_unpause_dag("dag_1", source_hook, dest_hook, pause_source=True, unpause_dest=False)
    assert result["source_paused_by_us"] is True
    assert result["dest_unpaused_by_us"] is False


# ---------------------------------------------------------------------------
# Service: _get_dag_status_from_state
# ---------------------------------------------------------------------------


def test_get_dag_status_from_state(mock_variable):
    from astronomer_starship.cutover.service import (
        _get_dag_status_from_state,
        create_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    assert _get_dag_status_from_state(mid, "dag_1") == "pending"
    assert _get_dag_status_from_state(mid, "nonexistent") == "unknown"
    assert _get_dag_status_from_state("bad_id", "dag_1") == "unknown"


# ---------------------------------------------------------------------------
# Service: migrate_single_dag
# ---------------------------------------------------------------------------


def test_migrate_single_dag_completed(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        migrate_single_dag,
    )

    mid = create_migration(
        "bigbang",
        {
            "source_conn_id": "x",
            "dag_run_limit": 10,
            "pause_in_source": False,
            "unpause_in_destination": False,
            "wait_for_scheduler": False,
            "wait_for_running": False,
        },
        ["dag_1"],
    )

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get_hook, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ) as mock_local:
        source = MagicMock()
        source.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True}
        source.get_dag_runs.return_value = {"dag_runs": []}
        mock_get_hook.return_value = source

        dest = MagicMock()
        dest.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True, "dag_run_count": 0}
        mock_local.return_value = dest

        status = migrate_single_dag(
            mid,
            "dag_1",
            {
                "source_conn_id": "x",
                "dag_run_limit": 10,
                "pause_in_source": False,
                "unpause_in_destination": False,
                "wait_for_scheduler": False,
                "wait_for_running": False,
            },
        )

    assert status == "completed"
    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "completed"


def test_migrate_single_dag_aborted_before_start(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        migrate_single_dag,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()
    _abort_events[mid].set()

    status = migrate_single_dag(mid, "dag_1", {"source_conn_id": "x"})
    assert status == "aborted"
    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "aborted"


def test_migrate_single_dag_deferred(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        migrate_single_dag,
    )

    mid = create_migration(
        "bigbang",
        {
            "source_conn_id": "x",
            "wait_for_running": True,
        },
        ["dag_1"],
    )

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get_hook, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ), patch("astronomer_starship.cutover.service._dag_has_active_runs", return_value=True):
        mock_get_hook.return_value = MagicMock()

        status = migrate_single_dag(
            mid,
            "dag_1",
            {
                "source_conn_id": "x",
                "wait_for_running": True,
            },
        )

    assert status == "deferred"
    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "deferred"


def test_migrate_single_dag_failed(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        migrate_single_dag,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get_hook, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ) as mock_local:
        source = MagicMock()
        source.get_dag.side_effect = Exception("Source unreachable")
        mock_get_hook.return_value = source
        mock_local.return_value = MagicMock()

        status = migrate_single_dag(mid, "dag_1", {"source_conn_id": "x"})

    assert status == "failed"
    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "failed"
    assert "not found or inaccessible" in m["dags"]["dag_1"]["error"]


# ---------------------------------------------------------------------------
# Service: _run_dag_batch (sequential)
# ---------------------------------------------------------------------------


def test_run_dag_batch_sequential(mock_variable):
    from astronomer_starship.cutover.service import (
        _run_dag_batch,
        create_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1", "dag_2"])

    with patch("astronomer_starship.cutover.service.migrate_single_dag") as mock_migrate:
        mock_migrate.return_value = "completed"
        _run_dag_batch(mid, ["dag_1", "dag_2"], {"parallel_workers": 1, "source_conn_id": "x"})

    assert mock_migrate.call_count == 2


def test_run_dag_batch_sequential_abort(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        _run_dag_batch,
        create_migration,
        get_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1", "dag_2", "dag_3"])
    _abort_events[mid] = threading.Event()

    def _side_effect(migration_id, dag_id, config):
        # Abort after first DAG
        _abort_events[mid].set()
        return "completed"

    with patch("astronomer_starship.cutover.service.migrate_single_dag", side_effect=_side_effect):
        _run_dag_batch(mid, ["dag_1", "dag_2", "dag_3"], {"parallel_workers": 1, "source_conn_id": "x"})

    # dag_2 and dag_3 should be aborted (never migrated)
    m = get_migration(mid)
    assert m["dags"]["dag_2"]["status"] == "aborted"
    assert m["dags"]["dag_3"]["status"] == "aborted"


def test_run_dag_batch_parallel(mock_variable):
    from astronomer_starship.cutover.service import (
        _run_dag_batch,
        create_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1", "dag_2"])

    with patch("astronomer_starship.cutover.service.migrate_single_dag") as mock_migrate:
        mock_migrate.return_value = "completed"
        _run_dag_batch(mid, ["dag_1", "dag_2"], {"parallel_workers": 2, "source_conn_id": "x"})

    assert mock_migrate.call_count == 2


# ---------------------------------------------------------------------------
# Service: run_migration
# ---------------------------------------------------------------------------


def test_run_migration_completed(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        run_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()

    with patch("astronomer_starship.cutover.service.migrate_single_dag") as mock_migrate:
        mock_migrate.return_value = "completed"

        # Simulate migrate_single_dag updating the state
        def _side_effect(migration_id, dag_id, config):
            from astronomer_starship.cutover.service import update_dag_status

            update_dag_status(migration_id, dag_id, "completed", dag_runs_migrated=5)
            return "completed"

        mock_migrate.side_effect = _side_effect
        run_migration(mid, ["dag_1"], {"source_conn_id": "x", "parallel_workers": 1})

    m = get_migration(mid)
    assert m["status"] == "completed"


def test_run_migration_all_failed(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        run_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()

    def _side_effect(migration_id, dag_id, config):
        from astronomer_starship.cutover.service import update_dag_status

        update_dag_status(migration_id, dag_id, "failed", error="boom")
        return "failed"

    with patch("astronomer_starship.cutover.service.migrate_single_dag", side_effect=_side_effect):
        run_migration(mid, ["dag_1"], {"source_conn_id": "x", "parallel_workers": 1})

    m = get_migration(mid)
    assert m["status"] == "failed"


def test_run_migration_aborted(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        run_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1", "dag_2"])
    _abort_events[mid] = threading.Event()

    def _side_effect(migration_id, dag_id, config):
        from astronomer_starship.cutover.service import update_dag_status

        _abort_events[mid].set()
        update_dag_status(migration_id, dag_id, "completed")
        return "completed"

    with patch("astronomer_starship.cutover.service.migrate_single_dag", side_effect=_side_effect):
        run_migration(mid, ["dag_1", "dag_2"], {"source_conn_id": "x", "parallel_workers": 1})

    m = get_migration(mid)
    assert m["status"] == "aborted"


def test_run_migration_with_retry_deferred(mock_variable):
    """Deferred DAGs are retried and eventually skipped."""
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        run_migration,
    )

    mid = create_migration(
        "bigbang",
        {
            "source_conn_id": "x",
            "wait_for_running": True,
            "retry_interval": 0,  # no wait in tests
            "max_retries": 1,
        },
        ["dag_1"],
    )
    _abort_events[mid] = threading.Event()

    def _side_effect(migration_id, dag_id, config):
        from astronomer_starship.cutover.service import update_dag_status

        update_dag_status(migration_id, dag_id, "deferred", error="Active runs")
        return "deferred"

    with patch("astronomer_starship.cutover.service.migrate_single_dag", side_effect=_side_effect), patch(
        "astronomer_starship.cutover.service.time.sleep"
    ):
        run_migration(
            mid,
            ["dag_1"],
            {
                "source_conn_id": "x",
                "parallel_workers": 1,
                "wait_for_running": True,
                "retry_interval": 0,
                "max_retries": 1,
            },
        )

    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "skipped"


# ---------------------------------------------------------------------------
# Service: start_migration_thread
# ---------------------------------------------------------------------------


def test_start_migration_thread(mock_variable):
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        start_migration_thread,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])

    with patch("astronomer_starship.cutover.service.run_migration"):
        t = start_migration_thread(mid, ["dag_1"], {"source_conn_id": "x"})

    assert t.is_alive() or t.daemon
    assert mid in _abort_events
    t.join(timeout=2)


# ---------------------------------------------------------------------------
# Service: rollback_dag
# ---------------------------------------------------------------------------


def test_rollback_dag_basic(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        rollback_dag,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    update_dag_status(mid, "dag_1", "completed", latest_data_interval_end="2024-01-01T00:00:00")

    source_hook = MagicMock()
    dest_hook = MagicMock()

    with patch("astronomer_starship.cutover.service.delete_migrated_dag_data") as mock_delete:
        rollback_dag(mid, "dag_1", source_hook=source_hook, dest_hook=dest_hook)

    mock_delete.assert_called_once()
    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "rolled_back"


def test_rollback_dag_not_found(mock_variable):
    from astronomer_starship.cutover.service import rollback_dag

    with pytest.raises(ValueError, match="not found"):
        rollback_dag("nonexistent", "dag_1")


def test_rollback_dag_already_rolled_back(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        rollback_dag,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    update_dag_status(mid, "dag_1", "rolled_back")

    with pytest.raises(ValueError, match="already rolled back"):
        rollback_dag(mid, "dag_1", source_hook=MagicMock(), dest_hook=MagicMock())


def test_rollback_dag_reverses_pause(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        rollback_dag,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    update_dag_status(
        mid, "dag_1", "completed", source_paused_by_us=True, dest_unpaused_by_us=True, latest_data_interval_end=None
    )

    source_hook = MagicMock()
    dest_hook = MagicMock()

    rollback_dag(mid, "dag_1", source_hook=source_hook, dest_hook=dest_hook)

    source_hook.set_dag_is_paused.assert_called_once_with(dag_id="dag_1", is_paused=False)
    dest_hook.set_dag_is_paused.assert_called_once_with(dag_id="dag_1", is_paused=True)


def test_rollback_dag_missing_dag_id(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        rollback_dag,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])

    with pytest.raises(ValueError, match="not in migration"):
        rollback_dag(mid, "dag_nonexistent", source_hook=MagicMock(), dest_hook=MagicMock())


# ---------------------------------------------------------------------------
# Service: rollback_migration
# ---------------------------------------------------------------------------


def test_rollback_migration_success(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        rollback_migration,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1", "dag_2"])
    update_dag_status(mid, "dag_1", "completed", latest_data_interval_end=None)
    update_dag_status(mid, "dag_2", "failed", latest_data_interval_end=None)

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ):
        mock_get.return_value = MagicMock()
        rollback_migration(mid)

    m = get_migration(mid)
    assert m["status"] == "rolled_back"


def test_rollback_migration_not_found(mock_variable):
    from astronomer_starship.cutover.service import rollback_migration

    with pytest.raises(ValueError, match="not found"):
        rollback_migration("nonexistent")


def test_rollback_migration_partial_failure(mock_variable):
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        rollback_migration,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1", "dag_2"])
    update_dag_status(mid, "dag_1", "completed", latest_data_interval_end="2024-01-01")
    update_dag_status(mid, "dag_2", "completed", latest_data_interval_end="2024-01-01")

    call_count = 0

    def _rollback_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("DB error")
        # Second call succeeds - simulate actual rollback behavior
        from astronomer_starship.cutover.service import update_dag_status as _uds

        _uds(mid, "dag_2", "rolled_back")

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ), patch("astronomer_starship.cutover.service.rollback_dag", side_effect=_rollback_side_effect):
        mock_get.return_value = MagicMock()
        rollback_migration(mid)

    m = get_migration(mid)
    assert m["status"] == "failed"


# ---------------------------------------------------------------------------
# Auth: AstroBearerAuth.__call__
# ---------------------------------------------------------------------------


def test_astro_bearer_auth_call():
    from astronomer_starship.cutover.auth import AstroBearerAuth

    auth = AstroBearerAuth(password="my-token")  # pragma: allowlist secret
    req = MagicMock()
    req.headers = {}

    result = auth(req)
    assert result.headers["Authorization"] == "Bearer my-token"


# ---------------------------------------------------------------------------
# Auth: ComposerV2BearerAuth, _get_default_credentials, impersonation
# These tests require google-auth; skip if not installed.
# ---------------------------------------------------------------------------

try:
    import google.auth  # noqa: F401

    _has_google_auth = True
except ImportError:
    _has_google_auth = False

gcp_only = pytest.mark.skipif(not _has_google_auth, reason="requires google-auth")


@gcp_only
def test_composer_v2_bearer_auth_call():
    from astronomer_starship.cutover.auth import ComposerV2BearerAuth

    mock_creds = MagicMock()
    mock_creds.valid = True
    mock_creds.token = b"gcp-token"

    with patch("astronomer_starship.cutover.auth._get_default_credentials", return_value=mock_creds):
        auth = ComposerV2BearerAuth()

    req = MagicMock()
    req.headers = {}

    with patch("google.auth._helpers.from_bytes", return_value="gcp-token"):
        result = auth(req)

    assert result.headers["Authorization"] == "Bearer gcp-token"


@gcp_only
def test_composer_v2_bearer_auth_refreshes():
    from astronomer_starship.cutover.auth import ComposerV2BearerAuth

    mock_creds = MagicMock()
    mock_creds.valid = False
    mock_creds.token = b"refreshed-token"

    with patch("astronomer_starship.cutover.auth._get_default_credentials", return_value=mock_creds):
        auth = ComposerV2BearerAuth()

    req = MagicMock()
    req.headers = {}

    with patch("google.auth._helpers.from_bytes", return_value="refreshed-token"), patch(
        "google.auth.transport.requests.Request"
    ):
        result = auth(req)

    mock_creds.refresh.assert_called_once()
    assert result.headers["Authorization"] == "Bearer refreshed-token"


@gcp_only
def test_get_default_credentials_cached():
    """Second call returns cached credentials."""
    import astronomer_starship.cutover.auth as auth_module

    original = auth_module._default_creds
    try:
        auth_module._default_creds = None

        mock_creds = MagicMock()
        with patch("google.auth.default", return_value=(mock_creds, "project")):
            result1 = auth_module._get_default_credentials()
            result2 = auth_module._get_default_credentials()

        assert result1 is result2
        assert result1 is mock_creds
    finally:
        auth_module._default_creds = original


@gcp_only
def test_impersonated_auth_call():
    from astronomer_starship.cutover.auth import _make_impersonated_auth

    auth_class = _make_impersonated_auth(["target-sa@project.iam.gserviceaccount.com"])
    assert auth_class.__name__ == "ImpersonatedComposerAuth"

    mock_creds = MagicMock()
    mock_creds.valid = True
    mock_creds.token = b"impersonated-token"

    with patch("astronomer_starship.cutover.auth._get_default_credentials") as mock_default, patch(
        "google.auth.impersonated_credentials.Credentials", return_value=mock_creds
    ):
        mock_default.return_value = MagicMock()
        auth = auth_class()

    req = MagicMock()
    req.headers = {}

    with patch("google.auth._helpers.from_bytes", return_value="impersonated-token"):
        result = auth(req)

    assert result.headers["Authorization"] == "Bearer impersonated-token"


@gcp_only
def test_impersonated_auth_with_delegates():
    from astronomer_starship.cutover.auth import _make_impersonated_auth

    chain = ["delegate@project.iam.gserviceaccount.com", "target@project.iam.gserviceaccount.com"]
    auth_class = _make_impersonated_auth(chain)

    with patch("astronomer_starship.cutover.auth._get_default_credentials") as mock_default, patch(
        "google.auth.impersonated_credentials.Credentials"
    ) as mock_imp_creds:
        mock_default.return_value = MagicMock()
        mock_imp_creds.return_value = MagicMock(valid=True, token=b"t")
        auth_class()

    mock_imp_creds.assert_called_once()
    call_kwargs = mock_imp_creds.call_args
    assert call_kwargs.kwargs["target_principal"] == "target@project.iam.gserviceaccount.com"
    assert call_kwargs.kwargs["delegates"] == ["delegate@project.iam.gserviceaccount.com"]


# ---------------------------------------------------------------------------
# Hooks: StarshipHttpHook new methods
# ---------------------------------------------------------------------------


def test_http_hook_get_dag_caches():
    """get_dag uses a cache populated from get_dags."""
    from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

    hook = StarshipHttpHook.__new__(StarshipHttpHook)
    hook.get_dags = MagicMock(
        return_value=[
            {"dag_id": "dag_a", "is_paused": True},
            {"dag_id": "dag_b", "is_paused": False},
        ]
    )

    result = hook.get_dag("dag_a")
    assert result["dag_id"] == "dag_a"

    # Second call uses cache
    result2 = hook.get_dag("dag_b")
    assert result2["dag_id"] == "dag_b"
    hook.get_dags.assert_called_once()


def test_http_hook_get_dag_not_found():
    from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

    hook = StarshipHttpHook.__new__(StarshipHttpHook)
    hook.get_dags = MagicMock(return_value=[{"dag_id": "dag_a"}])

    with pytest.raises(ValueError, match="not found"):
        hook.get_dag("nonexistent")


def test_http_hook_get_task_instance_history():
    from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

    hook = StarshipHttpHook.__new__(StarshipHttpHook)
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"task_instances": []}
    mock_session.get.return_value = mock_response
    hook.get_conn = MagicMock(return_value=mock_session)
    hook.url_from_endpoint = MagicMock(return_value="http://test/api/starship/task_instance_history")

    result = hook.get_task_instance_history("dag_1", limit=100)
    assert result == {"task_instances": []}
    mock_response.raise_for_status.assert_called_once()


def test_http_hook_set_task_instance_history():
    from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

    hook = StarshipHttpHook.__new__(StarshipHttpHook)
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "ok"}
    mock_session.post.return_value = mock_response
    hook.get_conn = MagicMock(return_value=mock_session)
    hook.url_from_endpoint = MagicMock(return_value="http://test/api/starship/task_instance_history")

    result = hook.set_task_instance_history([{"dag_id": "dag_1", "task_id": "t1"}])
    assert result == {"status": "ok"}
    mock_response.raise_for_status.assert_called_once()


# ---------------------------------------------------------------------------
# Service: migrate_dag with TI history
# ---------------------------------------------------------------------------


def test_migrate_dag_with_ti_history():
    """TI history is migrated when available."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True}
    source_hook.get_dag_runs.return_value = {
        "dag_runs": [
            {"dag_id": "dag_1", "run_id": "r1", "data_interval_end": "2024-01-01T00:00:00"},
        ],
    }
    source_hook.get_task_instances.return_value = {
        "task_instances": [{"dag_id": "dag_1", "task_id": "t1", "run_id": "r1"}],
    }
    source_hook.get_task_instance_history.return_value = {
        "task_instances": [
            {"dag_id": "dag_1", "task_id": "t1", "run_id": "r1", "try_number": 1},
            {"dag_id": "dag_1", "task_id": "t1", "run_id": "r1", "try_number": 2},
        ],
    }

    dest_hook = MagicMock()
    dest_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True, "dag_run_count": 0}

    result = migrate_dag("dag_1", source_hook, dest_hook)
    assert result["dag_runs_migrated"] == 1
    assert result["task_instance_history_migrated"] == 2
    dest_hook.set_task_instance_history.assert_called_once()


def test_migrate_dag_source_not_found():
    """Migration fails if DAG not accessible in source."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.side_effect = Exception("404 Not Found")
    dest_hook = MagicMock()

    with pytest.raises(RuntimeError, match="not found or inaccessible"):
        migrate_dag("missing_dag", source_hook, dest_hook)


def test_migrate_dag_dest_not_found():
    """Migration fails if DAG not deployed in destination."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True}
    dest_hook = MagicMock()

    # Import NoResultFound from sqlalchemy and use it
    from sqlalchemy.exc import NoResultFound

    dest_hook.get_dag.side_effect = NoResultFound("No row found")

    with pytest.raises(RuntimeError, match="not found in destination"):
        migrate_dag("dag_1", source_hook, dest_hook)


# ---------------------------------------------------------------------------
# _af2 cutover: _start_retry and _retry_batch_worker
# ---------------------------------------------------------------------------


@af2_only
def test_start_retry(mock_variable):
    from astronomer_starship._af2.cutover import _start_retry
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    update_dag_status(mid, "dag_1", "failed", error="boom")

    with patch("astronomer_starship._af2.cutover._retry_batch_worker"):
        _start_retry(mid, ["dag_1"], {"source_conn_id": "x"})

    m = get_migration(mid)
    assert m["dags"]["dag_1"]["status"] == "pending"
    assert m["status"] == "running"


@af2_only
def test_retry_batch_worker_completed(mock_variable):
    from astronomer_starship._af2.cutover import _retry_batch_worker
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        update_dag_status,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()

    def _batch_side_effect(migration_id, dag_ids, config):
        update_dag_status(migration_id, "dag_1", "completed", dag_runs_migrated=10)

    with patch("astronomer_starship._af2.cutover._run_dag_batch", side_effect=_batch_side_effect):
        _retry_batch_worker(mid, ["dag_1"], {"source_conn_id": "x"})

    m = get_migration(mid)
    assert m["status"] == "completed"


@af2_only
def test_retry_batch_worker_aborted(mock_variable):
    from astronomer_starship._af2.cutover import _retry_batch_worker
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_events[mid] = threading.Event()
    _abort_events[mid].set()

    with patch("astronomer_starship._af2.cutover._run_dag_batch"):
        _retry_batch_worker(mid, ["dag_1"], {"source_conn_id": "x"})

    m = get_migration(mid)
    assert m["status"] == "aborted"


# ---------------------------------------------------------------------------
# Service: migrate_dag with on_step callback and wait_for_scheduler
# ---------------------------------------------------------------------------


def test_migrate_dag_on_step_callback():
    """on_step callback is called at each migration phase."""
    from astronomer_starship.cutover.service import migrate_dag

    source_hook = MagicMock()
    source_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True}
    source_hook.get_dag_runs.return_value = {
        "dag_runs": [{"dag_id": "dag_1", "run_id": "r1", "data_interval_end": "2024-01-01T00:00:00"}],
    }
    source_hook.get_task_instances.return_value = {"task_instances": []}
    source_hook.get_task_instance_history.side_effect = Exception("Not available")

    dest_hook = MagicMock()
    dest_hook.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True, "dag_run_count": 0}

    steps = []
    migrate_dag("dag_1", source_hook, dest_hook, on_step=lambda s: steps.append(s))
    assert "Pre-checks" in steps
    assert "Fetching DAG runs" in steps
    assert "Writing DAG runs" in steps


def test_migrate_dag_abort_mid_migration():
    """Abort check during migration raises _AbortedError."""
    from astronomer_starship.cutover.service import (
        _abort_events,
        _AbortedError,
        migrate_dag,
    )

    mid = "test_abort_mid"
    _abort_events[mid] = threading.Event()
    _abort_events[mid].set()

    source_hook = MagicMock()
    dest_hook = MagicMock()

    with pytest.raises(_AbortedError):
        migrate_dag("dag_1", source_hook, dest_hook, migration_id=mid)


# ---------------------------------------------------------------------------
# Service: migrate_single_dag with pause/unpause and abort-after-transfer
# ---------------------------------------------------------------------------


def test_migrate_single_dag_with_pause_unpause(mock_variable):
    """migrate_single_dag correctly pauses source and unpauses dest."""
    from astronomer_starship.cutover.service import (
        create_migration,
        get_migration,
        migrate_single_dag,
    )

    config = {
        "source_conn_id": "x",
        "dag_run_limit": 10,
        "pause_in_source": True,
        "unpause_in_destination": True,
        "wait_for_scheduler": False,
        "wait_for_running": False,
    }
    mid = create_migration("bigbang", config, ["dag_1"])

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get_hook, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ) as mock_local:
        source = MagicMock()
        source.get_dag.return_value = {"dag_id": "dag_1", "is_paused": False}
        source.get_dag_runs.return_value = {"dag_runs": []}
        mock_get_hook.return_value = source

        dest = MagicMock()
        dest.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True, "dag_run_count": 0}
        mock_local.return_value = dest

        status = migrate_single_dag(mid, "dag_1", config)

    assert status == "completed"
    m = get_migration(mid)
    assert m["dags"]["dag_1"]["source_paused_by_us"] is True
    assert m["dags"]["dag_1"]["dest_unpaused_by_us"] is True


def test_migrate_single_dag_aborted_after_transfer(mock_variable):
    """Abort after data transfer still marks as completed but notes abort."""
    from astronomer_starship.cutover.service import (
        _abort_events,
        create_migration,
        get_migration,
        migrate_single_dag,
    )

    config = {
        "source_conn_id": "x",
        "dag_run_limit": 10,
        "pause_in_source": True,
        "unpause_in_destination": True,
        "wait_for_scheduler": False,
        "wait_for_running": False,
    }
    mid = create_migration("bigbang", config, ["dag_1"])
    _abort_events[mid] = threading.Event()

    from astronomer_starship.cutover.service import migrate_dag as real_migrate_dag

    with patch("astronomer_starship.cutover.service.get_source_hook") as mock_get_hook, patch(
        "astronomer_starship.cutover.service.StarshipLocalHook"
    ) as mock_local:
        source = MagicMock()
        source.get_dag.return_value = {"dag_id": "dag_1", "is_paused": False}
        source.get_dag_runs.return_value = {"dag_runs": []}
        mock_get_hook.return_value = source

        dest = MagicMock()
        dest.get_dag.return_value = {"dag_id": "dag_1", "is_paused": True, "dag_run_count": 0}
        mock_local.return_value = dest

        def _abort_after_migrate(*args, **kwargs):
            result = real_migrate_dag(*args, **kwargs)
            _abort_events[mid].set()
            return result

        with patch("astronomer_starship.cutover.service.migrate_dag", side_effect=_abort_after_migrate):
            status = migrate_single_dag(mid, "dag_1", config)

    assert status == "completed"
    m = get_migration(mid)
    assert "abort" in m["dags"]["dag_1"]["error"].lower()


# ---------------------------------------------------------------------------
# Service: delete_migrated_dag_data
# ---------------------------------------------------------------------------


def test_delete_migrated_dag_data_no_date():
    """Empty date returns 0."""
    from astronomer_starship.cutover.service import delete_migrated_dag_data

    assert delete_migrated_dag_data("dag_1", latest_data_interval_end=None, session=MagicMock()) == 0
    assert delete_migrated_dag_data("dag_1", latest_data_interval_end="", session=MagicMock()) == 0


# ---------------------------------------------------------------------------
# Service: purge_all_instance_dag_metadata
# ---------------------------------------------------------------------------


def test_purge_all_instance_dag_metadata():
    from astronomer_starship.cutover.service import purge_all_instance_dag_metadata

    mock_session = MagicMock()
    mock_session.query.return_value.distinct.return_value.all.return_value = [
        ("dag_1",),
        ("dag_2",),
    ]

    with patch("astronomer_starship.cutover.service.purge_dag_metadata") as mock_purge, patch(
        "astronomer_starship.cutover.service.NEW_SESSION", mock_session
    ):
        mock_purge.return_value = 5
        result = purge_all_instance_dag_metadata(session=mock_session)

    assert result == {"purged": 2, "errors": 0}


def test_purge_all_instance_dag_metadata_with_errors():
    from astronomer_starship.cutover.service import purge_all_instance_dag_metadata

    mock_session = MagicMock()
    mock_session.query.return_value.distinct.return_value.all.return_value = [
        ("dag_1",),
        ("dag_2",),
    ]

    call_count = 0

    def _purge_side_effect(dag_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("DB error")
        return 5

    with patch("astronomer_starship.cutover.service.purge_dag_metadata", side_effect=_purge_side_effect), patch(
        "astronomer_starship.cutover.service.NEW_SESSION", mock_session
    ):
        result = purge_all_instance_dag_metadata(session=mock_session)

    assert result == {"purged": 1, "errors": 1}


# ---------------------------------------------------------------------------
# Service: _wait_for_scheduler_sync
# ---------------------------------------------------------------------------


def test_wait_for_scheduler_sync_none():
    """No-op when latest_data_interval_end is None."""
    from astronomer_starship.cutover.service import _wait_for_scheduler_sync

    _wait_for_scheduler_sync("dag_1", None)


def test_wait_for_scheduler_sync_no_schedule():
    """No-op when DAG has no schedule interval."""
    from astronomer_starship.cutover.service import _wait_for_scheduler_sync

    with patch("astronomer_starship.cutover.service._get_schedule_interval", return_value=None):
        _wait_for_scheduler_sync("dag_1", "2024-01-01T00:00:00")


def test_wait_for_scheduler_sync_already_synced():
    """Returns immediately when next_dagrun is already past target."""
    from datetime import datetime, timezone

    from astronomer_starship.cutover.service import _wait_for_scheduler_sync

    future = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("astronomer_starship.cutover.service._get_schedule_interval", return_value="@daily"), patch(
        "astronomer_starship.cutover.service._get_next_dagrun", return_value=future
    ):
        _wait_for_scheduler_sync("dag_1", "2024-01-01T00:00:00+00:00")


# ---------------------------------------------------------------------------
# Hooks: StarshipLocalHook new methods (mocked compat layer)
# ---------------------------------------------------------------------------


def test_local_hook_set_dag_runs():
    from astronomer_starship.providers.starship.hooks.starship import StarshipLocalHook

    hook = StarshipLocalHook.__new__(StarshipLocalHook)
    mock_compat = MagicMock()
    mock_compat.set_dag_runs.return_value = {"status": "ok"}

    with patch(
        "astronomer_starship.providers.starship.hooks.starship.StarshipCompatabilityLayer",
        return_value=mock_compat,
    ):
        result = hook.set_dag_runs([{"dag_id": "dag_1"}])

    assert result == {"status": "ok"}
    mock_compat.set_dag_runs.assert_called_once()


def test_local_hook_set_task_instances():
    from astronomer_starship.providers.starship.hooks.starship import StarshipLocalHook

    hook = StarshipLocalHook.__new__(StarshipLocalHook)
    mock_compat = MagicMock()
    mock_compat.set_task_instances.return_value = {"status": "ok"}

    with patch(
        "astronomer_starship.providers.starship.hooks.starship.StarshipCompatabilityLayer",
        return_value=mock_compat,
    ):
        result = hook.set_task_instances([{"task_id": "t1"}])

    assert result == {"status": "ok"}


def test_local_hook_get_task_instance_history():
    from astronomer_starship.providers.starship.hooks.starship import StarshipLocalHook

    hook = StarshipLocalHook.__new__(StarshipLocalHook)
    mock_compat = MagicMock()
    mock_compat.get_task_instance_history.return_value = {"task_instances": []}

    with patch(
        "astronomer_starship.providers.starship.hooks.starship.StarshipCompatabilityLayer",
        return_value=mock_compat,
    ):
        result = hook.get_task_instance_history("dag_1", limit=50)

    assert result == {"task_instances": []}


def test_local_hook_set_task_instance_history():
    from astronomer_starship.providers.starship.hooks.starship import StarshipLocalHook

    hook = StarshipLocalHook.__new__(StarshipLocalHook)
    mock_compat = MagicMock()
    mock_compat.set_task_instance_history.return_value = {"status": "ok"}

    with patch(
        "astronomer_starship.providers.starship.hooks.starship.StarshipCompatabilityLayer",
        return_value=mock_compat,
    ):
        result = hook.set_task_instance_history([{"task_id": "t1"}])

    assert result == {"status": "ok"}


# ---------------------------------------------------------------------------
# Service: _is_aborted slow path (rate-limited DB check)
# ---------------------------------------------------------------------------


def test_is_aborted_rate_limited(mock_variable):
    """_is_aborted rate-limits DB reads via _abort_check_cache."""
    from astronomer_starship.cutover.service import (
        _abort_check_cache,
        _is_aborted,
        create_migration,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    _abort_check_cache.pop(mid, None)

    assert _is_aborted(mid) is False
    assert _is_aborted(mid) is False


def test_is_aborted_detects_persisted_abort_no_event(mock_variable):
    """_is_aborted detects abort from Variable even without in-memory event."""
    from astronomer_starship.cutover.service import (
        _abort_check_cache,
        _is_aborted,
        create_migration,
        request_abort,
    )

    mid = create_migration("bigbang", {"source_conn_id": "x"}, ["dag_1"])
    request_abort(mid)

    # Force cache expiry
    _abort_check_cache[mid] = 0

    assert _is_aborted(mid) is True


# ---------------------------------------------------------------------------
# Service: resolve_dag_patterns with empty patterns after filtering
# ---------------------------------------------------------------------------


def test_resolve_dag_patterns_empty_after_filter():
    """All whitespace patterns are skipped."""
    from astronomer_starship.cutover.service import resolve_dag_patterns

    source_hook = MagicMock()
    source_hook.get_dags.return_value = [{"dag_id": "etl_a"}]
    local_hook = MagicMock()
    local_hook.get_dags.return_value = [{"dag_id": "etl_a"}]

    result = resolve_dag_patterns(source_hook, patterns=["  ", "", "  \t  "], local_hook=local_hook)
    assert result == []
