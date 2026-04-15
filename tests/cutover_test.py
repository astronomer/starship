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
