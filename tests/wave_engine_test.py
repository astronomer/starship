"""Unit tests for the cutover wave engine.

State management, abort, DAG-pattern resolution, retry/purge/enrich
helpers, and the high-level ``start_wave`` validation. The compat-layer
state methods are replaced with an in-memory fake so these tests do not
hit a real Airflow DB.
"""

import threading
from unittest.mock import MagicMock, patch

import pytest


class FakeCompat:
    """Minimal stand-in for StarshipCompatabilityLayer that keeps cutover state in a dict."""

    def __init__(self):
        self.store: dict = {}

    def get_cutover_state(self, key):
        return self.store.get(key, {"migrations": []})

    def save_cutover_state(self, key, state):
        self.store[key] = state


@pytest.fixture
def fake_variable():
    """Patch ``_compat`` inside the cutover module with an in-memory fake."""
    from astronomer_starship.providers.starship import cutover as _cutover

    fc = FakeCompat()
    with patch.object(_cutover, "_compat", return_value=fc):
        # Also reset the in-memory caches so tests don't leak state.
        _cutover._abort_events.clear()
        _cutover._abort_check_cache.clear()
        _cutover._step_labels.clear()
        yield fc


class TestStateManagement:
    def test_empty_state_returns_default(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        assert _cutover.get_state() == {"migrations": []}

    def test_create_migration_appends_and_assigns_id(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {"source_conn_id": "x"}, ["dag_a", "dag_b"])
        assert mid.startswith("incremental_")
        state = _cutover.get_state()
        assert len(state["migrations"]) == 1
        m = state["migrations"][0]
        assert m["id"] == mid
        assert m["type"] == "incremental"
        assert set(m["dags"].keys()) == {"dag_a", "dag_b"}
        for dag_state in m["dags"].values():
            assert dag_state["status"] == "pending"

    def test_update_dag_status_patches_details(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["dag_a"])
        _cutover.update_dag_status(mid, "dag_a", "running", step="Fetching", dag_runs_migrated=3)
        m = _cutover.get_migration(mid)
        assert m["dags"]["dag_a"]["status"] == "running"
        assert m["dags"]["dag_a"]["step"] == "Fetching"
        assert m["dags"]["dag_a"]["dag_runs_migrated"] == 3

    def test_update_migration_status_sets_completed_at(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("bigbang", {}, ["dag_a"])
        _cutover.update_migration_status(mid, "completed")
        m = _cutover.get_migration(mid)
        assert m["status"] == "completed"
        assert m["completed_at"] is not None

    def test_step_labels_are_in_memory(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        _cutover.set_step_label("m1", "d1", "Step A")
        assert _cutover.get_step_label("m1", "d1") == "Step A"
        _cutover.cleanup_step_labels("m1")
        assert _cutover.get_step_label("m1", "d1") is None


class TestAbort:
    def test_request_abort_sets_flag_and_in_memory_event(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1"])
        _cutover._abort_events[mid] = threading.Event()
        _cutover.request_abort(mid)
        assert _cutover.get_migration(mid)["abort_requested"] is True
        assert _cutover._abort_events[mid].is_set()
        assert _cutover._is_aborted(mid) is True

    def test_is_aborted_false_when_no_request(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1"])
        assert _cutover._is_aborted(mid) is False

    def test_cleanup_abort_event_drops_entry(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        _cutover._abort_events["m1"] = threading.Event()
        _cutover._abort_check_cache["m1"] = 0.0
        _cutover._cleanup_abort_event("m1")
        assert "m1" not in _cutover._abort_events
        assert "m1" not in _cutover._abort_check_cache


class TestResolveDagPatterns:
    def _hooks(self, source_dags, local_dags):
        src = MagicMock()
        src.get_dags.return_value = [{"dag_id": d} for d in source_dags]
        local = MagicMock()
        local.get_dags.return_value = [{"dag_id": d} for d in local_dags]
        return src, local

    def test_empty_patterns_returns_intersection(self):
        from astronomer_starship.providers.starship.cutover import resolve_dag_patterns

        src, local = self._hooks(["a", "b", "c"], ["a", "b"])
        assert resolve_dag_patterns(source_hook=src, patterns=[], local_hook=local) == ["a", "b"]

    def test_fnmatch_patterns_filter(self):
        from astronomer_starship.providers.starship.cutover import resolve_dag_patterns

        src, local = self._hooks(["etl_a", "etl_b", "reporting_c"], ["etl_a", "etl_b", "reporting_c"])
        assert resolve_dag_patterns(source_hook=src, patterns=["etl_*"], local_hook=local) == [
            "etl_a",
            "etl_b",
        ]

    def test_system_and_migration_dags_are_excluded(self):
        from astronomer_starship.providers.starship.cutover import resolve_dag_patterns

        src, local = self._hooks(
            ["airflow_monitoring", "starship_airflow_migration_dag_x", "my_dag"],
            ["airflow_monitoring", "starship_airflow_migration_dag_x", "my_dag"],
        )
        assert resolve_dag_patterns(source_hook=src, patterns=[], local_hook=local) == ["my_dag"]

    def test_dags_not_in_local_are_skipped(self):
        from astronomer_starship.providers.starship.cutover import resolve_dag_patterns

        src, local = self._hooks(["only_in_source", "on_both"], ["on_both"])
        assert resolve_dag_patterns(source_hook=src, patterns=[], local_hook=local) == ["on_both"]


class TestNormalizeConfig:
    def test_defaults(self):
        from astronomer_starship.providers.starship.cutover import _normalize_config

        cfg = _normalize_config(None)
        assert cfg["source_conn_id"] == "starship_source"
        assert cfg["dag_run_limit"] == 500
        assert cfg["parallel_workers"] == 4
        assert cfg["pause_in_source"] is True
        assert cfg["unpause_in_target"] is False

    def test_parallel_workers_capped_at_16(self):
        from astronomer_starship.providers.starship.cutover import _normalize_config

        assert _normalize_config({"parallel_workers": 99})["parallel_workers"] == 16

    def test_unpause_in_destination_backcompat(self):
        from astronomer_starship.providers.starship.cutover import _normalize_config

        # Old key name is still accepted so pre-rewrite persisted state keeps working.
        assert _normalize_config({"unpause_in_destination": True})["unpause_in_target"] is True


class TestStartWaveValidation:
    def test_unknown_strategy_rejected(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        with pytest.raises(_cutover.InvalidWaveConfigError):
            _cutover.start_wave(strategy="bogus", patterns=[])

    def test_incremental_requires_patterns(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        with pytest.raises(_cutover.InvalidWaveConfigError, match="at least one"):
            _cutover.start_wave(strategy="incremental", patterns=[])


class TestRetryDagsInWave:
    def test_retry_failed_selector(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1", "d2", "d3"])
        _cutover.update_dag_status(mid, "d1", "failed")
        _cutover.update_dag_status(mid, "d2", "completed")
        _cutover.update_dag_status(mid, "d3", "failed")
        with patch.object(_cutover, "start_retry") as mocked:
            dags = _cutover.retry_dags_in_wave(mid, "failed")
        assert sorted(dags) == ["d1", "d3"]
        mocked.assert_called_once()

    def test_retry_skipped_selector(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1", "d2"])
        _cutover.update_dag_status(mid, "d1", "skipped")
        _cutover.update_dag_status(mid, "d2", "completed")
        with patch.object(_cutover, "start_retry") as mocked:
            dags = _cutover.retry_dags_in_wave(mid, "skipped")
        assert dags == ["d1"]
        mocked.assert_called_once()

    def test_retry_specific_dag_rejects_non_failed(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1"])
        _cutover.update_dag_status(mid, "d1", "completed")
        with pytest.raises(_cutover.InvalidWaveConfigError, match="not in a failed/skipped"):
            _cutover.retry_dags_in_wave(mid, "d1")

    def test_retry_unknown_dag_rejected(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1"])
        with pytest.raises(_cutover.DagNotInWaveError, match="not in wave"):
            _cutover.retry_dags_in_wave(mid, "nope")

    def test_retry_nonexistent_wave_rejected(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        with pytest.raises(_cutover.WaveNotFoundError, match="not found"):
            _cutover.retry_dags_in_wave("no-such-wave", "failed")


class TestEnrichWaveForDisplay:
    def test_unknown_wave_returns_none(self, fake_variable):
        from astronomer_starship.providers.starship.cutover import enrich_wave_for_display

        assert enrich_wave_for_display("nope") is None

    def test_overlays_in_memory_step_for_running_dags(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1", "d2"])
        _cutover.update_dag_status(mid, "d1", "running", step="Stale")
        _cutover.update_dag_status(mid, "d2", "completed", step="Done")
        _cutover.set_step_label(mid, "d1", "Live step")
        # Only d1 (running) should get the in-memory overlay.
        _cutover.set_step_label(mid, "d2", "Should-be-ignored")

        enriched = _cutover.enrich_wave_for_display(mid)
        assert enriched["dags"]["d1"]["step"] == "Live step"
        assert enriched["dags"]["d2"]["step"] == "Done"
        assert enriched["summary"]["running"] == 1
        assert enriched["summary"]["completed"] == 1
        assert enriched["summary"]["total"] == 2


class TestListMigrations:
    def test_newest_first_with_limit(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        m1 = _cutover.create_migration("incremental", {}, ["d1"])
        m2 = _cutover.create_migration("incremental", {}, ["d2"])
        m3 = _cutover.create_migration("incremental", {}, ["d3"])
        out = _cutover.list_migrations(limit=2)
        assert out["total"] == 3
        assert out["limit"] == 2
        assert [m["id"] for m in out["migrations"]] == [m3, m2]
        _ = m1  # included in total only
