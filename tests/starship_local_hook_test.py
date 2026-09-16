"""Unit tests for the migration operators' source hook.

``SourceHook`` is resolved once, at import time, to whichever class the
running Airflow version supports: AF2 direct-DB ``StarshipLocalHook``, or
HTTP-based ``StarshipHttpHook`` otherwise (Airflow 3 today).
``assert_source_conn_exists`` is the separate, per-call connection check --
a no-op on AF2, and a clear error on AF3 if the HTTP connection is missing.
The default dev venv pins ``apache-airflow<3.4`` so this file typically runs
against Airflow 3.x.
"""

from unittest.mock import patch

import pytest

from astronomer_starship.compat import AIRFLOW_V_2, AIRFLOW_V_3
from astronomer_starship.providers.starship.hooks.starship import (
    STARSHIP_SOURCE_CONN_ID,
    StarshipHook,
    StarshipHttpHook,
)
from astronomer_starship.providers.starship.operators.starship import (
    SourceHook,
    assert_source_conn_exists,
)

af3_only = pytest.mark.skipif(not AIRFLOW_V_3, reason="AF3-only behaviour")
af2_only = pytest.mark.skipif(not AIRFLOW_V_2, reason="AF2-only behaviour")


class TestSourceHookAf3:
    """AF3: SourceHook is StarshipHttpHook, used against the ``starship_source`` connection."""

    @af3_only
    def test_default_conn_id_constant(self):
        assert STARSHIP_SOURCE_CONN_ID == "starship_source"

    @af3_only
    def test_is_plain_http_hook(self):
        assert SourceHook is StarshipHttpHook

    @af3_only
    def test_missing_conn_raises_helpful_error(self, monkeypatch):
        # No connection defined and no env var -> assert_source_conn_exists
        # should raise a RuntimeError that names the connection and mentions
        # the HTTP requirement, not a bare AirflowNotFoundException.
        monkeypatch.delenv("AIRFLOW_CONN_STARSHIP_SOURCE", raising=False)
        with pytest.raises(RuntimeError) as exc_info:
            assert_source_conn_exists(STARSHIP_SOURCE_CONN_ID)
        msg = str(exc_info.value)
        assert "starship_source" in msg
        assert "HTTP" in msg

    @af3_only
    def test_env_var_conn_passes_check(self, monkeypatch):
        # Setting AIRFLOW_CONN_STARSHIP_SOURCE is Airflow's supported way to
        # register a connection without touching the metadata DB.
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        assert_source_conn_exists(STARSHIP_SOURCE_CONN_ID)  # does not raise
        hook = SourceHook(http_conn_id=STARSHIP_SOURCE_CONN_ID)
        assert isinstance(hook, StarshipHttpHook)
        assert isinstance(hook, StarshipHook)

    @af3_only
    def test_custom_conn_id(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_MY_SOURCE", "http://source.example.com/")
        assert_source_conn_exists("my_source")  # does not raise
        hook = SourceHook(http_conn_id="my_source")
        assert hook.http_conn_id == "my_source"

    @af3_only
    def test_set_dag_is_paused_hits_expected_url(self, monkeypatch):
        # set_dag_is_paused pauses the source DAG during migration -- verify
        # the plain StarshipHttpHook implementation is used as-is (no guard
        # or override layered on top of it).
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        hook = SourceHook(http_conn_id=STARSHIP_SOURCE_CONN_ID)

        class _MockResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {"dag_id": "d", "is_paused": True}

        with patch.object(hook, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.patch.return_value = _MockResponse()
            result = hook.set_dag_is_paused(dag_id="d", is_paused=True)

        assert result == {"dag_id": "d", "is_paused": True}
        mock_get_conn.return_value.patch.assert_called_once()
        called_url = mock_get_conn.return_value.patch.call_args.args[0]
        assert called_url.endswith("/api/starship/dags")


class TestSourceHookAf2:
    """AF2: SourceHook is StarshipLocalHook, reading directly from the local Airflow DB."""

    @af2_only
    def test_is_basehook_not_httphook(self):
        from airflow.hooks.base import BaseHook
        from airflow.providers.http.hooks.http import HttpHook

        hook = SourceHook(http_conn_id=STARSHIP_SOURCE_CONN_ID)
        assert isinstance(hook, BaseHook)
        assert not isinstance(hook, HttpHook)

    @af2_only
    def test_assert_source_conn_exists_is_a_noop(self):
        # Direct DB access needs no connection, so the check never raises on
        # AF2, even for a nonexistent connection id.
        assert_source_conn_exists("does-not-exist")

    @af2_only
    @pytest.mark.parametrize(
        "method, kwargs",
        [
            ("set_variable", {}),
            ("set_pool", {}),
            ("set_connection", {}),
            ("set_dag_runs", {"dag_runs": []}),
            ("set_task_instances", {"task_instances": []}),
        ],
    )
    def test_read_only_setters_raise(self, method, kwargs):
        hook = SourceHook(http_conn_id=STARSHIP_SOURCE_CONN_ID)
        with pytest.raises(RuntimeError, match="not supported"):
            getattr(hook, method)(**kwargs)

    @af2_only
    def test_accepts_http_conn_id_kwarg(self):
        # For API parity with the HTTP-based source hook so operator code can
        # pass `http_conn_id=...` uniformly across Airflow versions.
        hook = SourceHook(http_conn_id="some_source")
        from airflow.hooks.base import BaseHook

        assert isinstance(hook, BaseHook)


class TestStarshipMigrationOperator:
    """Constructor-level wiring of source/target connection kwargs."""

    def _make_op(self, **kwargs):
        from astronomer_starship.providers.starship.operators.starship import (
            StarshipMigrationOperator,
        )

        # BaseOperator requires task_id; DAG context is not needed for __init__.
        return StarshipMigrationOperator(task_id="t", **kwargs)

    def test_target_http_conn_id_wins_over_http_conn_id(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        op = self._make_op(
            http_conn_id="legacy_target",
            target_http_conn_id="explicit_target",
        )
        assert op.target_hook.http_conn_id == "explicit_target"

    def test_http_conn_id_used_as_target_fallback(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        op = self._make_op(http_conn_id="legacy_target")
        assert op.target_hook.http_conn_id == "legacy_target"

    @af3_only
    def test_source_http_conn_id_wires_source_hook(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_CUSTOM_SOURCE", "http://source.example.com/")
        op = self._make_op(
            http_conn_id="target",
            source_http_conn_id="custom_source",
        )
        assert op.source_hook.http_conn_id == "custom_source"

    @af3_only
    def test_source_defaults_to_starship_source(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        op = self._make_op(http_conn_id="target")
        assert op.source_hook.http_conn_id == STARSHIP_SOURCE_CONN_ID
