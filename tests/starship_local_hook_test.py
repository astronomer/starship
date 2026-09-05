"""Unit tests for the StarshipLocalHook class.

These tests exercise whichever code path (AF2 direct-DB or AF3 HTTP) is
selected at import time based on the Airflow version running the test suite.
The default dev venv pins ``apache-airflow<3.4`` so this file typically runs
against Airflow 3.x.
"""

from unittest.mock import patch

import pytest

from astronomer_starship.compat import AIRFLOW_V_3
from astronomer_starship.providers.starship.hooks.starship import (
    STARSHIP_SOURCE_CONN_ID,
    StarshipHook,
    StarshipLocalHook,
)

af3_only = pytest.mark.skipif(not AIRFLOW_V_3, reason="AF3-only behaviour")
af2_only = pytest.mark.skipif(AIRFLOW_V_3, reason="AF2-only behaviour")


class TestStarshipLocalHookAf3:
    """AF3: LocalHook is an HttpHook against the ``starship_source`` connection."""

    @af3_only
    def test_default_conn_id_constant(self):
        assert STARSHIP_SOURCE_CONN_ID == "starship_source"

    @af3_only
    def test_missing_conn_raises_helpful_error(self, monkeypatch):
        # No connection defined and no env var -> the __init__ check should
        # raise a RuntimeError that names the connection and mentions the AF3
        # HTTP requirement, not a bare AirflowNotFoundException.
        monkeypatch.delenv("AIRFLOW_CONN_STARSHIP_SOURCE", raising=False)
        with pytest.raises(RuntimeError) as exc_info:
            StarshipLocalHook()
        msg = str(exc_info.value)
        assert "starship_source" in msg
        assert "HTTP" in msg
        assert "Airflow 3" in msg

    @af3_only
    def test_env_var_conn_allows_instantiation(self, monkeypatch):
        # Setting AIRFLOW_CONN_STARSHIP_SOURCE is Airflow's supported way to
        # register a connection without touching the metadata DB.
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        hook = StarshipLocalHook()
        from airflow.providers.http.hooks.http import HttpHook

        assert isinstance(hook, HttpHook)
        assert isinstance(hook, StarshipHook)

    @af3_only
    def test_custom_conn_id(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_MY_SOURCE", "http://source.example.com/")
        hook = StarshipLocalHook(http_conn_id="my_source")
        assert hook.http_conn_id == "my_source"

    @af3_only
    @pytest.mark.parametrize(
        "method, kwargs",
        [
            ("set_variable", {"key": "k", "val": "v"}),
            ("set_pool", {"name": "p", "slots": 1}),
            ("set_connection", {"conn_id": "c"}),
            ("set_dag_runs", {"dag_runs": []}),
            ("set_task_instances", {"task_instances": []}),
        ],
    )
    def test_read_only_setters_raise(self, monkeypatch, method, kwargs):
        # Read-only semantics are preserved from the AF2 LocalHook: mutating
        # the source instance is not part of Starship's migration flow, so
        # these setters raise RuntimeError before any HTTP call is made.
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        hook = StarshipLocalHook()
        with pytest.raises(RuntimeError, match="not supported"):
            getattr(hook, method)(**kwargs)

    @af3_only
    def test_set_dag_is_paused_hits_expected_url(self, monkeypatch):
        # set_dag_is_paused is the one allowed setter -- it pauses the source
        # DAG during migration.
        monkeypatch.setenv("AIRFLOW_CONN_STARSHIP_SOURCE", "http://source.example.com/")
        hook = StarshipLocalHook()

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


class TestStarshipLocalHookAf2:
    """AF2: LocalHook reads directly from the local Airflow DB via the compat layer."""

    @af2_only
    def test_is_basehook_not_httphook(self):
        from airflow.hooks.base import BaseHook
        from airflow.providers.http.hooks.http import HttpHook

        hook = StarshipLocalHook()
        assert isinstance(hook, BaseHook)
        assert not isinstance(hook, HttpHook)

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
        hook = StarshipLocalHook()
        with pytest.raises(RuntimeError, match="not supported"):
            getattr(hook, method)(**kwargs)
