"""Unit tests for the unified operator engine, source-auth module, and the
StarshipHttpHook fixes that ship with the provider consolidation PR.

These are **unit** tests: they mock Airflow's hooks and do not hit a real
Airflow database. The module imports require ``apache-airflow`` to be
installed (so ``from airflow.hooks.base import BaseHook`` resolves), but
no DB-facing entry point is exercised.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

# Import BaseHook directly so ``patch.object(BaseHook, ...)`` can target the
# already-loaded class. The earlier attempt to use
# ``patch("airflow.hooks.base.BaseHook.get_connection", ...)`` broke on CI
# because Airflow 2.10's ``airflow.hooks`` package uses deprecation
# shims that interfere with ``mock._dot_lookup``'s attribute walk, even
# after ``import airflow.hooks.base``.
from airflow.hooks.base import BaseHook  # noqa: E402  isort:skip

from astronomer_starship.providers.starship.auth.factory import BearerTokenAuth
from astronomer_starship.providers.starship.auth.mwaa import make_mwaa_auth

# ---------------------------------------------------------------------------
# Auth classes
# ---------------------------------------------------------------------------


class TestAuthClasses:
    def test_bearer_sets_authorization_header(self):
        req = requests.Request(method="GET", url="https://x").prepare()
        auth = BearerTokenAuth(password="mytoken")
        auth(req)
        assert req.headers["Authorization"] == "Bearer mytoken"

    def test_bearer_without_token_raises(self):
        req = requests.Request(method="GET", url="https://x").prepare()
        auth = BearerTokenAuth(password=None)
        with pytest.raises(RuntimeError):
            auth(req)

    def test_mwaa_factory_requires_region_and_env(self):
        with pytest.raises(RuntimeError):
            make_mwaa_auth(region="", environment_name="env")
        with pytest.raises(RuntimeError):
            make_mwaa_auth(region="us-west-2", environment_name="")

    def test_mwaa_factory_returns_auth_subclass(self):
        cls = make_mwaa_auth(region="us-west-2", environment_name="env")
        assert issubclass(cls, requests.auth.AuthBase)


# ---------------------------------------------------------------------------
# Auth factory dispatch
# ---------------------------------------------------------------------------


def _fake_conn(conn_type="http", password=None, login=None, extra=None):
    c = MagicMock()
    c.conn_type = conn_type
    c.password = password
    c.login = login
    c.extra = json.dumps(extra) if extra is not None else None
    return c


class TestResolveSourceAuth:
    def _get_connection_patch(self, conn):
        # Patch ``get_connection`` on the already-imported ``BaseHook`` class.
        # Using ``patch.object`` avoids mock's dotted-string ``_dot_lookup``
        # which can't walk ``airflow.hooks.base`` on clean CI interpreters
        # (Airflow's deprecation shims drop the attribute after import).
        return patch.object(BaseHook, "get_connection", return_value=conn)

    def test_http_bearer_dispatch(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(conn_type="http", password="tok")  # pragma: allowlist secret
        ):
            assert factory.resolve_source_auth("starship_source") is BearerTokenAuth

    def test_http_basic_dispatch_returns_none(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(conn_type="http", login="user", password="pw")  # pragma: allowlist secret
        ):
            # None = HttpHook does Basic natively.
            assert factory.resolve_source_auth("starship_source") is None

    def test_http_without_credentials_rejected(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(conn_type="http")):
            with pytest.raises(RuntimeError):
                factory.resolve_source_auth("starship_source")

    def test_gcc_without_impersonation_picks_default(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(conn_type="google_cloud_platform")):
            cls = factory.resolve_source_auth("starship_source")
            assert cls.__name__ == "ComposerV2BearerAuth"

    def test_gcc_with_impersonation_chain_picks_impersonated(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(conn_type="google_cloud_platform", extra={"impersonation_chain": ["sa@x.iam"]})
        ):
            cls = factory.resolve_source_auth("starship_source")
            assert cls.__name__ == "ImpersonatedComposerAuth"

    def test_mwaa_dispatch(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(
                conn_type="aws",
                extra={
                    "region_name": "us-west-2",
                    "environment_name": "env1",
                },
            )
        ):
            cls = factory.resolve_source_auth("starship_source")
            assert issubclass(cls, requests.auth.AuthBase)

    def test_aws_missing_region_rejected(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(conn_type="aws", extra={"environment_name": "env1"})):
            with pytest.raises(RuntimeError, match="region_name"):
                factory.resolve_source_auth("starship_source")

    def test_unknown_conn_type_rejected(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(conn_type="bogus")):
            with pytest.raises(RuntimeError):
                factory.resolve_source_auth("starship_source")


# ---------------------------------------------------------------------------
# StarshipHttpHook: non-HTTP extras should not leak as headers
# ---------------------------------------------------------------------------


class TestHttpHookHeaderStrip:
    def test_non_standard_extras_are_dropped_from_session(self):
        """HttpHook forwards connection ``extra`` keys as session headers;
        Starship must strip the auth-factory hints so they don't go out as
        real HTTP headers."""
        from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

        hook = StarshipHttpHook(http_conn_id="dummy")
        fake_session = requests.Session()
        fake_session.headers.update(
            {
                "Accept": "application/json",
                "impersonation_chain": "leaked-value",
                "region_name": "us-west-2",
                "Authorization": "Bearer legit",
            }
        )
        with patch("airflow.providers.http.hooks.http.HttpHook.get_conn", return_value=fake_session):
            session = hook.get_conn()
        assert "impersonation_chain" not in session.headers
        assert "region_name" not in session.headers
        # Legit HTTP headers are preserved.
        assert session.headers["Accept"] == "application/json"
        assert session.headers["Authorization"] == "Bearer legit"


class TestHttpHookBearerRebind:
    def test_bearer_token_conn_rebinds_auth_with_password(self):
        """Airflow 2.10 HttpHook calls ``auth_type()`` with zero args when
        ``conn.login`` is empty. For bearer-token-in-password connections
        that strips the token. StarshipHttpHook must re-instantiate auth
        with both (login, password) so the token lands."""
        from astronomer_starship.providers.starship.auth.factory import BearerTokenAuth
        from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

        hook = StarshipHttpHook(http_conn_id="dummy", auth_type=BearerTokenAuth)
        fake_session = requests.Session()
        # Simulate what HttpHook's zero-arg branch does: session.auth set to
        # a BearerTokenAuth with no token.
        fake_session.auth = BearerTokenAuth()
        fake_conn = MagicMock(login=None, password="my-token")

        with patch("airflow.providers.http.hooks.http.HttpHook.get_conn", return_value=fake_session), patch.object(
            StarshipHttpHook, "get_connection", return_value=fake_conn
        ):
            session = hook.get_conn()

        # Rebound: a fresh BearerTokenAuth carrying the real token.
        assert isinstance(session.auth, BearerTokenAuth)
        assert session.auth.token == "my-token"

    def test_rebind_is_skipped_when_login_is_set(self):
        """HTTP Basic (login + password) already works via super(); don't
        overwrite the correctly-bound auth."""
        from astronomer_starship.providers.starship.auth.factory import BearerTokenAuth
        from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

        hook = StarshipHttpHook(http_conn_id="dummy", auth_type=BearerTokenAuth)
        sentinel_auth = object()
        fake_session = requests.Session()
        fake_session.auth = sentinel_auth  # pretend super() bound it correctly
        fake_conn = MagicMock(login="user", password="pw")

        with patch("airflow.providers.http.hooks.http.HttpHook.get_conn", return_value=fake_session), patch.object(
            StarshipHttpHook, "get_connection", return_value=fake_conn
        ):
            session = hook.get_conn()

        # Untouched — the rebind guard only fires when login is empty.
        assert session.auth is sentinel_auth


# ---------------------------------------------------------------------------
# Unified operator engine: migrate_dag_history + pause_unpause_dag
# ---------------------------------------------------------------------------


def _mk_hook():
    hook = MagicMock()
    hook.get_dag.return_value = {"is_paused": True, "dag_run_count": 0}
    hook.get_dag_runs.return_value = {
        "dag_runs": [{"run_id": "r1", "data_interval_end": "2024-01-02T00:00:00+00:00"}],
    }
    hook.get_task_instances.return_value = {"task_instances": [{"task_id": "t1", "run_id": "r1"}]}
    hook.get_task_instance_history.return_value = {"task_instances": []}
    return hook


class TestMigrateDagHistory:
    def test_happy_path_writes_runs_and_tis(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        result = migrate_dag_history(
            source_hook=src,
            target_hook=tgt,
            target_dag_id="d1",
            dag_run_limit=10,
            pause_dag_in_source=False,
            unpause_dag_in_target=False,
        )
        assert result["dag_runs_migrated"] == 1
        assert result["task_instances_migrated"] == 1
        assert result["latest_data_interval_end"] == "2024-01-02T00:00:00+00:00"
        tgt.set_dag_runs.assert_called_once()
        tgt.set_task_instances.assert_called_once()
        # TI history is opt-in; by default it's not fetched.
        src.get_task_instance_history.assert_not_called()

    def test_empty_dag_runs_skips(self):
        from airflow.exceptions import AirflowSkipException

        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src = _mk_hook()
        src.get_dag_runs.return_value = {"dag_runs": []}
        with pytest.raises(AirflowSkipException):
            migrate_dag_history(
                source_hook=src,
                target_hook=_mk_hook(),
                target_dag_id="d1",
                pause_dag_in_source=False,
            )

    def test_pause_in_source_calls_source_set_paused(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        migrate_dag_history(
            source_hook=src,
            target_hook=tgt,
            target_dag_id="d1",
            pause_dag_in_source=True,
        )
        src.set_dag_is_paused.assert_any_call(dag_id="d1", is_paused=True)

    def test_unpause_in_target_calls_target_set_paused(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        migrate_dag_history(
            source_hook=src,
            target_hook=tgt,
            target_dag_id="d1",
            pause_dag_in_source=False,
            unpause_dag_in_target=True,
        )
        tgt.set_dag_is_paused.assert_any_call(dag_id="d1", is_paused=False)

    def test_pre_checks_reject_unpaused_target(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        tgt.get_dag.return_value = {"is_paused": False, "dag_run_count": 0}
        with pytest.raises(RuntimeError, match="active in target"):
            migrate_dag_history(
                source_hook=src,
                target_hook=tgt,
                target_dag_id="d1",
                pause_dag_in_source=False,
                pre_checks=True,
            )

    def test_pre_checks_reject_target_with_existing_runs(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        tgt.get_dag.return_value = {"is_paused": True, "dag_run_count": 42}
        with pytest.raises(RuntimeError, match="already has runs"):
            migrate_dag_history(
                source_hook=src,
                target_hook=tgt,
                target_dag_id="d1",
                pause_dag_in_source=False,
                pre_checks=True,
            )

    def test_migrate_ti_history_fetches_and_writes(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        src.get_task_instance_history.return_value = {"task_instances": [{"task_id": "t1", "try_number": 1}]}
        result = migrate_dag_history(
            source_hook=src,
            target_hook=tgt,
            target_dag_id="d1",
            pause_dag_in_source=False,
            migrate_ti_history=True,
        )
        src.get_task_instance_history.assert_called_once()
        tgt.set_task_instance_history.assert_called_once()
        assert result["task_instance_history_migrated"] == 1

    def test_on_step_callback_invoked(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        steps = []
        migrate_dag_history(
            source_hook=_mk_hook(),
            target_hook=_mk_hook(),
            target_dag_id="d1",
            pause_dag_in_source=False,
            on_step=steps.append,
        )
        # At minimum we touched "Fetching DAG runs" / "Fetching task instances"
        # / "Writing DAG runs" / "Writing task instances".
        assert "Fetching DAG runs" in steps
        assert "Writing DAG runs" in steps
        assert "Writing task instances" in steps

    def test_check_abort_before_write_bubbles(self):
        from astronomer_starship.providers.starship.operators.starship import (
            AbortedError,
            migrate_dag_history,
        )

        def _raise():
            raise AbortedError("stop")

        with pytest.raises(AbortedError):
            migrate_dag_history(
                source_hook=_mk_hook(),
                target_hook=_mk_hook(),
                target_dag_id="d1",
                pause_dag_in_source=False,
                check_abort=_raise,
            )


class TestPauseUnpauseDag:
    def test_pause_source_only(self):
        from astronomer_starship.providers.starship.operators.starship import pause_unpause_dag

        src, tgt = MagicMock(), MagicMock()
        r = pause_unpause_dag(
            dag_id="d1",
            source_hook=src,
            target_hook=tgt,
            pause_in_source=True,
            unpause_in_target=False,
        )
        assert r == {"source_paused_by_us": True, "target_unpaused_by_us": False}
        src.set_dag_is_paused.assert_called_once_with(dag_id="d1", is_paused=True)
        tgt.set_dag_is_paused.assert_not_called()

    def test_unpause_target_only(self):
        from astronomer_starship.providers.starship.operators.starship import pause_unpause_dag

        src, tgt = MagicMock(), MagicMock()
        r = pause_unpause_dag(
            dag_id="d1",
            source_hook=src,
            target_hook=tgt,
            pause_in_source=False,
            unpause_in_target=True,
        )
        assert r == {"source_paused_by_us": False, "target_unpaused_by_us": True}
        tgt.set_dag_is_paused.assert_called_once_with(dag_id="d1", is_paused=False)
        src.set_dag_is_paused.assert_not_called()

    def test_neither(self):
        from astronomer_starship.providers.starship.operators.starship import pause_unpause_dag

        src, tgt = MagicMock(), MagicMock()
        r = pause_unpause_dag(
            dag_id="d1",
            source_hook=src,
            target_hook=tgt,
            pause_in_source=False,
            unpause_in_target=False,
        )
        assert r == {"source_paused_by_us": False, "target_unpaused_by_us": False}
        src.set_dag_is_paused.assert_not_called()
        tgt.set_dag_is_paused.assert_not_called()
