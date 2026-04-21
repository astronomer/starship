"""Unit tests for the relocated cutover wave engine, unified operator engine,
auth factory, and source_connection helper.

These are **unit** tests: they mock Airflow's Variable / hooks and do not hit
a real Airflow database. Stress and end-to-end tests are tracked separately.

The module imports require ``apache-airflow`` to be installed (so
``from airflow.models import ...`` resolves), but the DB itself is never
exercised — every DB-facing entry point is patched.
"""

import json
import threading
from unittest.mock import MagicMock, patch

import pytest
import requests

from astronomer_starship.common import (
    STARSHIP_SOURCE_CONN_ID,
    SUPPORTED_SOURCE_PLATFORMS,
    HttpError,
    build_source_connection_kwargs,
)
from astronomer_starship.providers.starship.auth.astro import AstroBearerAuth
from astronomer_starship.providers.starship.auth.mwaa import make_mwaa_auth
from astronomer_starship.providers.starship.auth.oss import OssBearerAuth, resolve_oss_auth


# ---------------------------------------------------------------------------
# common.build_source_connection_kwargs
# ---------------------------------------------------------------------------


class TestBuildSourceConnectionKwargs:
    def test_astro_maps_token_to_password(self):
        k = build_source_connection_kwargs(
            {"platform": "astro", "url": "https://foo.astronomer.run/bar", "token": "tok"}
        )
        assert k["conn_id"] == STARSHIP_SOURCE_CONN_ID
        assert k["conn_type"] == "http"
        # Full URL lives in host so HttpHook uses it verbatim (covers the
        # Astro deployment-slug path segment).
        assert k["host"] == "https://foo.astronomer.run/bar"
        assert k["schema"] == "https"
        assert k["password"] == "tok"
        extras = json.loads(k["extra"])
        assert extras["starship_platform"] == "astro"

    def test_gcc_impersonation_chain_lands_in_extras(self):
        k = build_source_connection_kwargs(
            {
                "platform": "gcc",
                "url": "https://composer.example.com/",
                "impersonation_chain": ["a@x.iam", "b@x.iam"],
            }
        )
        extras = json.loads(k["extra"])
        assert extras["impersonation_chain"] == ["a@x.iam", "b@x.iam"]
        assert extras["starship_platform"] == "gcc"
        assert "password" not in k

    def test_gcc_rejects_non_list_impersonation_chain(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs(
                {
                    "platform": "gcc",
                    "url": "https://composer.example.com/",
                    "impersonation_chain": "single@x.iam",
                }
            )
        assert exc.value.status_code == 400

    def test_mwaa_requires_region(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs({"platform": "mwaa", "url": "https://mwaa.example.com/"})
        assert exc.value.status_code == 400
        assert "region" in exc.value.msg.lower()

    def test_mwaa_carries_region_env_and_role(self):
        k = build_source_connection_kwargs(
            {
                "platform": "mwaa",
                "url": "https://mwaa.example.com/",
                "region": "us-west-2",
                "environment_name": "my-env",
                "role_arn": "arn:aws:iam::1:role/StarshipSource",
            }
        )
        extras = json.loads(k["extra"])
        assert extras["region_name"] == "us-west-2"
        assert extras["environment_name"] == "my-env"
        assert extras["role_arn"] == "arn:aws:iam::1:role/StarshipSource"

    def test_oss_basic_passes_login_password(self):
        k = build_source_connection_kwargs(
            {
                "platform": "oss",
                "url": "https://airflow.example.com:8080/base",
                "login": "u",
                "password": "p",
            }
        )
        assert k["login"] == "u"
        assert k["password"] == "p"
        assert k["port"] == 8080
        # Full base URL lives in `host` so HttpHook uses it verbatim as
        # the base URL, preserving the /base deployment-path segment.
        assert k["host"] == "https://airflow.example.com:8080/base"

    def test_astro_style_path_segment_is_preserved_in_host(self):
        """Regression test for the Astro deployment-slug bug: the user's
        URL has ``/<slug>`` as a path; we must keep it in ``host`` so
        plugin-endpoint calls hit ``.../<slug>/api/starship/...`` rather
        than the bare hostname (which Astro's edge proxy rejects)."""
        k = build_source_connection_kwargs(
            {
                "platform": "astro",
                "url": "https://abc123.astronomer.run/slug-xyz",
                "token": "tok",
            }
        )
        assert k["host"] == "https://abc123.astronomer.run/slug-xyz"
        # extras.endpoint is no longer used — path lives in host.
        extras = json.loads(k["extra"])
        assert "endpoint" not in extras

    def test_url_without_path_still_works(self):
        """Hostnames without a path (MWAA, some OSS installs) must still
        produce a valid base URL — ``https://host`` with no trailing path."""
        k = build_source_connection_kwargs(
            {"platform": "astro", "url": "https://abc123.astronomer.run", "token": "tok"}
        )
        assert k["host"] == "https://abc123.astronomer.run"

    def test_oss_bearer_only(self):
        k = build_source_connection_kwargs(
            {"platform": "oss", "url": "https://airflow.example.com/", "token": "tok"}
        )
        assert k["password"] == "tok"
        assert "login" not in k

    def test_oss_without_creds_is_rejected(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs({"platform": "oss", "url": "https://airflow.example.com/"})
        assert exc.value.status_code == 400

    def test_astro_without_token_is_rejected(self):
        with pytest.raises(HttpError):
            build_source_connection_kwargs({"platform": "astro", "url": "https://x.astronomer.run/y"})

    def test_invalid_platform_rejected(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs({"platform": "bogus", "url": "https://x"})
        assert exc.value.status_code == 400

    def test_missing_url_rejected(self):
        with pytest.raises(HttpError):
            build_source_connection_kwargs({"platform": "astro", "token": "tok"})

    def test_invalid_url_rejected(self):
        with pytest.raises(HttpError):
            build_source_connection_kwargs({"platform": "astro", "url": "not-a-url", "token": "tok"})

    def test_supported_platforms_constant(self):
        # Guardrail: UI constants must stay aligned with the Python side.
        assert set(SUPPORTED_SOURCE_PLATFORMS) == {"astro", "mwaa", "gcc", "oss"}


# ---------------------------------------------------------------------------
# Auth classes (pure requests.auth.AuthBase subclasses)
# ---------------------------------------------------------------------------


class TestAuthClasses:
    def test_astro_bearer_sets_authorization_header(self):
        req = requests.Request(method="GET", url="https://x").prepare()
        auth = AstroBearerAuth(password="mytoken")
        auth(req)
        assert req.headers["Authorization"] == "Bearer mytoken"

    def test_astro_bearer_without_token_raises(self):
        req = requests.Request(method="GET", url="https://x").prepare()
        auth = AstroBearerAuth(password=None)
        with pytest.raises(RuntimeError):
            auth(req)

    def test_oss_bearer_sets_authorization_header(self):
        req = requests.Request(method="GET", url="https://x").prepare()
        auth = OssBearerAuth(password="tok")
        auth(req)
        assert req.headers["Authorization"] == "Bearer tok"

    def test_resolve_oss_auth_returns_none_for_basic(self):
        # None means "HttpHook does Basic itself".
        assert resolve_oss_auth("user", "pw") is None

    def test_resolve_oss_auth_returns_bearer_class_for_token_only(self):
        assert resolve_oss_auth(None, "tok") is OssBearerAuth

    def test_resolve_oss_auth_requires_something(self):
        with pytest.raises(RuntimeError):
            resolve_oss_auth(None, None)

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


def _fake_conn(password=None, login=None, extra=None):
    c = MagicMock()
    c.password = password
    c.login = login
    c.extra = json.dumps(extra) if extra is not None else None
    return c


class TestResolveSourceAuth:
    def _patch(self, conn):
        return patch(
            "astronomer_starship.providers.starship.auth.factory.BaseHook.get_connection",
            return_value=conn,
            create=False,
        )

    def _get_connection_patch(self, conn):
        # BaseHook is imported inside the function, so patch where it comes from.
        return patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn)

    def test_astro_dispatch(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(password="tok", extra={"starship_platform": "astro"})):
            assert factory.resolve_source_auth("starship_source") is AstroBearerAuth

    def test_oss_bearer_dispatch(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(password="tok", extra={"starship_platform": "oss"})):
            assert factory.resolve_source_auth("starship_source") is OssBearerAuth

    def test_oss_basic_dispatch_returns_none(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(login="user", password="pw", extra={"starship_platform": "oss"})
        ):
            # None = HttpHook does Basic natively.
            assert factory.resolve_source_auth("starship_source") is None

    def test_gcc_without_impersonation_picks_default(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(extra={"starship_platform": "gcc"})):
            cls = factory.resolve_source_auth("starship_source")
            assert cls.__name__ == "ComposerV2BearerAuth"

    def test_gcc_with_impersonation_chain_picks_impersonated(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(extra={"starship_platform": "gcc", "impersonation_chain": ["sa@x.iam"]})
        ):
            cls = factory.resolve_source_auth("starship_source")
            assert cls.__name__ == "ImpersonatedComposerAuth"

    def test_mwaa_dispatch(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(
            _fake_conn(
                extra={
                    "starship_platform": "mwaa",
                    "region_name": "us-west-2",
                    "environment_name": "env1",
                }
            )
        ):
            cls = factory.resolve_source_auth("starship_source")
            assert issubclass(cls, requests.auth.AuthBase)

    def test_unknown_platform_rejected(self):
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(extra={"starship_platform": "bogus"})):
            with pytest.raises(RuntimeError):
                factory.resolve_source_auth("starship_source")

    def test_backward_compat_no_hint_bearer_password_only(self):
        """Connections created before this refactor have no starship_platform hint.
        Fall back to AstroBearerAuth when only password is set."""
        from astronomer_starship.providers.starship.auth import factory

        with self._get_connection_patch(_fake_conn(password="tok")):
            assert factory.resolve_source_auth("starship_source") is AstroBearerAuth


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
                "starship_platform": "gcc",
                "Authorization": "Bearer legit",
            }
        )
        with patch("airflow.providers.http.hooks.http.HttpHook.get_conn", return_value=fake_session):
            session = hook.get_conn()
        assert "impersonation_chain" not in session.headers
        assert "starship_platform" not in session.headers
        # Legit HTTP headers are preserved.
        assert session.headers["Accept"] == "application/json"
        assert session.headers["Authorization"] == "Bearer legit"


class TestHttpHookBearerRebind:
    def test_bearer_token_conn_rebinds_auth_with_password(self):
        """Airflow 2.10 HttpHook calls ``auth_type()`` with zero args when
        ``conn.login`` is empty. For bearer-token-in-password connections
        (Astro, OSS-bearer) that strips the token. StarshipHttpHook must
        re-instantiate auth with both (login, password) so the token lands."""
        from astronomer_starship.providers.starship.auth.astro import AstroBearerAuth
        from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

        hook = StarshipHttpHook(http_conn_id="dummy", auth_type=AstroBearerAuth)
        fake_session = requests.Session()
        # Simulate what HttpHook's zero-arg branch does: session.auth set to
        # an AstroBearerAuth with no token.
        fake_session.auth = AstroBearerAuth()
        fake_conn = MagicMock(login=None, password="my-token")

        with (
            patch("airflow.providers.http.hooks.http.HttpHook.get_conn", return_value=fake_session),
            patch.object(StarshipHttpHook, "get_connection", return_value=fake_conn),
        ):
            session = hook.get_conn()

        # Rebound: a fresh AstroBearerAuth carrying the real token.
        assert isinstance(session.auth, AstroBearerAuth)
        assert session.auth.token == "my-token"

    def test_rebind_is_skipped_when_login_is_set(self):
        """HTTP Basic (login + password) already works via super(); don't
        overwrite the correctly-bound auth."""
        from astronomer_starship.providers.starship.auth.oss import OssBearerAuth
        from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

        hook = StarshipHttpHook(http_conn_id="dummy", auth_type=OssBearerAuth)
        sentinel_auth = object()
        fake_session = requests.Session()
        fake_session.auth = sentinel_auth  # pretend super() bound it correctly
        fake_conn = MagicMock(login="user", password="pw")

        with (
            patch("airflow.providers.http.hooks.http.HttpHook.get_conn", return_value=fake_session),
            patch.object(StarshipHttpHook, "get_connection", return_value=fake_conn),
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
            source_hook=src, target_hook=tgt, target_dag_id="d1", dag_run_limit=10,
            pause_dag_in_source=False, unpause_dag_in_target=False,
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
                source_hook=src, target_hook=_mk_hook(), target_dag_id="d1", pause_dag_in_source=False,
            )

    def test_pause_in_source_calls_source_set_paused(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        migrate_dag_history(
            source_hook=src, target_hook=tgt, target_dag_id="d1", pause_dag_in_source=True,
        )
        src.set_dag_is_paused.assert_any_call(dag_id="d1", is_paused=True)

    def test_unpause_in_target_calls_target_set_paused(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        migrate_dag_history(
            source_hook=src, target_hook=tgt, target_dag_id="d1",
            pause_dag_in_source=False, unpause_dag_in_target=True,
        )
        tgt.set_dag_is_paused.assert_any_call(dag_id="d1", is_paused=False)

    def test_pre_checks_reject_unpaused_target(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        tgt.get_dag.return_value = {"is_paused": False, "dag_run_count": 0}
        with pytest.raises(RuntimeError, match="active in target"):
            migrate_dag_history(
                source_hook=src, target_hook=tgt, target_dag_id="d1",
                pause_dag_in_source=False, pre_checks=True,
            )

    def test_pre_checks_reject_target_with_existing_runs(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        tgt.get_dag.return_value = {"is_paused": True, "dag_run_count": 42}
        with pytest.raises(RuntimeError, match="already has runs"):
            migrate_dag_history(
                source_hook=src, target_hook=tgt, target_dag_id="d1",
                pause_dag_in_source=False, pre_checks=True,
            )

    def test_migrate_ti_history_fetches_and_writes(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        src, tgt = _mk_hook(), _mk_hook()
        src.get_task_instance_history.return_value = {
            "task_instances": [{"task_id": "t1", "try_number": 1}]
        }
        result = migrate_dag_history(
            source_hook=src, target_hook=tgt, target_dag_id="d1",
            pause_dag_in_source=False, migrate_ti_history=True,
        )
        src.get_task_instance_history.assert_called_once()
        tgt.set_task_instance_history.assert_called_once()
        assert result["task_instance_history_migrated"] == 1

    def test_on_step_callback_invoked(self):
        from astronomer_starship.providers.starship.operators.starship import migrate_dag_history

        steps = []
        migrate_dag_history(
            source_hook=_mk_hook(), target_hook=_mk_hook(), target_dag_id="d1",
            pause_dag_in_source=False, on_step=steps.append,
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
                source_hook=_mk_hook(), target_hook=_mk_hook(), target_dag_id="d1",
                pause_dag_in_source=False, check_abort=_raise,
            )


class TestPauseUnpauseDag:
    def test_pause_source_only(self):
        from astronomer_starship.providers.starship.operators.starship import pause_unpause_dag

        src, tgt = MagicMock(), MagicMock()
        r = pause_unpause_dag(
            dag_id="d1", source_hook=src, target_hook=tgt,
            pause_in_source=True, unpause_in_target=False,
        )
        assert r == {"source_paused_by_us": True, "target_unpaused_by_us": False}
        src.set_dag_is_paused.assert_called_once_with(dag_id="d1", is_paused=True)
        tgt.set_dag_is_paused.assert_not_called()

    def test_unpause_target_only(self):
        from astronomer_starship.providers.starship.operators.starship import pause_unpause_dag

        src, tgt = MagicMock(), MagicMock()
        r = pause_unpause_dag(
            dag_id="d1", source_hook=src, target_hook=tgt,
            pause_in_source=False, unpause_in_target=True,
        )
        assert r == {"source_paused_by_us": False, "target_unpaused_by_us": True}
        tgt.set_dag_is_paused.assert_called_once_with(dag_id="d1", is_paused=False)
        src.set_dag_is_paused.assert_not_called()

    def test_neither(self):
        from astronomer_starship.providers.starship.operators.starship import pause_unpause_dag

        src, tgt = MagicMock(), MagicMock()
        r = pause_unpause_dag(
            dag_id="d1", source_hook=src, target_hook=tgt,
            pause_in_source=False, unpause_in_target=False,
        )
        assert r == {"source_paused_by_us": False, "target_unpaused_by_us": False}
        src.set_dag_is_paused.assert_not_called()
        tgt.set_dag_is_paused.assert_not_called()


# ---------------------------------------------------------------------------
# Wave engine: state, abort, patterns, orchestration helpers
# ---------------------------------------------------------------------------


class FakeVariable:
    """Minimal stand-in for airflow.models.Variable that keeps state in a dict."""

    def __init__(self):
        self.store: dict = {}

    def get(self, key, default_var=None):
        return self.store.get(key, default_var)

    def set(self, key, value):
        self.store[key] = value


@pytest.fixture
def fake_variable():
    """Patch ``Variable`` inside the cutover module with an in-memory impl."""
    from astronomer_starship.providers.starship import cutover as _cutover

    fv = FakeVariable()
    with patch.object(_cutover, "Variable", new=fv):
        # Also reset the in-memory caches so tests don't leak state.
        _cutover._abort_events.clear()
        _cutover._abort_check_cache.clear()
        _cutover._step_labels.clear()
        yield fv


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

    def test_corrupt_state_resets_gracefully(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        fake_variable.store[_cutover.VARIABLE_KEY] = "{not-json"
        assert _cutover.get_state() == {"migrations": []}

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
        from astronomer_starship.providers.starship.cutover import start_wave

        with pytest.raises(ValueError):
            start_wave(strategy="bogus", patterns=[])

    def test_incremental_requires_patterns(self, fake_variable):
        from astronomer_starship.providers.starship.cutover import start_wave

        with pytest.raises(ValueError, match="at least one"):
            start_wave(strategy="incremental", patterns=[])


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
        with pytest.raises(ValueError, match="not in a failed/skipped"):
            _cutover.retry_dags_in_wave(mid, "d1")

    def test_retry_unknown_dag_rejected(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        mid = _cutover.create_migration("incremental", {}, ["d1"])
        with pytest.raises(ValueError, match="not in wave"):
            _cutover.retry_dags_in_wave(mid, "nope")

    def test_retry_nonexistent_wave_rejected(self, fake_variable):
        from astronomer_starship.providers.starship import cutover as _cutover

        with pytest.raises(ValueError, match="not found"):
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
