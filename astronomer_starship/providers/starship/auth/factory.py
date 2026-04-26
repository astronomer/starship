"""Factory that resolves a source Airflow connection to an auth class + hook.

Dispatch keys off Airflow's standard ``conn_type``:

- ``http`` → bearer-in-password (Astro, OSS bearer) or HTTP Basic when both
  ``login`` and ``password`` are set. HTTP Basic is handled natively by
  ``HttpHook`` (we return ``None``).
- ``google_cloud_platform`` → GCP Application Default Credentials, with
  optional ``impersonation_chain`` in extras (same convention as
  ``GoogleBaseHook``).
- ``aws`` → MWAA web-login token via boto3, reading ``region_name`` and
  ``environment_name`` from extras (same convention as ``AwsBaseHook``).
"""

import json
from enum import Enum
from typing import Optional, Type

import requests

from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook


class SourceConnType(str, Enum):
    """Airflow ``conn_type`` values supported as Starship sources."""

    HTTP = "http"
    GCP = "google_cloud_platform"
    AWS = "aws"


class BearerTokenAuth(requests.auth.AuthBase):
    """Static bearer-token auth for ``conn_type=http`` sources.

    Used for any HTTP source where the token is stored in the connection's
    ``password`` field — e.g. Astro Organization / Workspace / Personal /
    Deployment access tokens, or any OSS Airflow behind a bearer proxy.

    The ``login`` parameter is accepted but ignored: Airflow's ``HttpHook``
    instantiates the auth class as ``auth_type(conn.login, conn.password)``,
    so the signature has to accommodate both.
    """

    def __init__(self, login: Optional[str] = None, password: Optional[str] = None) -> None:
        self.token = password

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        if not self.token:
            raise RuntimeError("HTTP source connection has no token configured.")
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


def _load_extras(conn) -> dict:
    raw = getattr(conn, "extra", None)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def _assert_required_fields(conn_id: str, conn_type: SourceConnType, conn, extras: dict) -> None:
    """Fail fast with a clear, user-actionable message if the saved Connection
    is missing fields this conn_type needs. Runs before we open an HTTP session,
    so the caller gets a proper error instead of a cryptic auth-time exception.
    """
    if conn_type is SourceConnType.HTTP:
        if not conn.password and not conn.login:
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' has neither a password (token) nor a "
                f"login+password set. Provide credentials via the Connections UI."
            )
    elif conn_type is SourceConnType.AWS:
        if not extras.get("region_name"):
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' (MWAA) has no `region_name` in its "
                f"extras. Add it via the Connections UI."
            )
        if not extras.get("environment_name"):
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' (MWAA) has no `environment_name` in "
                f"its extras. Add it via the Connections UI."
            )
    # GCP uses ADC — nothing to check at setup time; failures surface on
    # first token refresh.


def resolve_source_auth(conn_id: str) -> Optional[Type[requests.auth.AuthBase]]:
    """Return the ``requests.auth.AuthBase`` subclass to use for a source conn.

    Returns ``None`` for the HTTP Basic case, where Airflow's ``HttpHook``
    handles authentication natively from the connection's login/password.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    extras = _load_extras(conn)

    try:
        conn_type = SourceConnType((conn.conn_type or "").lower())
    except ValueError as err:
        supported = ", ".join(t.value for t in SourceConnType)
        raise RuntimeError(
            f"Unsupported conn_type '{conn.conn_type}' on connection '{conn_id}'. Expected one of: {supported}."
        ) from err

    _assert_required_fields(conn_id, conn_type, conn, extras)

    if conn_type is SourceConnType.HTTP:
        if conn.login and conn.password:
            return None  # HttpHook does HTTP Basic natively.
        return BearerTokenAuth

    if conn_type is SourceConnType.GCP:
        from astronomer_starship.providers.starship.auth.gcc import (
            ComposerV2BearerAuth,
            make_impersonated_auth,
        )

        chain = extras.get("impersonation_chain") or []
        if chain:
            return make_impersonated_auth(chain)
        return ComposerV2BearerAuth

    if conn_type is SourceConnType.AWS:
        from astronomer_starship.providers.starship.auth.mwaa import make_mwaa_auth

        return make_mwaa_auth(
            region=extras.get("region_name"),
            environment_name=extras.get("environment_name"),
            role_arn=extras.get("role_arn"),
        )

    # Unreachable — SourceConnType(...) above has already validated the value.
    raise RuntimeError(f"Unhandled SourceConnType '{conn_type}'.")


def resolve_source_hook(conn_id: str) -> StarshipHttpHook:
    """Construct a :class:`StarshipHttpHook` bound to ``conn_id``.

    The source_connection endpoint stores the full base URL (including
    any deployment-path segment) in ``conn.host``, so the hook picks up
    the correct ``base_url`` automatically via Airflow's native
    ``HttpHook`` behaviour — no endpoint-prefix juggling needed here.
    """
    auth_type = resolve_source_auth(conn_id)
    kwargs = {"http_conn_id": conn_id}
    if auth_type is not None:
        kwargs["auth_type"] = auth_type
    return StarshipHttpHook(**kwargs)
