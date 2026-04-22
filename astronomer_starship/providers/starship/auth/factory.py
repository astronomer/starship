"""Factory that resolves a source Airflow connection to an auth class + hook.

The source connection is created by the Setup page (``POST
/api/starship/source_connection``) which stores a ``starship_platform`` hint
in the connection's ``extra`` JSON. This module reads that hint and picks
the appropriate auth factory from the sibling modules.
"""

import json

from astronomer_starship.providers.starship.auth.astro import AstroBearerAuth
from astronomer_starship.providers.starship.auth.oss import resolve_oss_auth
from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook


def _load_extras(conn) -> dict:
    raw = getattr(conn, "extra", None)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def _assert_required_fields(conn_id: str, platform: str, conn, extras: dict) -> None:
    """Fail fast with a clear, user-actionable message if the saved Connection
    is missing fields this platform needs. Runs before we open an HTTP session,
    so the caller gets a proper error instead of a cryptic auth-time exception.
    """
    if platform == "astro":
        if not conn.password:
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' is configured as an Astro source but has no password set. "
                f"Re-open the Starship Source Setup page and save the connection with a Deployment API token."
            )
    elif platform == "oss":
        if not conn.password:
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' is configured as an OSS source but has no password set. "
                f"Provide either a bearer token or a login + password via the Starship Source Setup page."
            )
    elif platform == "mwaa":
        if not extras.get("region_name"):
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' is configured as an MWAA source but has no `region_name` "
                f"in its extras. Re-save via the Starship Source Setup page with an AWS region."
            )
        if not extras.get("environment_name"):
            raise RuntimeError(
                f"Airflow Connection '{conn_id}' is configured as an MWAA source but has no `environment_name` "
                f"in its extras. Re-save via the Starship Source Setup page with the MWAA environment name."
            )
    # gcc uses ADC — nothing to check at setup time; failures surface on first token refresh.


def resolve_source_auth(conn_id: str):
    """Return the ``requests.auth.AuthBase`` subclass to use for a source conn.

    Returns ``None`` for the OSS-Basic case, where Airflow's ``HttpHook``
    handles authentication natively from the connection's login/password.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    extras = _load_extras(conn)
    platform = extras.get("starship_platform")

    # Back-compat fallback when the platform hint isn't set: look at what
    # fields are populated and infer the closest match.
    if not platform:
        if conn.password and not conn.login:
            return AstroBearerAuth
        return resolve_oss_auth(conn.login, conn.password)

    _assert_required_fields(conn_id, platform, conn, extras)

    if platform == "astro":
        return AstroBearerAuth

    if platform == "oss":
        return resolve_oss_auth(conn.login, conn.password)

    if platform == "gcc":
        from astronomer_starship.providers.starship.auth.gcc import (
            ComposerV2BearerAuth,
            make_impersonated_auth,
        )

        chain = extras.get("impersonation_chain") or []
        if chain:
            return make_impersonated_auth(chain)
        return ComposerV2BearerAuth

    if platform == "mwaa":
        from astronomer_starship.providers.starship.auth.mwaa import make_mwaa_auth

        return make_mwaa_auth(
            region=extras.get("region_name"),
            environment_name=extras.get("environment_name"),
            role_arn=extras.get("role_arn"),
        )

    raise RuntimeError(f"Unsupported starship_platform '{platform}' on connection '{conn_id}'.")


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
