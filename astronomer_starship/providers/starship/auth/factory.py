"""Factory that resolves a source Airflow connection to an auth class + hook.

The source connection is created by the Setup page (``POST
/api/starship/source_connection``) which stores a ``starship_platform`` hint
in the connection's ``extra`` JSON. This module reads that hint and picks
the appropriate auth factory from the sibling modules.
"""

import json
from typing import Optional

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


def resolve_source_hook(conn_id: str, endpoint: Optional[str] = None) -> StarshipHttpHook:
    """Construct a :class:`StarshipHttpHook` bound to ``conn_id``.

    If the connection's ``extra`` carries an ``endpoint`` key (set by the
    source_connection endpoint when the URL has a path), it's forwarded to
    the hook.
    """
    auth_type = resolve_source_auth(conn_id)
    kwargs = {"http_conn_id": conn_id}
    if auth_type is not None:
        kwargs["auth_type"] = auth_type
    hook = StarshipHttpHook(**kwargs)

    if endpoint is None:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        extras = _load_extras(conn)
        endpoint = extras.get("endpoint")
    if endpoint:
        # ``base_url`` is not set at construction time on HttpHook — callers
        # that need path-aware URL building should pass ``endpoint`` to
        # ``url_from_endpoint`` themselves; we just annotate the hook for
        # convenience of downstream consumers.
        hook.starship_endpoint_prefix = endpoint
    return hook
