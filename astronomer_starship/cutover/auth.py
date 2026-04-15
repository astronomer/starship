"""
Auth for connecting to the source Airflow instance via HTTP.

Supports three source types:
- Astro deployment: Set password field on the HTTP connection to the Deployment API Key.
- Composer 2 (GCP ADC): Leave login/password empty, uses Application Default Credentials.
- Composer 2 with impersonation: Set Extra JSON to:
    {"impersonation_chain": ["target-sa@project.iam.gserviceaccount.com"]}

The get_source_hook() factory auto-detects the auth type based on the connection fields.
"""

import logging
import threading

import requests

from astronomer_starship.providers.starship.hooks.starship import StarshipHttpHook

logger = logging.getLogger(__name__)

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# Standard HTTP headers that should be kept on the session.
# Everything else (connection Extra keys like impersonation_chain) is stripped.
_STANDARD_HEADERS = {
    "accept",
    "accept-encoding",
    "accept-language",
    "authorization",
    "cache-control",
    "connection",
    "content-type",
    "cookie",
    "host",
    "origin",
    "pragma",
    "referer",
    "user-agent",
}

# Cached GCP default credentials — avoids hitting the metadata server on
# every hook instantiation when running parallel workers.
_default_creds = None
_default_creds_lock = threading.Lock()


def _get_default_credentials():
    """Return cached GCP default credentials, creating them on first call.

    Always requests cloud-platform scope so the cached creds work for both
    direct Composer V2 auth and as the source for impersonated credentials.
    """
    global _default_creds
    if _default_creds is None:
        with _default_creds_lock:
            if _default_creds is None:
                import google.auth

                creds, _ = google.auth.default(scopes=[AUTH_SCOPE])
                _default_creds = creds
    return _default_creds


class AstroBearerAuth(requests.auth.AuthBase):
    """Bearer token auth using a static Astro Deployment API key."""

    def __init__(self, login=None, password=None):
        self.token = password

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


class ComposerV2BearerAuth(requests.auth.AuthBase):
    """Bearer token auth using GCP Application Default Credentials."""

    def __init__(self, login=None, password=None):
        self._creds = _get_default_credentials()

    def __call__(self, r):
        from google.auth import _helpers
        from google.auth.transport.requests import Request

        if not self._creds.valid:
            self._creds.refresh(Request())
        r.headers["Authorization"] = f"Bearer {_helpers.from_bytes(self._creds.token)}"
        return r


def _make_impersonated_auth(impersonation_chain: list):
    """Return an auth class that uses GCP impersonated credentials.

    The last entry in the chain is the target principal.
    Any preceding entries are delegates (intermediate SAs).
    """
    target_principal = impersonation_chain[-1]
    delegates = impersonation_chain[:-1] if len(impersonation_chain) > 1 else []

    class ImpersonatedComposerAuth(requests.auth.AuthBase):
        def __init__(self, login=None, password=None):
            from google.auth import impersonated_credentials

            source_creds = _get_default_credentials()
            self._creds = impersonated_credentials.Credentials(
                source_credentials=source_creds,
                target_principal=target_principal,
                target_scopes=[AUTH_SCOPE],
                delegates=delegates,
            )

        def __call__(self, r):
            from google.auth import _helpers
            from google.auth.transport.requests import Request

            if not self._creds.valid:
                self._creds.refresh(Request())
            r.headers["Authorization"] = f"Bearer {_helpers.from_bytes(self._creds.token)}"
            return r

    return ImpersonatedComposerAuth


class CutoverHttpHook(StarshipHttpHook):
    """StarshipHttpHook with auth support and header cleanup.

    Overrides get_conn() to strip non-standard headers (like impersonation_chain)
    that Airflow's HttpHook injects from connection extras.
    """

    def get_conn(self, headers=None):
        session = super().get_conn(headers=headers)
        extra_keys = [k for k in session.headers if k.lower() not in _STANDARD_HEADERS]
        for key in extra_keys:
            session.headers.pop(key, None)
        return session


def get_source_hook(conn_id: str) -> CutoverHttpHook:
    """Create a CutoverHttpHook with auto-detected auth.

    - If the connection has a password -> AstroBearerAuth (Astro API key)
    - If impersonation_chain is set in extras -> ImpersonatedComposerAuth
    - Otherwise -> ComposerV2BearerAuth (direct GCP ADC)
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)

    if conn.password:
        auth_type = AstroBearerAuth
    else:
        extras = conn.extra_dejson
        chain = extras.get("impersonation_chain", [])
        if chain:
            auth_type = _make_impersonated_auth(chain)
        else:
            auth_type = ComposerV2BearerAuth

    return CutoverHttpHook(http_conn_id=conn_id, auth_type=auth_type)
