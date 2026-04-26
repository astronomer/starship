"""Google Cloud Composer auth via Application Default Credentials (ADC).

Supports direct ADC and optional service-account impersonation. GCP SDK
imports are deferred so non-GCC installs don't need ``google-auth``.
"""

import threading
from typing import List, Type

import requests

GCP_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


# Cached default credentials shared by workers in the same process.
_default_creds = None
_default_creds_lock = threading.Lock()


def _get_default_credentials():
    """Return cached GCP default credentials, creating them on first call."""
    global _default_creds
    if _default_creds is None:
        with _default_creds_lock:
            if _default_creds is None:
                import google.auth

                creds, _ = google.auth.default(scopes=[GCP_CLOUD_PLATFORM_SCOPE])
                _default_creds = creds
    return _default_creds


def _refresh_and_authorize(creds, request: requests.PreparedRequest) -> requests.PreparedRequest:
    """Refresh the GCP credentials if needed and set the Authorization header."""
    from google.auth import _helpers
    from google.auth.transport.requests import Request

    if not creds.valid:
        creds.refresh(Request())
    request.headers["Authorization"] = f"Bearer {_helpers.from_bytes(creds.token)}"
    return request


class ComposerV2BearerAuth(requests.auth.AuthBase):
    """Bearer-token auth using GCP Application Default Credentials."""

    def __init__(self) -> None:
        self._creds = _get_default_credentials()

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        return _refresh_and_authorize(self._creds, r)


def make_impersonated_auth(impersonation_chain: List[str]) -> Type[requests.auth.AuthBase]:
    """Return an auth class that mints tokens via GCP impersonated credentials.

    The last entry in ``impersonation_chain`` is the target principal. Any
    preceding entries are delegates (intermediate service accounts).
    """
    target_principal = impersonation_chain[-1]
    delegates = impersonation_chain[:-1] if len(impersonation_chain) > 1 else []

    class ImpersonatedComposerAuth(requests.auth.AuthBase):
        def __init__(self) -> None:
            from google.auth import impersonated_credentials

            self._creds = impersonated_credentials.Credentials(
                source_credentials=_get_default_credentials(),
                target_principal=target_principal,
                target_scopes=[GCP_CLOUD_PLATFORM_SCOPE],
                delegates=delegates,
            )

        def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
            return _refresh_and_authorize(self._creds, r)

    return ImpersonatedComposerAuth
