"""OSS Airflow auth.

Two flavours, both natively handled by Airflow's ``HttpHook``:

- **Bearer token**: connection ``password`` set, no ``login``. Resolved via
  :class:`OssBearerAuth`.
- **HTTP Basic**: connection ``login`` + ``password`` set. Handled by
  ``HttpHook`` itself without any custom auth class (we return ``None``
  from :func:`resolve_oss_auth` so the hook falls back to its default).
"""

from typing import Optional

import requests


class OssBearerAuth(requests.auth.AuthBase):
    """Static bearer-token auth, reading the token from the connection password."""

    def __init__(self, login=None, password=None):
        self.token = password

    def __call__(self, r):
        if not self.token:
            raise RuntimeError("OSS bearer source has no token configured.")
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


def resolve_oss_auth(login: Optional[str], password: Optional[str]):
    """Pick the OSS auth class based on available credential fields.

    Returns ``None`` for HTTP Basic so ``HttpHook`` handles it natively;
    returns :class:`OssBearerAuth` when only a token is set.
    """
    if login and password:
        return None  # HttpHook will do HTTP Basic with login + password.
    if password:
        return OssBearerAuth
    raise RuntimeError("OSS source requires a password (token) or login+password.")
