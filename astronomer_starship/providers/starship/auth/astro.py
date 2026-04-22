"""Astro access-token auth for source Airflow connections.

Works with **Organization**, **Workspace**, and **Personal** access tokens.
Deployment API Tokens are *not* supported — Astro's edge proxy (Istio)
rejects them on Starship plugin endpoints with a 401. This matches the
Target Setup flow, which has always used Org/Workspace/Personal tokens.
"""

import requests


class AstroBearerAuth(requests.auth.AuthBase):
    """Static bearer-token auth for an Astro source.

    The token is read from the HTTP Connection's ``password`` field by
    Airflow's ``HttpHook`` before this auth object is constructed.
    """

    def __init__(self, login=None, password=None):
        self.token = password

    def __call__(self, r):
        if not self.token:
            raise RuntimeError("Astro source connection has no token configured.")
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r
