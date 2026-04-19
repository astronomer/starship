"""Astro Deployment API-token auth for source Airflow connections."""

import requests


class AstroBearerAuth(requests.auth.AuthBase):
    """Bearer-token auth backed by a static Astro Deployment API key.

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
