"""AWS MWAA auth via boto3 web-login tokens.

MWAA exposes the Airflow webserver behind an authenticated reverse proxy.
To call it programmatically we mint a short-lived web-login token via the
``CreateWebLoginToken`` MWAA API and use it as a bearer. Tokens are
~60 seconds, so we refresh eagerly on each request after expiry.

boto3 is a lazy import — non-AWS installs don't need it.
"""

import threading
import time
from typing import Optional

import requests

# Refresh the token this many seconds before its stated expiry, to avoid
# racing a 401 on long-running requests. MWAA tokens live ~60s total.
_REFRESH_MARGIN_S = 10


def _build_boto3_session(region: str, role_arn: Optional[str]):
    import boto3

    if not role_arn:
        return boto3.Session(region_name=region)

    sts = boto3.client("sts", region_name=region)
    assumed = sts.assume_role(RoleArn=role_arn, RoleSessionName="starship-cutover")
    creds = assumed["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


def make_mwaa_auth(*, region: str, environment_name: str, role_arn: Optional[str] = None):
    """Return an auth class that mints MWAA web-login tokens via boto3."""
    if not environment_name:
        raise RuntimeError("MWAA auth requires `environment_name` in the connection extras.")
    if not region:
        raise RuntimeError("MWAA auth requires `region_name` in the connection extras.")

    class MwaaWebTokenAuth(requests.auth.AuthBase):
        _lock = threading.Lock()
        _token: Optional[str] = None
        _expires_at: float = 0.0

        def __init__(self, login=None, password=None):
            self._session = _build_boto3_session(region, role_arn)

        def _mint_token(self) -> str:
            client = self._session.client("mwaa", region_name=region)
            resp = client.create_web_login_token(Name=environment_name)
            # MWAA returns a token valid for ~60 seconds.
            return resp["WebToken"]

        def __call__(self, r):
            with MwaaWebTokenAuth._lock:
                now = time.time()
                if not MwaaWebTokenAuth._token or now >= MwaaWebTokenAuth._expires_at:
                    MwaaWebTokenAuth._token = self._mint_token()
                    MwaaWebTokenAuth._expires_at = now + 60 - _REFRESH_MARGIN_S
                token = MwaaWebTokenAuth._token
            r.headers["Authorization"] = f"Bearer {token}"
            return r

    return MwaaWebTokenAuth
