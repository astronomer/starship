"""Auth factories for connecting to a source Airflow instance.

The Cutover Tool saves source credentials as an Airflow HTTP Connection
(``conn_id="starship_source"``) via the Starship API. The connection's
``extra`` JSON carries a ``starship_platform`` hint — ``astro``, ``gcc``,
``mwaa``, or ``oss`` — which ``resolve_source_auth`` uses to pick the right
``requests.auth.AuthBase`` subclass.

Cloud SDK imports are lazy so a given platform's dependencies are only
required when that platform is actually used.
"""

from astronomer_starship.providers.starship.auth.factory import (
    resolve_source_auth,
    resolve_source_hook,
)

__all__ = [
    "resolve_source_auth",
    "resolve_source_hook",
]
