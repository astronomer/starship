"""Unit tests for the source-connection helper in ``common``.

The endpoint that wraps this helper is exercised end-to-end by the
existing _af2/_af3 connection-CRUD tests; here we focus on the
payload-to-Airflow-Connection mapping, which contains all the
platform-specific quirks (Astro deployment-slug paths, MWAA region
required, OSS basic-vs-bearer, etc.).
"""

import json

import pytest

from astronomer_starship.common import (
    STARSHIP_SOURCE_CONN_ID,
    SUPPORTED_SOURCE_PLATFORMS,
    HttpError,
    build_source_connection_kwargs,
)


class TestBuildSourceConnectionKwargs:
    def test_astro_maps_token_to_password(self):
        k = build_source_connection_kwargs(
            {"platform": "astro", "url": "https://foo.astronomer.run/bar", "token": "tok"}
        )
        assert k["conn_id"] == STARSHIP_SOURCE_CONN_ID
        assert k["conn_type"] == "http"
        # Full URL lives in host so HttpHook uses it verbatim (covers the
        # Astro deployment-slug path segment).
        assert k["host"] == "https://foo.astronomer.run/bar"
        assert k["schema"] == "https"
        assert k["password"] == "tok"  # pragma: allowlist secret

    def test_gcc_impersonation_chain_lands_in_extras(self):
        k = build_source_connection_kwargs(
            {
                "platform": "gcc",
                "url": "https://composer.example.com/",
                "impersonation_chain": ["a@x.iam", "b@x.iam"],
            }
        )
        assert k["conn_type"] == "google_cloud_platform"
        extras = json.loads(k["extra"])
        assert extras["impersonation_chain"] == ["a@x.iam", "b@x.iam"]
        assert "password" not in k

    def test_gcc_rejects_non_list_impersonation_chain(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs(
                {
                    "platform": "gcc",
                    "url": "https://composer.example.com/",
                    "impersonation_chain": "single@x.iam",
                }
            )
        assert exc.value.status_code == 400

    def test_mwaa_requires_region(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs({"platform": "mwaa", "url": "https://mwaa.example.com/"})
        assert exc.value.status_code == 400
        assert "region" in exc.value.msg.lower()

    def test_mwaa_carries_region_env_and_role(self):
        k = build_source_connection_kwargs(
            {
                "platform": "mwaa",
                "url": "https://mwaa.example.com/",
                "region": "us-west-2",
                "environment_name": "my-env",
                "role_arn": "arn:aws:iam::1:role/StarshipSource",
            }
        )
        assert k["conn_type"] == "aws"
        extras = json.loads(k["extra"])
        assert extras["region_name"] == "us-west-2"
        assert extras["environment_name"] == "my-env"
        assert extras["role_arn"] == "arn:aws:iam::1:role/StarshipSource"

    def test_oss_basic_passes_login_password(self):
        k = build_source_connection_kwargs(
            {
                "platform": "oss",
                "url": "https://airflow.example.com:8080/base",
                "login": "u",
                "password": "p",
            }
        )
        assert k["conn_type"] == "http"
        assert k["login"] == "u"
        assert k["password"] == "p"
        assert k["port"] == 8080
        # Full base URL lives in `host` so HttpHook uses it verbatim as
        # the base URL, preserving the /base deployment-path segment.
        assert k["host"] == "https://airflow.example.com:8080/base"

    def test_astro_style_path_segment_is_preserved_in_host(self):
        """Regression test for the Astro deployment-slug bug: the user's
        URL has ``/<slug>`` as a path; we must keep it in ``host`` so
        plugin-endpoint calls hit ``.../<slug>/api/starship/...`` rather
        than the bare hostname (which Astro's edge proxy rejects)."""
        k = build_source_connection_kwargs(
            {
                "platform": "astro",
                "url": "https://abc123.astronomer.run/slug-xyz",
                "token": "tok",
            }
        )
        assert k["host"] == "https://abc123.astronomer.run/slug-xyz"
        # extras.endpoint is no longer used — path lives in host.
        extras = json.loads(k["extra"])
        assert "endpoint" not in extras

    def test_url_without_path_still_works(self):
        """Hostnames without a path (MWAA, some OSS installs) must still
        produce a valid base URL — ``https://host`` with no trailing path."""
        k = build_source_connection_kwargs(
            {"platform": "astro", "url": "https://abc123.astronomer.run", "token": "tok"}
        )
        assert k["host"] == "https://abc123.astronomer.run"

    def test_oss_bearer_only(self):
        k = build_source_connection_kwargs({"platform": "oss", "url": "https://airflow.example.com/", "token": "tok"})
        assert k["conn_type"] == "http"
        assert k["password"] == "tok"  # pragma: allowlist secret
        assert "login" not in k

    def test_oss_without_creds_is_rejected(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs({"platform": "oss", "url": "https://airflow.example.com/"})
        assert exc.value.status_code == 400

    def test_astro_without_token_is_rejected(self):
        with pytest.raises(HttpError):
            build_source_connection_kwargs({"platform": "astro", "url": "https://x.astronomer.run/y"})

    def test_invalid_platform_rejected(self):
        with pytest.raises(HttpError) as exc:
            build_source_connection_kwargs({"platform": "bogus", "url": "https://x"})
        assert exc.value.status_code == 400

    def test_missing_url_rejected(self):
        with pytest.raises(HttpError):
            build_source_connection_kwargs({"platform": "astro", "token": "tok"})

    def test_invalid_url_rejected(self):
        with pytest.raises(HttpError):
            build_source_connection_kwargs({"platform": "astro", "url": "not-a-url", "token": "tok"})

    def test_supported_platforms_constant(self):
        # Guardrail: UI constants must stay aligned with the Python side.
        assert set(SUPPORTED_SOURCE_PLATFORMS) == {"astro", "mwaa", "gcc", "oss"}
