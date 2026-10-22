import pytest

from astronomer_starship.common import HttpError, validate_proxy_url


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://[fd00:ec2::254]/latest/meta-data/",
        "http://[::ffff:169.254.169.254]/",
        "http://100.100.100.200/latest/meta-data/",
        "http://127.0.0.1:8080/api/v1/health",
        "http://localhost:8080/api/v1/health",
        "http://[::1]/",
        "http://0.0.0.0/",
        "http://168.63.129.16/machine?comp=goalstate",
        "http://[fec0::1]/",
    ],
)
def test_validate_proxy_url_blocks_internal_hosts(url):
    with pytest.raises(HttpError) as e:
        validate_proxy_url(url)
    assert e.value.status_code == 403


@pytest.mark.parametrize("url", ["file:///etc/passwd", "gopher://example.com/", "example.com", "http://:80/"])
def test_validate_proxy_url_requires_http_url(url):
    with pytest.raises(HttpError) as e:
        validate_proxy_url(url)
    assert e.value.status_code == 400


@pytest.mark.parametrize("url", ["https://93.184.215.14/api/v1/health", "http://10.0.0.5:8080/api/v2/monitor/health"])
def test_validate_proxy_url_allows_public_and_private_hosts(url):
    validate_proxy_url(url)


def test_af3_proxy_requires_authentication():
    pytest.importorskip("airflow.api_fastapi")
    from fastapi.testclient import TestClient

    from astronomer_starship._af3.starship import starship_app

    res = TestClient(starship_app).get(
        "/proxy",
        params={"url": "http://169.254.169.254/latest/meta-data/"},
        headers={"Starship-Proxy-Token": "anything"},
    )
    assert res.status_code == 401, res.text
