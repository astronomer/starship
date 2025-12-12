import gzip
import os
from pathlib import Path
from typing import Optional

import httpx
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse

PLUGIN_DIR = Path(__file__).parent.parent
STATIC_DIR = PLUGIN_DIR / "static"
ASSETS_DIR = STATIC_DIR / "assets"

ALLOWED_PROXY_METHODS = ["GET", "POST", "PATCH", "DELETE", "OPTIONS"]

starship_app = FastAPI(title="Starship", version="1.0.0")


@starship_app.get("/")
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html", media_type="text/html")


@starship_app.get("/assets/{filename:path}")
async def serve_assets(filename: str):
    file_path = ASSETS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        return Response(
            content=f"Static asset not found: {filename}. The requested file does not exist in the Starship assets directory.",
            status_code=404,
        )

    if filename.endswith(".js"):
        media_type = "application/javascript"
    elif filename.endswith(".css"):
        media_type = "text/css"
    else:
        media_type = "application/octet-stream"

    return FileResponse(
        file_path,
        media_type=media_type,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


def _get_proxy_token(request: Request) -> Optional[str]:
    headers = request.headers
    return (
        headers.get("Starship-Proxy-Token")
        or headers.get("STARSHIP-PROXY-TOKEN")
        or headers.get("starship-proxy-token")
        or headers.get("Starship_Proxy_Token")
        or headers.get("STARSHIP_PROXY_TOKEN")
        or headers.get("starship_proxy_token")
    )


@starship_app.api_route("/proxy", methods=ALLOWED_PROXY_METHODS)
async def proxy(request: Request):
    if request.method == "OPTIONS":
        return Response(
            content="",
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "*, Starship-Proxy-Token, Content-Type, Authorization",
                "Access-Control-Max-Age": "600",
            },
        )

    token = _get_proxy_token(request)
    if not token:
        return Response(
            content="No Token Provided",
            status_code=400,
            headers={"Access-Control-Allow-Origin": "*"},
        )

    url = request.query_params.get("url")
    if not url:
        return Response(content="No URL Provided", status_code=400)

    if "/api/v1/health" in url:
        url = url.replace("/api/v1/health", "/api/v2/monitor/health")

    body = await request.body()
    proxy_headers = {"Authorization": f"Bearer {token}"}
    if body:
        proxy_headers["Content-Type"] = "application/json"

    try:
        async with httpx.AsyncClient(verify=False, timeout=30.0) as client:  # noqa: S501  # nosec B501
            response = await client.request(
                method=request.method,
                url=url,
                headers=proxy_headers,
                params=dict(request.query_params) if request.query_params else None,
                content=body if body else None,
            )

        if os.getenv("DEBUG", False):
            print(
                f"url={url}\n"
                f"request_method={request.method}\n"
                f"proxy_headers={proxy_headers}\n"
                f"body={body}\n"
                "===========\n"
                f"response.status_code={response.status_code}\n"
                f"response.content={response.content}\n"
            )

        response_content = response.content
        response_headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true",
            "Starship-Proxy-Status": "OK",
            "Content-Type": response.headers.get("content-type", "application/json"),
        }

        # GZIP compress response if large (>1KB) and client accepts gzip
        accept_encoding = request.headers.get("Accept-Encoding", "")
        if len(response_content) > 1024 and "gzip" in accept_encoding.lower():
            response_content = gzip.compress(response_content)
            response_headers["Content-Encoding"] = "gzip"
            response_headers["X-Uncompressed-Size"] = str(len(response.content))

        return Response(
            content=response_content,
            status_code=response.status_code,
            headers=response_headers,
        )

    except httpx.RequestError as e:
        return Response(
            content=f"Proxy request failed: {str(e)}",
            status_code=502,
        )


class StarshipPlugin(AirflowPlugin):
    name = "starship"

    fastapi_apps = [
        {
            "app": starship_app,
            "url_prefix": "/starship",
            "name": "Starship Static",
        }
    ]

    external_views = [
        {
            "name": "Migration Tool Starship",
            "href": "starship/",
            "destination": "nav",
            "category": "Admin",
            "url_route": "starship",
        }
    ]
