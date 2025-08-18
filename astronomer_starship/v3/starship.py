import os
import requests
from pathlib import Path
from airflow import __version__
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


ALLOWED_PROXY_METHODS = ["GET", "POST", "PATCH", "DELETE"]

# API
app = FastAPI()

# STATIC & TEMPLATES
app.mount(
    "/static",
    # serve files in "<this folder>/src" without any modification (js, html, css, images, etc.)
    StaticFiles(directory=Path(__file__).parent.parent / "static"),
    name="static",
)

# serve files in "<this folder>/templates" and render any {{ jinja }} inside the files
templates = Jinja2Templates(directory=Path(__file__).parent.parent / "templates")


# STATIC ROUTES
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the index page."""
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "airflow_version": __version__,
        },
    )


# API ROUTES
@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "OK"}


@app.api_route("/proxy", methods=ALLOWED_PROXY_METHODS)
async def proxy(request: Request):
    """Proxy for the React app to use to access the Airflow API."""
    request_method = request.method
    if request_method not in ALLOWED_PROXY_METHODS:
        return Response(
            "Method not in " + ", ".join(ALLOWED_PROXY_METHODS), status_code=405
        )
    request_headers = dict(request.headers)
    token = (
        request_headers.get("Starship-Proxy-Token")
        or request_headers.get("STARSHIP-PROXY-TOKEN")
        or request_headers.get("starship-proxy-token")
        or request_headers.get("Starship_Proxy_Token")
        or request_headers.get("STARSHIP_PROXY_TOKEN")
        or request_headers.get("starship_proxy_token")
    )
    if not token:
        return Response("No token provided", status_code=400)
    body = await request.body()
    request_headers = dict(
        Authorization=f"Bearer {token}",
        **({"Content-type": "application/json"} if body else {}),
    )

    url = request.query_params.get("url")
    if not url:
        return Response("No URL provided", status_code=400)

    response = requests.request(
        request_method,
        url,
        headers=request_headers,
        params=request.query_params if request.query_params else None,
        **({"data": body} if body else {}),
    )
    response_headers = dict(response.headers)
    if not response.ok:
        print(response.content)

    if os.getenv("DEBUG", False):
        print(
            f"url={url}\n"
            f"request_method={request_method}\n"
            f"request_headers={request_headers}\n"
            f"request.body={body}\n"
            "==========="
            f"response_headers={response_headers}\n"
            f"response.status_code={response.status_code}\n"
            f"response.content={response.content}\n"
        )

    # runs into issues with ERR_CONTENT_DECODING_FAILED on Airflow 3 deployments
    # axios.get does not return a valid response
    encoding_headers = [
        "content-encoding",
        "transfer-encoding",
    ]
    response_headers = {
        k: v for k, v in response.headers.items() if k.lower() not in encoding_headers
    }
    response_headers["Starship-Proxy-Status"] = "OK"
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=response_headers,
    )


app_with_metadata = {"app": app, "url_prefix": "/starship", "name": "starship"}


class StarshipPlugin(AirflowPlugin):
    name = "starship"
    fastapi_apps = [app_with_metadata]
