import logging

import requests
from airflow.plugins_manager import AirflowPlugin

# INCOMPATIBILITY: Airflow 1.10.15
# File "/home/airflow/.local/lib/python3.6/site-packages/astronomer_starship/starship.py", line 9, in <module>
#   from airflow.security import permissions
# ImportError: cannot import name 'permissions'
from airflow.security import permissions
from airflow.www import auth
from airflow.www.app import csrf
from flask import Blueprint, Response, request
from flask_appbuilder import BaseView, expose

logger = logging.getLogger(__name__)

ALLOWED_PROXY_METHODS = ["GET", "POST", "PATCH", "DELETE"]


class Starship(BaseView):
    default_view = "main"

    @expose("/")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def main(self):
        """Main view - just bootstraps the React app."""
        return self.render_template("index.html")

    @expose("/proxy", methods=ALLOWED_PROXY_METHODS)
    @csrf.exempt
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def proxy(self):
        """Proxy for the React app to use to access the Airflow API."""
        request_method = request.method
        if request_method not in ALLOWED_PROXY_METHODS:
            return Response("Method not in " + ", ".join(ALLOWED_PROXY_METHODS), status=405)

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
            return Response("No Token Provided", status=400)
        request_headers = dict(
            Authorization=f"Bearer {token}",
            **({"Content-type": "application/json"} if request.data else {}),
        )

        url = request.args.get("url")
        if not url:
            return Response("No URL Provided", status=400)

        response = requests.request(
            request_method,
            url,
            headers=request_headers,
            params=request.args if request.args else None,
            **({"data": request.data} if request.data else {}),
        )
        response_headers = dict(response.headers)
        if not response.ok:
            logger.error("Proxied request to %s failed with status %s: %s", url, response.status_code, response.content)

        if logger.isEnabledFor(logging.DEBUG):
            logged_request_headers = {
                k: "<redacted>" if k.lower() == "authorization" else v for k, v in request_headers.items()
            }
            logger.debug(
                "url=%s request_method=%s request_headers=%s request.data=%s "
                "response_headers=%s response.status_code=%s response.content=%s",
                url,
                request_method,
                logged_request_headers,
                request.data,
                response_headers,
                response.status_code,
                response.content,
            )
        response_headers["Starship-Proxy-Status"] = "OK"
        return Response(response.content, status=response.status_code, headers=response_headers)


starship_view = Starship()
starship_bp = Blueprint(
    "starship",
    __name__,
    static_folder="../static",  # should be default, just being explicit
    template_folder="../templates",  # should be default, just being explicit
    static_url_path="/starship/static",  # so static/foo.html is at /starship/static/foo.html
)


class StarshipPlugin(AirflowPlugin):
    name = "starship"
    flask_blueprints = [starship_bp]
    appbuilder_views = [
        {
            "name": "Migration Tool 🚀 Starship",
            "category": "Astronomer",
            "view": starship_view,
        }
    ]
