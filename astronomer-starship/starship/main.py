from urllib.parse import urlparse

from cachetools.func import ttl_cache
from airflow.plugins_manager import AirflowPlugin
from airflow import models

from airflow.www import auth
from airflow.security import permissions

import jwt

from flask import Blueprint, session, request, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from starship.services.astrohub_client import AstroClient
from starship.services.local_client import LocalAirflowClient

import requests

from pydash import at

from python_graphql_client import GraphqlClient

import datetime
import json
import socket
import urllib.error
from contextlib import redirect_stderr, redirect_stdout

import requests
from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from astronomer.aeroscope.util import clean_airflow_report_output
from flask import Blueprint, Response, flash, redirect, request
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from wtforms import Form, StringField, validators
from typing import Any, Dict, List, Union

import base64
import json
import logging
from json import JSONDecodeError


def clean_airflow_report_output(log_string: str) -> Union[dict, str]:
    r"""Look for the magic string from the Airflow report and then decode the base64 and convert to json
    Or return output as a list, trimmed and split on newlines
    >>> clean_airflow_report_output('INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\naGVsbG8gd29ybGQ=')
    'hello world'
    >>> clean_airflow_report_output('INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\neyJvdXRwdXQiOiAiaGVsbG8gd29ybGQifQ==')
    {'output': 'hello world'}
    """

    def get_json_or_clean_str(o: str) -> Union[List[Any], Dict[Any, Any], Any]:
        """Either load JSON (if we can) or strip and split the string, while logging the error"""
        try:
            return json.loads(o)
        except (JSONDecodeError, TypeError) as e:
            logging.debug(e)
            logging.debug(o)
            return o.strip()

    log_lines = log_string.split("\n")
    enumerated_log_lines = list(enumerate(log_lines))
    found_i = -1
    for i, line in enumerated_log_lines:
        if "%%%%%%%" in line:
            found_i = i + 1
            break
    if found_i != -1:
        output = base64.decodebytes("\n".join(log_lines[found_i:]).encode("utf-8")).decode("utf-8")
        try:
            return json.loads(output)
        except JSONDecodeError:
            return get_json_or_clean_str(output)
    else:
        return get_json_or_clean_str(log_string)


bp = Blueprint(
    "starship",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/starship",
)


class AstroMigration(AppBuilderBaseView):
    default_view = "main"

    def __init__(self):
        super().__init__()
        self.local_client = LocalAirflowClient()
        self.astro_client = AstroClient()

    def get_astro_username(self, token):
        if not token:
            pass

        headers = {"Authorization": f"Bearer {token}"}
        client = GraphqlClient(
            endpoint="https://api.astronomer.io/hub/v1", headers=headers
        )

        query = "{self {user {username}}}"

        try:
            api_rv = at(client.execute(query), "data.self.user.username")[0]

            return api_rv
        except Exception as exc:
            print(exc)
            return None

    @ttl_cache(ttl=1)
    def astro_deployments(self, token):
        if token:
            headers = {"Authorization": f"Bearer {token}"}
            client = GraphqlClient(
                endpoint="https://api.astronomer.io/hub/v1", headers=headers
            )
            query = """
            {
                deployments
                {
                    id,
                    label,
                    releaseName,
                    workspace
                    {
                        id,
                        label
                    },
                    deploymentShortId,
                    deploymentSpec
                    {
                        environmentVariables
                        webserver {
                            ingressHostname,
                            url
                        }
                    }
                }
            }
            """

            try:
                api_rv = at(client.execute(query), "data.deployments")[0]

                return {deploy["id"]: deploy for deploy in (api_rv or [])}
            except Exception as exc:
                print(exc)
                return {}

    def get_astro_config(self, deployment_url: str, token: str):
        resp = requests.get(
            f"{deployment_url}/api/v1/config",
            headers={"Authorization": f"Bearer {token}"},
        )
        return resp.json()

    def get_astro_variables(self, deployment_url: str, token: str):
        resp = requests.get(
            f"{deployment_url}/api/v1/variables",
            headers={"Authorization": f"Bearer {token}"},
        )
        return resp.json()["variables"]

    @ttl_cache(ttl=1)
    def get_astro_connections(self, deployment_url: str, token: str):
        resp = requests.get(
            f"{deployment_url}/api/v1/connections",
            headers={"Authorization": f"Bearer {token}"},
        )
        return resp.json()["connections"]

    @expose("/", methods=["GET", "POST"])
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def main(self):
        session.update(request.form)
        return self.render_template("main.html")

    @staticmethod
    @ttl_cache(ttl=3600)
    def get_jwk(token: str):
        jwks_url = "https://auth.astronomer.io/.well-known/jwks.json"
        jwks_client = jwt.PyJWKClient(jwks_url)
        return jwks_client.get_signing_key_from_jwt(token)

    @expose("/modal/token")
    def modal_token_entry(self):
        token = session.get("token")

        if not token:
            return self.render_template(
                "components/token_modal.html", show=True, error=None
            )

        try:
            jwt.decode(
                token,
                audience=["astronomer-ee"],
                key=self.get_jwk(token).key,
                algorithms=["RS256"],
            )
        except jwt.exceptions.ExpiredSignatureError:
            return self.render_template(
                "components/token_modal.html", show=True, error="expired"
            )
        except jwt.exceptions.InvalidTokenError:
            return self.render_template(
                "components/token_modal.html", show=True, error="invalid"
            )

        return self.render_template("components/token_modal.html", show=False)

    @expose("/button/save_token", methods=("POST",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def button_save_token(self):
        session["token"] = request.form.get("astroUserToken")
        return self.render_template("migration.html")

    @expose("/tabs/dags")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def tabs_dags(self):
        data = {"component": "dags", "dags": self.local_client.get_dags()}

        self.local_client.get_dags()

        return self.render_template("dags.html", data=data)

    @expose("/tabs/variables")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_vars(self):
        data = {
            "component": "variables",
            "vars": {var.key: var for var in self.local_client.get_variables()},
        }

        return self.render_template("variables.html", data=data)

    @expose("/tabs/connections")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def tabs_conns(self):
        data = {
            "component": "connections",
            "conns": {
                conn.conn_id: conn for conn in self.local_client.get_connections()
            },
        }

        return self.render_template("connections.html", data=data)

    @expose("/tabs/env")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def tabs_env(self):
        import os

        data = {
            "component": "env",
            "environ": os.environ,
        }

        return self.render_template("env.html", data=data)

    @expose(
        "/button_migrate_connection/<string:deployment>/<string:conn_id>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def button_migrate_connection(self, conn_id: str, deployment: str):
        deployment_url = self.get_deployment_url(deployment)

        if request.method == "POST":
            local_connections = {
                conn.conn_id: conn for conn in self.local_client.get_connections()
            }

            requests.post(
                f"{deployment_url}/api/v1/connections",
                headers={"Authorization": f"Bearer {session.get('token')}"},
                json={
                    "connection_id": local_connections[conn_id].conn_id,
                    "conn_type": local_connections[conn_id].conn_type,
                    "host": local_connections[conn_id].host,
                    "login": local_connections[conn_id].login,
                    "schema": local_connections[conn_id].schema,
                    "port": local_connections[conn_id].port,
                    "password": local_connections[conn_id].password or "",
                    "extra": local_connections[conn_id].extra,
                },
            )

        deployment_conns = self.get_astro_connections(
            deployment_url, session.get("token")
        )

        is_migrated = conn_id in [
            remote_conn["connection_id"] for remote_conn in deployment_conns
        ]

        return self.render_template(
            "components/migrate_connection_button.html",
            conn_id=conn_id,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/label_test_connection/<string:deployment>/<string:conn_id>")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def label_test_connection(self, conn_id: str, deployment: str):
        deployment_url = self.get_deployment_url(deployment)

        local_connections = {
            conn.conn_id: conn for conn in self.local_client.get_connections()
        }

        rv = requests.post(
            f"{deployment_url}/api/v1/connections/test",
            headers={"Authorization": f"Bearer {session.get('token')}"},
            json={
                "connection_id": local_connections[conn_id].conn_id,
                "conn_type": local_connections[conn_id].conn_type,
                "host": local_connections[conn_id].host,
                "login": local_connections[conn_id].login,
                "schema": local_connections[conn_id].schema,
                "port": local_connections[conn_id].port,
                "password": local_connections[conn_id].password or "",
                "extra": local_connections[conn_id].extra,
            },
        )

        return self.render_template(
            "components/test_connection_label.html", data=rv.json(), conn_id=conn_id
        )

    @expose(
        "/button/migrate/var/<string:deployment>/<string:variable>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def button_migrate_variable(self, variable: str, deployment: str):
        deployment_url = self.get_deployment_url(deployment)

        if request.method == "POST":
            local_vars = {var.key: var for var in self.local_client.get_variables()}

            requests.post(
                f"{deployment_url}/api/v1/variables",
                headers={"Authorization": f"Bearer {session.get('token')}"},
                json={"key": variable, "value": local_vars[variable].val},
            )

        remote_vars = self.get_astro_variables(deployment_url, session.get("token"))

        is_migrated = variable in [remote_var["key"] for remote_var in remote_vars]

        return self.render_template(
            "components/migrate_variable_button.html",
            variable=variable,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/button/migrate/env/<string:deployment>", methods=("POST",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def button_migrate_env(self, deployment: str):
        import os

        deployments = self.astro_deployments(session.get("token"))

        headers = {"Authorization": f"Bearer {session.get('token')}"}
        client = GraphqlClient(
            endpoint="https://api.astronomer.io/hub/v1", headers=headers
        )

        query = """
        fragment EnvironmentVariable on EnvironmentVariable {
            key
            value
            isSecret
            updatedAt
        }
        mutation deploymentVariablesUpdate($input: EnvironmentVariablesInput!) {
            deploymentVariablesUpdate(input: $input) {
                ...EnvironmentVariable
            }
        }
        """

        remote_vars = {
            remote_var["key"]: {
                "key": remote_var["key"],
                "value": remote_var["value"],
                "isSecret": remote_var["isSecret"],
            }
            for remote_var in deployments[deployment]["deploymentSpec"][
                "environmentVariables"
            ]
        }

        for _, key in (
            (key, value)
            for (key, value) in request.form.items()
            if key != "csrf_token" and key not in remote_vars.keys()
        ):
            remote_vars.setdefault(
                key,
                {
                    "key": key,
                    "value": os.environ[key],
                    "isSecret": False,
                },
            )

        client.execute(
            query,
            {
                "input": {
                    "deploymentId": deployment,
                    "environmentVariables": list(remote_vars.values()),
                }
            },
        )

        return self.tabs_env()

    @expose("/checkbox/migrate/env/<string:deployment>/<string:key>/", methods=("GET",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def checkbox_migrate_env(self, key: str, deployment: str):
        deployments = self.astro_deployments(session.get("token"))

        remote_vars = {
            remote_var["key"]: {
                "key": remote_var["key"],
                "value": remote_var["value"],
                "isSecret": remote_var["isSecret"],
            }
            for remote_var in deployments[deployment]["deploymentSpec"][
                "environmentVariables"
            ]
        }

        is_migrated = key in remote_vars.keys()

        return self.render_template(
            "components/env_checkbox.html",
            target=key,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/component/astro-deployment-selector")
    def deployment_selector(self):
        deployments = self.astro_deployments(session.get("token"))

        return self.render_template(
            "components/target_deployment_select.html",
            deployments=deployments,
            username=self.get_astro_username(token=session.get("token")),
        )

    @expose("/logout")
    def logout(self):
        session.pop("token")
        return redirect(url_for("Airflow.index"))

    @expose("/component/row/dags/<string:deployment>/<string:dag_id>")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def dag_cutover_row_get(self, deployment: str, dag_id: str):
        return self.dag_cutover_row(deployment, dag_id)

    @expose(
        "/component/row/dags/<string:deployment>/<string:dest>/<string:dag_id>/<string:action>",
        methods=(
            "GET",
            "POST",
        ),
    )
    @auth.has_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
    def dag_cutover_row(
        self, deployment: str, dag_id: str, dest: str = "local", action: str = None
    ):
        if dest not in ["local", "astro"]:
            raise Exception("dest must be 'local' or 'astro'")

        dag = self.local_client.get_dags()[dag_id]
        deployment_url = self.get_deployment_url(deployment)
        token = session.get("token")

        if request.method == "POST":
            if action == "pause":
                is_paused = True
            elif action == "unpause":
                is_paused = False
            else:
                raise Exception("action must be 'pause' or 'unpause'")

            if dest == "local":
                models.DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)
            else:
                resp = requests.patch(
                    f"{deployment_url}/api/v1/dags?dag_id_pattern={dag_id}",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"is_paused": is_paused},
                )

        resp = requests.get(
            f"{deployment_url}/api/v1/dags/{dag_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

        is_on_astro = not resp.status_code == 404

        resp_contents = resp.json()

        return self.render_template(
            "components/dag_row.html",
            dag_={
                "id": dag.dag_id,
                "is_on_astro": is_on_astro,
                "is_paused_here": dag.is_paused,
                "is_paused_on_astro": resp_contents["is_paused"]
                if is_on_astro
                else False,
            },
        )

    def get_deployment_url(self, deployment):
        astro_deployments = self.astro_deployments(session.get("token"))

        if astro_deployments and astro_deployments.get(deployment):
            url = urlparse(
                astro_deployments[deployment]["deploymentSpec"]["webserver"]["url"]
            )
            return f"https:/{url.netloc}/{url.path}"


v_appbuilder_view = AstroMigration()

v_appbuilder_package = {
    "name": "Starship 🛸",
    "category": "Astronomer",
    "view": v_appbuilder_view,
}


class StarshipPlugin(AirflowPlugin):
    name = "starship"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]


bp = Blueprint(
    "aeroscope",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/aeroscope",
)


class AeroForm(Form):
    organization = StringField("Organization", [validators.Length(min=4, max=25)])
    presigned_url = StringField("Pre-signed URL (optional)", [validators.URL(), validators.optional()])


# Creating a flask appbuilder BaseView
class Aeroscope(AppBuilderBaseView):
    default_view = "aeroscope"

    @expose("/", methods=("GET", "POST"))
    def aeroscope(self):
        form = AeroForm(request.form)
        if request.method == "POST" and form.validate() and request.form["action"] == "Download":

            import io
            import runpy
            from urllib.request import urlretrieve

            VERSION = os.getenv("TELESCOPE_REPORT_RELEASE_VERSION", "latest")
            a = "airflow_report.pyz"
            if VERSION == "latest":
                urlretrieve("https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz", a)
            else:
                try:
                    urlretrieve(
                        f"https://github.com/astronomer/telescope/releases/download/{VERSION}/airflow_report.pyz", a
                    )
                except urllib.error.HTTPError as e:
                    flash(f"Error finding specified version:{VERSION} -- Reason:{e.reason}")
            s = io.StringIO()
            with redirect_stdout(s), redirect_stderr(s):
                runpy.run_path(a)
            date = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
            content = {
                "telescope_version": "aeroscope-latest",
                "report_date": date,
                "organization_name": form.organization.data,
                "local": {socket.gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}},
            }
            if len(form.presigned_url.data) > 1:
                upload = requests.put(form.presigned_url.data, data=json.dumps(content))
                if upload.ok:
                    flash("Upload successful")
                else:
                    flash(upload.reason, "error")
            filename = f"{date}.{form.organization.data}.data.json"
            return Response(
                json.dumps(content),
                mimetype="application/json",
                headers={"Content-Disposition": f"attachment;filename={filename}"},
            )
        elif request.method == "POST" and request.form["action"] == "Back to Airflow":
            return redirect(conf.get("webserver", "base_url"))

        else:
            return self.render_template("main.html", form=form)


v_appbuilder_view = Aeroscope()


# Defining the plugin class
class AeroscopePlugin(AirflowPlugin):
    name = "aeroscope"
    hooks = []
    macros = []
    flask_blueprints = [bp]
    appbuilder_views = [
        {
            "name": "Run Aeroscope Report",
            "category": "Astronomer",
            "view": v_appbuilder_view,
        },
    ]
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
