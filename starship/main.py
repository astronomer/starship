from urllib.parse import urlparse

from cachetools.func import ttl_cache
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf


from flask import Blueprint, session, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from starship.services.astrohub_client import AstroClient
from starship.services.local_client import LocalAirflowClient

import requests

from python_graphql_client import GraphqlClient

bp = Blueprint(
    "starship",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/starship",
)


class MigrationBaseView(AppBuilderBaseView):
    default_view = "main"

    def __init__(self):
        super().__init__()
        self.local_client = LocalAirflowClient()
        self.astro_client = AstroClient()

    @ttl_cache(ttl=10)
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
                api_rv = client.execute(query)["data"]["deployments"]

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
    @csrf.exempt
    def main(self):
        session.update(request.form)
        return self.render_template("migration.html")

    @expose("/tabs/dags")
    def tabs_dags(self):
        data = {"component": "dags", "dags": self.local_client.get_dags()}

        return self.render_template("dags.html", data=data)

    @expose("/tabs/variables")
    def tabs_vars(self):
        data = {
            "component": "variables",
            "vars": {var.key: var for var in self.local_client.get_variables()},
        }

        return self.render_template("variables.html", data=data)

    @expose("/tabs/connections")
    def tabs_conns(self):
        data = {
            "component": "connections",
            "conns": {
                conn.conn_id: conn for conn in self.local_client.get_connections()
            },
        }

        return self.render_template("connections.html", data=data)

    @expose("/tabs/env")
    def tabs_env(self):
        import os

        data = {
            "component": "env",
            "environ": os.environ,
        }

        return self.render_template("env.html", data=data)

    @expose(
        "/button/migrate/conn/<string:conn_id>/<string:deployment>",
        methods=("GET", "POST"),
    )
    def button_migrate_conn(self, conn_id: str, deployment: str):
        deployment_url = self.get_deployment_url(deployment)

        if request.method == "POST":
            local_connections = {
                conn.conn_id: conn for conn in self.local_client.get_connections()
            }

            requests.post(
                f"{deployment_url}/api/v1/connections",
                headers={"Authorization": f"Bearer {session.get('bearerToken')}"},
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
            deployment_url, session.get("bearerToken")
        )

        is_migrated = conn_id in [
            remote_conn["connection_id"] for remote_conn in deployment_conns
        ]

        return self.render_template(
            "components/migrate_button.html",
            type="conn",
            target=conn_id,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose(
        "/button/migrate/var/<string:key>/<string:deployment>", methods=("GET", "POST")
    )
    def button_migrate_var(self, key: str, deployment: str):
        deployment_url = self.get_deployment_url(deployment)

        if request.method == "POST":
            local_vars = {var.key: var for var in self.local_client.get_variables()}

            requests.post(
                f"{deployment_url}/api/v1/variables",
                headers={"Authorization": f"Bearer {session.get('bearerToken')}"},
                json={"key": key, "value": local_vars[key].val},
            )

        remote_vars = self.get_astro_variables(
            deployment_url, session.get("bearerToken")
        )

        is_migrated = key in [remote_var["key"] for remote_var in remote_vars]

        return self.render_template(
            "components/migrate_button.html",
            type="var",
            target=key,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/button/migrate/env/<string:deployment>", methods=("POST",))
    def button_migrate_env(self, deployment: str):
        import os

        deployments = self.astro_deployments(session.get("bearerToken"))

        headers = {"Authorization": f"Bearer {session.get('bearerToken')}"}
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

    @expose("/checkbox/migrate/env/<string:key>/<string:deployment>", methods=("GET",))
    def checkbox_migrate_env(self, key: str, deployment: str):
        deployments = self.astro_deployments(session.get("bearerToken"))

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
        deployments = self.astro_deployments(session.get("bearerToken"))

        return self.render_template(
            "components/target_deployment_select.html",
            deployments=deployments,
        )

    def get_deployment_url(self, deployment):
        astro_deployments = self.astro_deployments(session.get("bearerToken"))

        if astro_deployments and astro_deployments.get(deployment):
            url = urlparse(
                astro_deployments[deployment]["deploymentSpec"]["webserver"]["url"]
            )
            return f"https:/{url.netloc}/{url.path}"


v_appbuilder_view = MigrationBaseView()

v_appbuilder_package = {
    "name": "Astro Migration",
    "category": "Starship 🚀",
    "view": v_appbuilder_view,
}


class StarshipPlugin(AirflowPlugin):
    name = "starship"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
