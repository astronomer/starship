import requests

from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf

from flask import Blueprint, session, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from python_graphql_client import GraphqlClient
from starship.services.local_client import LocalAirflowClient
from urllib.parse import urlparse


bp = Blueprint("starship", __name__, template_folder="templates")


class MigrationBaseView(AppBuilderBaseView):
    default_view = "main"

    def __init__(self):
        super(MigrationBaseView, self).__init__()
        self.local_client = LocalAirflowClient()

    @expose("/astro/deployments", methods=["GET"])
    def astro_deployments(self):
        token = session.get("bearerToken")
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
                return client.execute(query)["data"]["deployments"]
            except Exception as exc:
                print(exc)
                return None

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

    def get_astro_connections(self, deployment_url: str, token: str):
        resp = requests.get(
            f"{deployment_url}/api/v1/connections",
            headers={"Authorization": f"Bearer {token}"},
        )
        return resp.json()["connections"]

    def process_move_button(self, api_url: str, token: str, data: dict):
        for key, value in request.form.items():
            var_to_move = (key.replace("move-var-", ""))
            if var_to_move is not key:
                print(f"Moving var {var_to_move}")
                requests.post(
                    f"{api_url}/api/v1/variables",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"key": var_to_move, "value": data["vars"][var_to_move].val},
                )
                return

            conn_to_move = (key.replace("move-conn-", ""))
            if conn_to_move is not key:
                print(f"Moving connection {conn_to_move}")

                requests.post(
                    f"{api_url}/api/v1/connections",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "connection_id": data["conns"][conn_to_move].conn_id,
                        "conn_type": data["conns"][conn_to_move].conn_type,
                        "host": data["conns"][conn_to_move].host,
                        "login": data["conns"][conn_to_move].login,
                        "schema": data["conns"][conn_to_move].schema,
                        "port": data["conns"][conn_to_move].port,
                        "password": data["conns"][conn_to_move].password or "",
                        "extra": data["conns"][conn_to_move].extra,
                    },
                )
                return

    def process_move_env_vars(self, api_url: str, token: str, data: dict):
        if "migrate-all-the-envs" not in request.form:
            return

        headers = {"Authorization": f"Bearer {token}"}
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
            for remote_var in data["deploys"][data["selected_deployment"]][
                "deploymentSpec"
            ]["environmentVariables"]
        }

        for key, value in request.form.items():
            var_to_move = (key.replace("move-env-", ""))
            if var_to_move is not key:
                remote_vars.setdefault(
                    var_to_move,
                    {
                        "key": var_to_move,
                        "value": data["environ"][var_to_move],
                        "isSecret": False,
                    },
                )

        variables = {
            "input": {
                "deploymentId": data["selected_deployment"],
                "environmentVariables": list(remote_vars.values()),
            }
        }

        client.execute(query, variables)

    @expose("/", methods=["GET", "POST"])
    def main(self):
        import os

        session.update(request.form)

        data = {
            "deploys": {
                deploy["id"]: deploy for deploy in (self.astro_deployments() or [])
            },
            "vars": {var.key: var for var in self.local_client.get_variables()},
            "conns": {
                conn.conn_id: conn
                for conn in self.local_client.get_connections_from_metastore()
            },
            "dags": self.local_client.get_dags(),
            "environ": os.environ,
            "selected_deployment": session.get("selectedAstroDeployment"),
            "remote": {
                "vars": set(),
                "conns": set(),
            },
        }

        if data.get("deploys") and data.get("selected_deployment"):
            data["remote"]["env"] = [
                env["key"]
                for env in data["deploys"][data["selected_deployment"]][
                    "deploymentSpec"
                ]["environmentVariables"]
            ]

            url = urlparse(
                data["deploys"][data.get("selected_deployment")]["deploymentSpec"][
                    "webserver"
                ]["url"]
            )
            api_url = f"https:/{url.netloc}/{url.path}"

            self.process_move_button(api_url, session.get("bearerToken"), data)
            self.process_move_env_vars(api_url, session.get("bearerToken"), data)

            # remote_config = self.get_astro_config(api_url, session.get("bearerToken"))  unused for now
            remote_vars = self.get_astro_variables(api_url, session.get("bearerToken"))
            remote_conns = self.get_astro_connections(
                api_url, session.get("bearerToken")
            )

            data["remote"]["vars"] = {remote_var["key"] for remote_var in remote_vars}
            data["remote"]["conns"] = {
                remote_conn["connection_id"] for remote_conn in remote_conns
            }

        return self.render_template("migration.html", data=data)


v_appbuilder_view = MigrationBaseView()

v_appbuilder_package = {
    "name": "Astro Migration",
    "category": "Starship",
    "view": v_appbuilder_view,
}


class StarshipPlugin(AirflowPlugin):
    name = "starship"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
