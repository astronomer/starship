from airflow.plugins_manager import AirflowPlugin

from airflow.www import auth
from airflow.security import permissions

import jwt

from flask import Blueprint, session, request, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

import requests

from starship.services import local_airflow_client, remote_airflow_client
from starship.services.astro_client import get_deployments, get_username, get_jwk, get_deployment_url, \
    set_environment_variables
from starship.services.remote_airflow_client import get_dag, test_connection, create_connection

bp = Blueprint(
    "starship",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/starship",
)


class AstroMigration(AppBuilderBaseView):
    default_view = "main"

    @expose("/", methods=["GET", "POST"])
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def main(self):
        session.update(request.form)
        return self.render_template("starship/main.html")

    @expose("/modal/token")
    def modal_token_entry(self):
        token = session.get("token")

        if not token:
            return self.render_template(
                "starship/components/token_modal.html", show=True, error=None
            )

        try:
            jwt.decode(
                token,
                audience=["astronomer-ee"],
                key=get_jwk(token).key,
                algorithms=["RS256"],
            )
        except jwt.exceptions.ExpiredSignatureError:
            return self.render_template(
                "starship/components/token_modal.html", show=True, error="expired"
            )
        except jwt.exceptions.InvalidTokenError:
            return self.render_template(
                "starship/components/token_modal.html", show=True, error="invalid"
            )

        return self.render_template("starship/components/token_modal.html", show=False)

    @expose("/button/save_token", methods=("POST",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def button_save_token(self):
        session["token"] = request.form.get("astroUserToken")
        return self.render_template("starship/migration.html")

    @expose("/tabs/dags")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def tabs_dags(self):
        data = {"component": "dags", "dags": local_airflow_client.get_dags()}

        return self.render_template("starship/dags.html", data=data)

    @expose("/tabs/variables")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_vars(self):
        data = {
            "component": "variables",
            "vars": {var.key: var for var in local_airflow_client.get_variables()},
        }

        return self.render_template("starship/variables.html", data=data)

    @expose("/tabs/pools")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_pools(self):
        data = {
            "component": "pools",
            "pools": {pool.pool: pool for pool in local_airflow_client.get_pools()},
        }

        return self.render_template("starship/pools.html", data=data)

    @expose("/tabs/connections")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def tabs_conns(self):
        data = {
            "component": "connections",
            "conns": {
                conn.conn_id: conn for conn in local_airflow_client.get_connections()
            },
        }

        return self.render_template("starship/connections.html", data=data)

    @expose("/tabs/env")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def tabs_env(self):
        import os

        data = {
            "component": "env",
            "environ": os.environ,
        }

        return self.render_template("starship/env.html", data=data)

    @expose(
        "/button_migrate_connection/<string:deployment>/<string:conn_id>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def button_migrate_connection(self, conn_id: str, deployment: str):
        deployment_url = get_deployment_url(deployment)

        if request.method == "POST":
            local_connections = {
                conn.conn_id: conn for conn in local_airflow_client.get_connections()
            }
            create_connection(conn_id, deployment_url, local_connections)

        deployment_conns = local_airflow_client.get_connections(
            deployment_url, session.get("token")
        )

        is_migrated = conn_id in [
            remote_conn["connection_id"] for remote_conn in deployment_conns
        ]

        return self.render_template(
            "starship/components/migrate_connection_button.html",
            conn_id=conn_id,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/label_test_connection/<string:deployment>/<string:conn_id>")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def label_test_connection(self, conn_id: str, deployment: str):
        deployment_url = get_deployment_url(deployment)

        local_connections = {
            conn.conn_id: conn for conn in local_airflow_client.get_connections()
        }
        rv = test_connection(conn_id, deployment_url, local_connections)

        return self.render_template(
            "starship/components/test_connection_label.html", data=rv.json(), conn_id=conn_id
        )

    @expose(
        "/button/migrate/var/<string:deployment>/<string:variable>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def button_migrate_variable(self, variable: str, deployment: str):
        deployment_url = get_deployment_url(deployment)

        if request.method == "POST":
            local_vars = {var.key: var for var in local_airflow_client.get_variables()}

            remote_airflow_client.create_variable(deployment_url, local_vars, variable)

        remote_vars = remote_airflow_client.get_variables(deployment_url, session.get("token"))

        is_migrated = variable in [remote_var["key"] for remote_var in remote_vars]

        return self.render_template(
            "starship/components/migrate_variable_button.html",
            variable=variable,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose(
        "/button/migrate/pool/<string:deployment>/<string:pool_name>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def button_migrate_pool(self, pool_name: str, deployment: str):
        deployment_url = get_deployment_url(deployment)

        if request.method == "POST":
            pool = [p for p in local_airflow_client.get_pools() if p.pool == pool_name][0]

            r = requests.post(
                f"{deployment_url}/api/v1/pools",
                headers={"Authorization": f"Bearer {session.get('token')}"},
                json={"name": pool_name, "slots": pool.slots, "description": pool.description},
            )
            print(r.request.body)
            r.raise_for_status()

        remote_vars = remote_airflow_client.get_pools(deployment_url, session.get("token"))

        is_migrated = any((True for remote_var in remote_vars if remote_var.get("name", "") == pool_name))

        return self.render_template(
            "starship/components/migrate_pool_button.html",
            pool=pool_name,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/button/migrate/env/<string:deployment>", methods=("POST",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def button_migrate_env(self, deployment: str):
        import os

        deployments = get_deployments(session.get("token"))

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
        set_environment_variables(deployment, remote_vars)

        return self.tabs_env()

    @expose("/checkbox/migrate/env/<string:deployment>/<string:key>/", methods=("GET",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def checkbox_migrate_env(self, key: str, deployment: str):
        deployments = get_deployments(session.get("token"))

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
            "starship/components/env_checkbox.html",
            target=key,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/component/astro-deployment-selector")
    def deployment_selector(self):
        deployments = get_deployments(session.get("token"))

        return self.render_template(
            "starship/components/target_deployment_select.html",
            deployments=deployments,
            username=get_username(token=session.get("token")),
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

        dag = local_airflow_client.get_dags()[dag_id]
        deployment_url = get_deployment_url(deployment)
        token = session.get("token")

        if request.method == "POST":
            if action == "pause":
                is_paused = True
            elif action == "unpause":
                is_paused = False
            else:
                raise Exception("action must be 'pause' or 'unpause'")

            if dest == "local":
                local_airflow_client.set_dag_is_paused(dag_id, is_paused)
            else:
                remote_airflow_client.set_dag_is_paused(dag_id, is_paused, deployment_url, token)

        resp = get_dag(dag_id, deployment_url, token)

        is_on_astro = not resp.status_code == 404

        resp_contents = resp.json()

        return self.render_template(
            "starship/components/dag_row.html",
            dag_={
                "id": dag.dag_id,
                "is_on_astro": is_on_astro,
                "is_paused_here": dag.is_paused,
                "is_paused_on_astro": resp_contents["is_paused"]
                if is_on_astro
                else False,
            },
        )


v_appbuilder_view = AstroMigration()

v_appbuilder_package = {
    "name": "Migration Tool 🚀 Starship",
    "category": "Astronomer",
    "view": v_appbuilder_view,
}


class StarshipPlugin(AirflowPlugin):
    name = "starship"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
