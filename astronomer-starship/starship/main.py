from airflow.plugins_manager import AirflowPlugin

from airflow.www import auth
from airflow.security import permissions

import jwt

from flask import Blueprint, session, request, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

import requests

import os

from starship.services import astro_client, remote_airflow_client, local_airflow_client
from starship.services.astro_client import is_environment_variable_migrated, get_jwk
from starship.services.astro_client import get_deployment_url
from starship.services.astro_client import get_deployments
from starship.services.remote_airflow_client import create_connection, get_dag
from starship.services.remote_airflow_client import test_connection
from starship.services.astro_client import get_username

bp = Blueprint(
    "starship",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/starship",
)


def get_page_data(page):
    return {
        "AstroMigration.tabs_vars": {
            "component": "variables",
            "vars": {
                var.key: var for var in local_airflow_client.get_variables()
            },
        },
        "AstroMigration.tabs_dags": {
            "component": "dags",
            "dags": local_airflow_client.get_dags()},
        "AstroMigration.tabs_pools": {
            "component": "pools",
            "pools": {
                pool.pool: pool for pool in local_airflow_client.get_pools()
            },
        },
        "AstroMigration.tabs_conns": {
            "component": "connections",
            "conns": {
                conn.conn_id: conn for conn in local_airflow_client.get_connections()
            },
        },
        "AstroMigration.tabs_env": {
            "component": "env",
            "environ": os.environ,
        }
    }[page]


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
        tab = session.get('tab', 'AstroMigration.tabs_conns')
        return self.render_template("starship/migration.html", data={"tab": tab})

    @expose("/tabs/dags")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def tabs_dags(self):
        session["tab"] = "AstroMigration.tabs_dags"
        return self.render_template("starship/dags.html", data=get_page_data(session['tab']))

    @expose("/tabs/variables")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_vars(self):
        session["tab"] = "AstroMigration.tabs_vars"
        return self.render_template("starship/variables.html", data=get_page_data(session['tab']))

    @expose("/tabs/pools")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_pools(self):
        session["tab"] = "AstroMigration.tabs_pools"
        return self.render_template("starship/pools.html", data=get_page_data(session['tab']))

    @expose("/tabs/connections")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def tabs_conns(self):
        session["tab"] = "AstroMigration.tabs_conns"
        return self.render_template("starship/connections.html", data=get_page_data(session['tab']))

    @expose("/tabs/env")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def tabs_env(self):
        session["tab"] = "AstroMigration.tabs_env"
        return self.render_template("starship/env.html", data=get_page_data(session['tab']))

    @expose(
        "/button_migrate_connection/<string:deployment>/<string:conn_id>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def button_migrate_connection(self, conn_id: str, deployment: str):
        deployment_url = get_deployment_url(deployment, session.get("token"))

        if request.method == "POST":
            local_connections = {
                conn.conn_id: conn for conn in local_airflow_client.get_connections()
            }
            create_connection(conn_id, deployment_url, local_connections)

        deployment_conns = remote_airflow_client.get_connections(
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
        if not os.getenv("ENABLE_CONN_TEST"):
            return "❔"


        deployment_url = get_deployment_url(deployment, session.get("token"))

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
        deployment_url = get_deployment_url(deployment, session.get("token"))

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
        deployment_url = get_deployment_url(deployment, session.get("token"))

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
        token = session.get("token")
        items = dict(**request.form)
        del items['csrf_token']
        astro_client.set_changed_environment_variables(deployment, token, iter(items.values()))
        return self.tabs_env()

    @expose("/checkbox/migrate/env/<string:deployment>/<string:key>/", methods=("GET",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def checkbox_migrate_env(self, key: str, deployment: str):
        token = session.get('token')
        is_migrated = is_environment_variable_migrated(deployment, token, key)
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
        methods=("GET", "POST",),
    )
    @auth.has_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
    def dag_cutover_row(
            self, deployment: str, dag_id: str, dest: str = "local", action: str = None
    ):
        if dest not in ["local", "astro"]:
            raise RuntimeError("dest must be 'local' or 'astro'")

        dag = local_airflow_client.get_dags()[dag_id]
        token = session.get("token")
        deployment_url = get_deployment_url(deployment, token)

        if request.method == "POST":
            if action == "pause":
                is_paused = True
            elif action == "unpause":
                is_paused = False
            else:
                raise RuntimeError("action must be 'pause' or 'unpause'")

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
