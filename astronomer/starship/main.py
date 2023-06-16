import os
import logging
from typing import Optional
import jwt
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www import auth
from airflow.www.app import csrf
from flask import Blueprint, Response, redirect, request, session, url_for
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

from astronomer.starship.services import (
    astro_client,
    local_airflow_client,
    remote_airflow_client,
)
from astronomer.starship.services.astro_client import is_environment_variable_migrated
from astronomer.starship.services.remote_airflow_client import is_pool_migrated

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
            "vars": {var.key: var for var in local_airflow_client.get_variables()},
        },
        "AstroMigration.tabs_dags": {
            "component": "dags",
            "dags": local_airflow_client.get_dags(),
        },
        "AstroMigration.tabs_pools": {
            "component": "pools",
            "pools": {pool.pool: pool for pool in local_airflow_client.get_pools()},
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
        },
    }[page]


class AstroMigration(AppBuilderBaseView):
    default_view = "main"

    @expose("/", methods=["GET", "POST"])
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def main(self):
        session.update(request.form)
        return self.render_template(
            "starship/main.html",
            data={"tab": session.get("tab", "AstroMigration.tabs_conns")},
        )

    @expose("/dag_history/receive", methods=["GET", "POST"])
    @csrf.exempt
    def receive_dag_history(self):
        data = request.json
        try:
            local_airflow_client.receive_dag(data=data)
            return Response("OK", 200)
        except Exception as e:
            logging.exception(e)
            return Response(str(e), 200)

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
                key=astro_client.get_jwk(token).key,
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
        tab = session.get("tab", "AstroMigration.tabs_conns")
        return self.render_template("starship/migration.html", data={"tab": tab})

    @expose("/tabs/dags")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def tabs_dags(self):
        session["tab"] = "AstroMigration.tabs_dags"
        return self.render_template(
            "starship/dags.html", data=get_page_data(session["tab"])
        )

    @expose("/tabs/variables")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_vars(self):
        session["tab"] = "AstroMigration.tabs_vars"
        return self.render_template(
            "starship/variables.html", data=get_page_data(session["tab"])
        )

    @expose("/tabs/pools")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def tabs_pools(self):
        session["tab"] = "AstroMigration.tabs_pools"
        return self.render_template(
            "starship/pools.html", data=get_page_data(session["tab"])
        )

    @expose("/tabs/connections")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def tabs_conns(self):
        session["tab"] = "AstroMigration.tabs_conns"
        return self.render_template(
            "starship/connections.html", data=get_page_data(session["tab"])
        )

    @expose("/tabs/env")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def tabs_env(self):
        session["tab"] = "AstroMigration.tabs_env"
        return self.render_template(
            "starship/env.html", data=get_page_data(session["tab"])
        )

    @expose(
        "/button_migrate_connection/<string:deployment>/<string:conn_id>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
    def button_migrate_connection(self, conn_id: str, deployment: str):
        token = session.get("token")
        deployment_url = astro_client.get_deployment_url(deployment, token)

        if request.method == "POST":
            local_connections = {
                conn.conn_id: conn for conn in local_airflow_client.get_connections()
            }
            remote_airflow_client.create_connection(
                deployment_url, token, local_connections[conn_id]
            )

        deployment_conns = remote_airflow_client.get_connections(deployment_url, token)

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
        token = session.get("token")
        deployment_url = astro_client.get_deployment_url(deployment, token)

        local_connection = {
            conn.conn_id: conn for conn in local_airflow_client.get_connections()
        }[conn_id]
        r = remote_airflow_client.do_test_connection(
            deployment_url, token, local_connection
        )

        return self.render_template(
            "starship/components/test_connection_label.html",
            data=r.json(),
            conn_id=conn_id,
        )

    @expose(
        "/button/migrate/var/<string:deployment>/<string:variable>",
        methods=("GET", "POST"),
    )
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
    def button_migrate_variable(self, variable: str, deployment: str):
        token = session.get("token")
        deployment_url = astro_client.get_deployment_url(deployment, token)

        if request.method == "POST":
            remote_airflow_client.create_variable(
                deployment_url, token, local_airflow_client.get_variable(variable)
            )

        is_migrated = remote_airflow_client.is_variable_migrated(
            deployment_url, token, variable
        )

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
    def button_migrate_pool(self, pool_name: str, deployment: Optional[str] = None):
        token = session.get("token")
        if deployment:
            deployment_url = astro_client.get_deployment_url(deployment, token)

            if request.method == "POST":
                pool = local_airflow_client.get_pool(pool_name)
                remote_airflow_client.create_pool(deployment_url, token, pool)

            is_migrated = is_pool_migrated(deployment_url, token, pool_name)
        else:
            # Deployment hasn't been selected yet
            is_migrated = False

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
        del items["csrf_token"]
        astro_client.set_changed_environment_variables(
            deployment, token, iter(items.values())
        )
        return self.tabs_env()

    @expose("/checkbox/migrate/env/<string:deployment>/<string:key>/", methods=("GET",))
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def checkbox_migrate_env(self, key: str, deployment: str):
        token = session.get("token")
        is_migrated = is_environment_variable_migrated(deployment, token, key)
        return self.render_template(
            "starship/components/env_checkbox.html",
            target=key,
            deployment=deployment,
            is_migrated=is_migrated,
        )

    @expose("/component/astro-deployment-selector")
    def deployment_selector(self):
        deployments = session.get("deployments")
        if not deployments:
            deployments = astro_client.get_deployments(session.get("token"))
            session["deployments"] = deployments

        user = session.get("user")
        if not user:
            user = astro_client.get_username(token=session.get("token"))
            session["user"] = user

        return self.render_template(
            "starship/components/target_deployment_select.html",
            deployments=deployments,
            username=user,
        )

    @expose("/logout")
    def logout(self):
        session.pop("token")
        session.pop("user")
        session.pop("deployments")
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
        self, deployment: str, dag_id: str, dest: str = "local", action: str = "init"
    ):
        valid_actions = ["pause", "unpause", "migrate", "init"]
        if dest not in ["local", "astro"]:
            raise RuntimeError("dest must be 'local' or 'astro'")
        if action not in valid_actions:
            raise RuntimeError(
                f"action({action}) must be in valid_actions({valid_actions})"
            )

        token = session.get("token")
        deployment_url = astro_client.get_deployment_url(deployment, token)

        if request.method == "POST":
            if action == "migrate":
                remote_airflow_client.migrate_dag(
                    dag_id, deployment_url=deployment_url, token=token
                )
                # force an update, make sure the change went through
                remote_airflow_client.get_dag(
                    dag_id, deployment_url, token, skip_cache=True
                )
            else:
                is_paused = "pause" == action
                if dest == "local":
                    local_airflow_client.set_dag_is_paused(dag_id, is_paused)
                else:
                    remote_airflow_client.set_dag_is_paused(
                        dag_id, is_paused, deployment_url, token
                    )
                    # force an update, make sure the change went through
                    remote_airflow_client.get_dag(
                        dag_id, deployment_url, token, skip_cache=True
                    )

        remote_dag_response = remote_airflow_client.get_dag(
            dag_id, deployment_url, token
        )
        is_on_astro = remote_dag_response is not None
        remote_dag = remote_dag_response or {}
        remote_dag_runs = (
            remote_airflow_client.get_dag_runs(dag_id, deployment_url, token).json()
            if is_on_astro
            else {}
        )
        has_history_on_astro = bool(remote_dag_runs.get("total_entries", 0))
        local_dag = local_airflow_client.get_dag(dag_id)

        return self.render_template(
            "starship/components/dag_row.html",
            dag_={
                "id": local_dag.dag_id,
                "is_on_astro": is_on_astro,
                "is_paused_here": local_dag.is_paused,
                "is_paused_on_astro": remote_dag["is_paused"] if is_on_astro else False,
                "has_history_on_astro": has_history_on_astro,
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
