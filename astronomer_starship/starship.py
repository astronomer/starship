from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_appbuilder import BaseView
from flask_appbuilder import expose

from airflow.security import permissions
from airflow.www import auth


class Starship(BaseView):
    default_view = "main"

    @expose("/")
    @expose("/<string:path>")
    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)])
    def main(self, path=""):
        """Main view - just bootstraps the React app."""
        return self.render_template("index.html")


starship_view = Starship()
starship_bp = Blueprint(
    "starship",
    __name__,
    static_folder="static",  # should be default, just being explicit
    template_folder="templates",  # should be default, just being explicit
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
