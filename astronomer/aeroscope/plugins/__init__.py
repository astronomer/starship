import datetime
import json
import os
import socket
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from textwrap import dedent
from typing import Optional

import requests
from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from aeroscope.util import clean_airflow_report_output
from flask import Blueprint, Response, flash, redirect, request
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from wtforms import Form, StringField, validators

bp = Blueprint(
    "starship_aeroscope",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/aeroscope",
)


class AeroForm(Form):
    organization = StringField("Organization", [validators.Length(min=4, max=25)])
    presigned_url = StringField(
        "Pre-signed URL (optional)",
        description=dedent("""The pre-signed URL field is optional and is to be supplied by an Astronomer Representative.
    If the pre-signed URL is used, the results of the Telescope Report is shipped to Astronomer."""),
        validators=[validators.URL(), validators.optional()]
    )


def get_aeroscope_report(date: str, organization: str):
    import io
    import runpy
    from urllib.request import urlretrieve

    version = os.getenv("TELESCOPE_REPORT_RELEASE_VERSION", "latest")
    a = "airflow_report.pyz"
    if version == "latest":
        urlretrieve("https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz", a)
    else:
        try:
            urlretrieve(
                f"https://github.com/astronomer/telescope/releases/download/{version}/airflow_report.pyz", a
            )
        except urllib.error.HTTPError as e:
            flash(f"Error finding specified version:{version} -- Reason:{e.reason}")
    s = io.StringIO()
    with redirect_stdout(s), redirect_stderr(s):
        runpy.run_path(a)
    return {
        "telescope_version": "aeroscope-latest",
        "report_date": date,
        "organization_name": organization,
        "local": {socket.gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}},
    }


# Creating a flask appbuilder BaseView
class StarshipAeroscope(AppBuilderBaseView):
    default_view = "aeroscope"

    @expose("/", methods=("GET", "POST"))
    def aeroscope(self):
        form = AeroForm(request.form)
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
        if request.method == "POST" and form.validate() and request.form["action"] == "Send/Download Report":
            filename = f"{date}.{form.organization.data}.data.json"
            content = get_aeroscope_report(
                date=date,
                organization=form.organization.data,
            )
            if len(form.presigned_url.data) > 1:
                upload = requests.put(form.presigned_url.data, data=json.dumps(content))
                if upload.ok:
                    flash("Upload successful")
                else:
                    flash(upload.reason, "error")

            return Response(
                json.dumps(content),
                mimetype="application/json",
                headers={"Content-Disposition": f"attachment;filename={filename}"},
            )
        elif request.method == "POST" and form.validate() and request.form["action"] == "Show Report":

            return self.render_template(
                "aeroscope/report.html",
                form=form,
                data=get_aeroscope_report(
                    date=date,
                    organization=form.organization.data,
                )
            )
        else:
            return self.render_template("aeroscope/main.html", form=form)


v_appbuilder_view = StarshipAeroscope()


# Defining the plugin class
class AeroscopePlugin(AirflowPlugin):
    name = "starship_aeroscope"
    hooks = []
    macros = []
    flask_blueprints = [bp]
    appbuilder_views = [
        {
            "name": "Run Report 🔭 Telescope",
            "category": "Astronomer",
            "view": v_appbuilder_view,
        },
    ]
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
