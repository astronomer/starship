from airflow.models import BaseOperator
from airflow.utils.session import provide_session

from astronomer.starship.connections.operators import AstroConnectionsMigrationOperator
from astronomer.starship.variables.operators import AstroVariableMigrationOperator
from astronomer.starship.env.operators import AstroEnvMigrationOperator

from sqlalchemy.orm import Session
from typing import Any, Sequence

from typing import Sequence

import datetime
import json
import logging
import os
import socket
import urllib
from contextlib import redirect_stderr, redirect_stdout

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from astronomer.aeroscope.util import clean_airflow_report_output



class AstroMigrationOperator(BaseOperator):
    """
    Sends connections, variables, and environment variables from a source Airflow to Astronomer Deployment
    """

    template_fields: Sequence[str] = ("token", "deployment_url")
    ui_color = "#974bde"

    def __init__(
        self,
        deployment_url,
        token,
        variables_exclude_list=None,
        connection_exclude_list=None,
        env_include_list=None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_url = deployment_url
        self.token = token
        self.variables_exclude_list = variables_exclude_list
        self.connection_exclude_list = connection_exclude_list
        self.env_include_list = env_include_list

    @provide_session
    def execute(self, context: Any, session: Session) -> None:
        variables = AstroVariableMigrationOperator(
            task_id="export_variables",
            deployment_url=self.deployment_url,
            token=self.token,
            variable_exclude_list=self.variables_exclude_list,
        )

        connections = AstroConnectionsMigrationOperator(
            task_id="export_connections",
            deployment_url=self.deployment_url,
            token=self.token,
            connection_exclude_list=self.connection_exclude_list,
        )

        env_vars = AstroEnvMigrationOperator(
            task_id="export_env_vars",
            deployment_url=self.deployment_url,
            token=self.token,
            env_include_list=self.env_include_list,
        )

        variables.execute(context=context)
        connections.execute(context=context)
        env_vars.execute(context=context)

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


class AeroscopeOperator(BaseOperator):
    template_fields: Sequence[str] = ("presigned_url", "organization")

    def __init__(
        self,
        presigned_url,
        organization,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.presigned_url = presigned_url
        self.organization = organization

    def execute(self, context):
        import io
        import runpy
        from urllib.request import urlretrieve

        import requests

        VERSION = os.getenv("TELESCOPE_REPORT_RELEASE_VERSION", "latest")
        a = "airflow_report.pyz"
        if VERSION == "latest":
            self.log.info("Running Latest Version of report")
            urlretrieve("https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz", a)
        else:
            try:
                self.log.info(f"Running Version {VERSION} of report")
                urlretrieve(
                    f"https://github.com/astronomer/telescope/releases/download/{VERSION}/airflow_report.pyz", a
                )
            except urllib.error.HTTPError as e:
                raise AirflowFailException(f"Error finding specified version:{VERSION} -- Reason:{e.reason}")
        s = io.StringIO()
        with redirect_stdout(s), redirect_stderr(s):
            runpy.run_path(a)
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
        content = {
            "telescope_version": "aeroscope-latest",
            "report_date": date,
            "organization_name": self.organization,
            "local": {socket.gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}},
        }
        upload = requests.put(self.presigned_url, data=json.dumps(content))
        if not upload.ok:
            logging.error(f"Upload failed with code {upload.status_code}::{upload.text}")
            upload.raise_for_status()
