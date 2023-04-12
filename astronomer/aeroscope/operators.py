import datetime
import json
import logging
import os
import socket
import urllib
from contextlib import redirect_stderr, redirect_stdout
from typing import Sequence

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator

from astronomer.aeroscope.util import clean_airflow_report_output


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
            urlretrieve(
                "https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz",
                a,
            )
        else:
            try:
                self.log.info(f"Running Version {VERSION} of report")
                urlretrieve(
                    f"https://github.com/astronomer/telescope/releases/download/{VERSION}/airflow_report.pyz",
                    a,
                )
            except urllib.error.HTTPError as e:
                raise AirflowFailException(
                    f"Error finding specified version:{VERSION} -- Reason:{e.reason}"
                )
        s = io.StringIO()
        with redirect_stdout(s), redirect_stderr(s):
            runpy.run_path(a)
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
        content = {
            "telescope_version": "aeroscope-latest",
            "report_date": date,
            "organization_name": self.organization,
            "local": {
                socket.gethostname(): {
                    "airflow_report": clean_airflow_report_output(s.getvalue())
                }
            },
        }
        upload = requests.put(self.presigned_url, data=json.dumps(content))
        if not upload.ok:
            logging.error(
                f"Upload failed with code {upload.status_code}::{upload.text}"
            )
            upload.raise_for_status()
