import base64
import datetime
import json
import logging
import socket

from airflow.models import BaseOperator
from json import JSONDecodeError
from contextlib import redirect_stderr, redirect_stdout
from typing import Any, Dict, List, Sequence, Union

class AeroscopeOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "presigned_url",
        "email",
    )

    def __init__(
        self,
        *,
        presigned_url,
        email,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.presigned_url = presigned_url
        self.email = email

    def clean_airflow_report_output(self, log_string: str) -> Union[dict, str]:
        r"""Look for the magic string from the Airflow report and then decode the base64 and convert to json
        Or return output as a list, trimmed and split on newlines
        >>> clean_airflow_report_output('INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\naGVsbG8gd29ybGQ=')
        'hello world'
        >>> clean_airflow_report_output('INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\neyJvdXRwdXQiOiAiaGVsbG8gd29ybGQifQ==')
        {'output': 'hello world'}
        """
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
                return self.get_json_or_clean_str(output)
        else:
            return self.get_json_or_clean_str(log_string)

    def get_json_or_clean_str(self, o: str) -> Union[List[Any], Dict[Any, Any], Any]:
        """Either load JSON (if we can) or strip and split the string, while logging the error"""
        log = logging.getLogger(__name__)
        try:
            return json.loads(o)
        except (JSONDecodeError, TypeError) as e:
            log.debug(e)
            log.debug(o)
            return o.strip()

    def execute(self, context: "Context"):
        import io
        import runpy
        from urllib.request import urlretrieve

        import requests

        a = "airflow_report.pyz"
        urlretrieve("https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz", a)
        s = io.StringIO()
        with redirect_stdout(s), redirect_stderr(s):
            runpy.run_path(a)
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
        content = {
            "telescope_version": "aeroscope",
            "report_date": date,
            "organization_name": "aeroscope",
            "local": {socket.gethostname(): {"airflow_report": self.clean_airflow_report_output(s.getvalue())}},
            "user_email": self.email,
        }
        s3 = requests.put(self.presigned_url, data=json.dumps(content))
        if s3.ok:
            return "success"
        else:
            raise ValueError(f"upload failed  with code {s3.status_code}::{s3.json()}")
        # return json.dumps(content)
