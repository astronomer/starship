"""Common utilities for Astronomer Starship which are Airflow version agnostic."""

import json
import os
from typing import Any, Dict, List, Union


class HttpError(Exception):
    """HTTP Error with status code"""

    def __init__(self, msg: str, status_code: int):
        self.msg = msg
        self.status_code = status_code


class NotFound(HttpError):
    """Not Found 404"""

    def __init__(self, msg: str):
        super().__init__(msg, 404)


class MethodNotAllowed(HttpError):
    """Method Not Allowed 405"""

    def __init__(self, msg: str):
        super().__init__(msg, 405)


class Conflict(HttpError):
    """Conflict 409"""

    def __init__(self, msg: str):
        super().__init__(msg, 409)


def get_json_or_clean_str(o: str) -> Union[List[Any], Dict[Any, Any], Any]:
    """For Aeroscope - Either load JSON (if we can) or strip and split the string, while logging the error"""
    from json import JSONDecodeError
    import logging

    try:
        return json.loads(o)
    except (JSONDecodeError, TypeError) as e:
        logging.debug(e)
        logging.debug(o)
        return o.strip()


def clean_airflow_report_output(log_string: str) -> Union[dict, str]:
    r"""For Aeroscope - Look for the magic string from the Airflow report and then decode the base64 and convert to json
    Or return output as a list, trimmed and split on newlines
    >>> clean_airflow_report_output('INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\naGVsbG8gd29ybGQ=')
    'hello world'
    >>> clean_airflow_report_output(
    ...   'INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\neyJvdXRwdXQiOiAiaGVsbG8gd29ybGQifQ=='
    ... )
    {'output': 'hello world'}
    """
    from json import JSONDecodeError
    import base64

    log_lines = log_string.split("\n")
    enumerated_log_lines = list(enumerate(log_lines))
    found_i = -1
    for i, line in enumerated_log_lines:
        if "%%%%%%%" in line:
            found_i = i + 1
            break
    if found_i != -1:
        output = base64.decodebytes(
            "\n".join(log_lines[found_i:]).encode("utf-8")
        ).decode("utf-8")
        try:
            return json.loads(output)
        except JSONDecodeError:
            return get_json_or_clean_str(output)
    else:
        return get_json_or_clean_str(log_string)


def telescope(
    *,
    organization: str,
    presigned_url: Union[str, None] = None,
) -> Union[Dict, str]:
    import requests
    from socket import gethostname
    import io
    import runpy
    from urllib.request import urlretrieve
    from contextlib import redirect_stdout, redirect_stderr
    from urllib.error import HTTPError
    from datetime import datetime, timezone

    aero_version = os.getenv("TELESCOPE_REPORT_RELEASE_VERSION", "latest")
    a = "airflow_report.pyz"
    aero_url = (
        "https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz"
        if aero_version == "latest"
        else f"https://github.com/astronomer/telescope/releases/download/{aero_version}/airflow_report.pyz"
    )
    try:
        urlretrieve(aero_url, a)
    except HTTPError as e:
        raise RuntimeError(
            f"Error finding specified version:{aero_version} -- Reason:{e.reason}"
        )

    s = io.StringIO()
    with redirect_stdout(s), redirect_stderr(s):
        runpy.run_path(a)
    report = {
        "telescope_version": "aeroscope-latest",
        "report_date": datetime.now(timezone.utc).isoformat()[:10],
        "organization_name": organization,
        "local": {
            gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}
        },
    }
    if presigned_url:
        try:
            upload = requests.put(
                presigned_url,
                data=json.dumps(report),
                timeout=30,
            )
            return upload.content, upload.status_code
        except requests.exceptions.ConnectionError as e:
            return str(e), 400
    return report
