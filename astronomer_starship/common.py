"""Common utilities for Astronomer Starship which are Airflow version agnostic."""

import json
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Union

from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from typing import Any, Dict, List, Tuple, TypedDict, Union

    class AttrDesc(TypedDict):
        attr: str
        """the name in the ORM, likely the same as the key"""

        methods: List[Tuple[str, bool]]
        """e.g. [("POST", True)] - if a given method shouldn't mention it, then it's omitted"""

        test_value: Any
        """any test value, for unit tests"""


logger = logging.getLogger(__name__)


class HttpError(Exception):
    """HTTP Error with status code"""

    def __init__(self, msg: str, status_code: int):
        self.msg = msg
        self.status_code = status_code


class NotFoundError(HttpError):
    """Not Found 404"""

    def __init__(self, msg: str):
        super().__init__(msg, 404)


class MethodNotAllowedError(HttpError):
    """Method Not Allowed 405"""

    def __init__(self, msg: str):
        super().__init__(msg, 405)


class ConflictError(HttpError):
    """Conflict 409"""

    def __init__(self, msg: str):
        super().__init__(msg, 409)


def get_json_or_clean_str(o: str) -> Union[List[Any], Dict[Any, Any], Any]:
    """For Aeroscope - Either load JSON (if we can) or strip and split the string, while logging the error"""
    import logging
    from json import JSONDecodeError

    try:
        return json.loads(o)
    except (JSONDecodeError, TypeError) as e:
        logging.debug(e)
        logging.debug(o)
        return o.strip()


def clean_airflow_report_output(log_string: str) -> Union[dict, str]:
    r"""For Aeroscope - Look for the magic string from the Airflow report and then decode the base64 and convert to json
    Or return output as a list, trimmed and split on newlines
    >>> clean_airflow_report_output("INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\naGVsbG8gd29ybGQ=")
    'hello world'
    >>> clean_airflow_report_output(
    ...     "INFO 123 - xyz - abc\n\n\nERROR - 1234\n%%%%%%%\neyJvdXRwdXQiOiAiaGVsbG8gd29ybGQifQ=="
    ... )
    {'output': 'hello world'}
    """
    import base64
    from json import JSONDecodeError

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


def telescope(
    *,
    organization: str,
    presigned_url: Union[str, None] = None,
) -> Union[Dict, str]:
    import io
    import runpy
    from contextlib import redirect_stderr, redirect_stdout
    from datetime import datetime, timezone
    from socket import gethostname
    from urllib.error import HTTPError
    from urllib.request import urlretrieve

    import requests

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
        raise RuntimeError(f"Error finding specified version:{aero_version} -- Reason:{e.reason}") from e

    s = io.StringIO()
    with redirect_stdout(s), redirect_stderr(s):
        runpy.run_path(a)
    report = {
        "telescope_version": "aeroscope-latest",
        "report_date": datetime.now(timezone.utc).isoformat()[:10],
        "organization_name": organization,
        "local": {gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}},
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


def get_from_request(args, json, key, required: bool = False) -> "Any":
    val = json.get(key, args.get(key))
    if val is None and required:
        raise RuntimeError(f"Missing required key: {key}")
    return val


def import_from_qualname(qualname: str) -> "Tuple[str, Any]":
    """Import a function or module from a qualified name
    :param qualname: The qualified name of the function or module to import (e.g. a.b.d.MyOperator or json)
    :return Tuple[str, Any]: The name of the function or module, and the function or module itself
    >>> import_from_qualname("json.loads")  # doctest: +ELLIPSIS
    ('loads', <function loads at ...>)
    >>> import_from_qualname("json")  # doctest: +ELLIPSIS
    ('json', <module 'json' from '...'>)
    """
    from importlib import import_module

    [module, name] = qualname.rsplit(".", 1) if "." in qualname else [qualname, qualname]
    imported_module = import_module(module)
    return (
        name,
        getattr(imported_module, name) if "." in qualname else imported_module,
    )


def get_kwargs_fn(request_method: str, args: dict, json: dict, attrs: dict):
    """
    Walks the attrs dict
    - if the request method is in the "methods" list we call get_from_request
        (which gets it from request.args or request.json, and throws if is_required and not given)
    - assuming we found it, we set it as the `attr_desc['attr']`
    - those kwargs later get passed directly to the function (e.g. set_pool)

    :param request_method: request.method
    :param args: request.args
    :param json: request.json
    :param attrs: the attrs to get from the request - e.g. StarshipAirflow27.dag_attrs()

    >>> get_kwargs_fn(
    ...     "POST",
    ...     {},
    ...     {"key": "key", "val": "val", "description": "description"},
    ...     StarshipAirflow.variable_attrs(),
    ... )  # get from request.json
    {'key': 'key', 'val': 'val', 'description': 'description'}
    >>> get_kwargs_fn(
    ...     "GET", {"dag_id": "foo"}, {}, StarshipAirflow.dag_runs_attrs()
    ... )  # with optional request.args, that don't exist, don't get passed through
    {'dag_id': 'foo'}
    >>> get_kwargs_fn(
    ...     "GET", {"dag_id": "foo", "limit": 5}, {}, StarshipAirflow.dag_runs_attrs()
    ... )  # with optional request.args, that exists, gets passed through
    {'dag_id': 'foo', 'limit': 5}
    """
    kwargs = {}
    for attr, attr_desc in attrs.items():
        for method_and_is_required in attr_desc["methods"]:
            (method, is_required) = method_and_is_required
            if request_method == method:
                key = attr_desc.get("attr") or attr
                val = get_from_request(args, json, attr, required=is_required)
                if val is not None:
                    kwargs[key] = val
    return kwargs


def results_to_list_via_attrs(results: "List[Any]", attrs: dict) -> "List[Dict[str, Any]]":
    """

    >>> class Foo:
    ...     def __init__(self, key, val):
    ...         self.key = key
    ...         self.val = val
    >>> results_to_list_via_attrs(
    ...     [Foo("key", "val")], {"key": {"attr": "key", "methods": [("POST", True)], "test_value": "key"}}
    ... )
    [{'key': 'key'}]
    """
    return json.loads(
        json.dumps(
            [
                {
                    attr: (getattr(result, attr_desc["attr"], None) if attr_desc["attr"] else None)
                    for attr, attr_desc in attrs.items()
                }
                for result in results
            ],
            default=str,
        )
    )


def generic_get_all(session: Session, qualname: str, attrs: dict) -> list:
    (_, thing_cls) = import_from_qualname(qualname)
    results = session.query(thing_cls).all()
    return results_to_list_via_attrs(results, attrs)


def generic_set_one(session: Session, qualname: str, attrs: dict, **kwargs):
    """
    :param session: The SQLAlchemy session
    :param qualname: The qualified name of the object to create
    :param attrs: attrs which inform what to return
    :param kwargs: The kwargs given to the created object
    """
    (_, thing_cls) = import_from_qualname(qualname)
    try:
        thing = thing_cls(**kwargs)
        session.add(thing)
        session.commit()
        return results_to_list_via_attrs([thing], attrs)[0]
    except Exception as e:
        session.rollback()
        raise e


def generic_delete(session: Session, qualname: str, **kwargs) -> None:
    from sqlalchemy import delete

    (_, thing_cls) = import_from_qualname(qualname)

    try:
        filters = [getattr(thing_cls, attr) == val for attr, val in kwargs.items()]
        deleted_rows = session.execute(delete(thing_cls).where(*filters)).rowcount
        session.commit()
        logger.info("Deleted %s rows for table %s", deleted_rows, qualname)
    except Exception as e:
        logger.error("Error deleting row(s) for table %s: %s", qualname, e)
        session.rollback()
        raise e


def get_test_data(attrs: dict, method: "Union[str, None]" = None) -> "Dict[str, Any]":
    """
    >>> get_test_data(
    ...     method="POST", attrs={"key": {"attr": "key", "methods": [("POST", True)], "test_value": "key"}}
    ... )
    {'key': 'key'}
    >>> get_test_data(method="PATCH", attrs=StarshipAirflow.dag_attrs())
    {'dag_id': 'dag_0', 'is_paused': False}
    >>> get_test_data(attrs=StarshipAirflow.dag_attrs())  # doctest: +ELLIPSIS
    {'dag_id': 'dag_0', 'schedule_interval': '@once', 'is_paused': False, ... 'dag_run_count': 0}
    """

    if method:
        return {
            attr: attr_desc["test_value"]
            for attr, attr_desc in attrs.items()
            if any(method == _method for (_method, _) in attr_desc["methods"])
        }
    else:
        return {attr: attr_desc["test_value"] for attr, attr_desc in attrs.items()}


def normalize_test_data(data: "Union[Dict, List]") -> "Union[Dict, List]":
    """

    >>> import datetime
    >>> normalize_test_data({"date": datetime.datetime(1970, 1, 1, 0, 0)})
    {'date': '1970-01-01 00:00:00'}
    >>> normalize_test_data([{"value": 123}])
    [{'value': 123}]
    """
    return json.loads(json.dumps(data, default=str))


# Keys: datetime values
_DATETIME_KEYS = {
    "queued_at",
    "logical_date",
    "start_date",
    "end_date",
    "data_interval_start",
    "data_interval_end",
    "run_after",
    "last_scheduling_decision",
    "execution_date",
    "queued_dttm",
    "scheduled_dttm",
}


def normalize_for_comparison(data: "Union[Dict, List]") -> "Union[Dict, List]":
    if isinstance(data, dict):
        result = {}
        for k, v in data.items():
            if k in _DATETIME_KEYS and isinstance(v, str):
                result[k] = v.replace("+00:00", "")
            else:
                result[k] = normalize_for_comparison(v)
        return result
    if isinstance(data, list):
        return [normalize_for_comparison(item) for item in data]
    return data


class BaseStarshipAirflow:
    """Base class for all Starship Airflow compatibility layers.

    It provides the common interface & functionality that can be used across different major Airflow versions.
    """

    def __init__(self):
        self._session = None

    @property
    def session(self) -> Session:
        from airflow.settings import Session

        if self._session is None:
            self._session = Session()
        return self._session

    @classmethod
    def get_airflow_version(cls):
        from airflow import __version__

        return __version__

    @classmethod
    def get_info(cls):
        from airflow import __version__ as airflow_version

        from astronomer_starship import __version__ as starship_version

        return {
            "airflow_version": airflow_version,
            "starship_version": starship_version,
        }

    @classmethod
    def get_env_vars(cls):
        return dict(os.environ)

    @classmethod
    def pool_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement pool_attrs method")

    def get_pools(self):
        return generic_get_all(self.session, "airflow.models.Pool", self.pool_attrs())

    def set_pool(self, **kwargs):
        return generic_set_one(self.session, "airflow.models.Pool", self.pool_attrs(), **kwargs)

    def delete_pool(self, **kwargs):
        attrs = {self.pool_attrs()[k]["attr"]: v for k, v in kwargs.items() if k in self.pool_attrs()}
        return generic_delete(self.session, "airflow.models.Pool", **attrs)

    @classmethod
    def variable_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement variable_attrs method")

    def get_variables(self):
        return generic_get_all(self.session, "airflow.models.Variable", self.variable_attrs())

    def set_variable(self, **kwargs):
        return generic_set_one(self.session, "airflow.models.Variable", self.variable_attrs(), **kwargs)

    def delete_variable(self, **kwargs):
        attrs = {self.variable_attrs()[k]["attr"]: v for k, v in kwargs.items()}
        return generic_delete(self.session, "airflow.models.Variable", **attrs)

    @classmethod
    def connection_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement connection_attrs method")

    def get_connections(self):
        return generic_get_all(self.session, "airflow.models.Connection", self.connection_attrs())

    def set_connection(self, **kwargs):
        return generic_set_one(self.session, "airflow.models.Connection", self.connection_attrs(), **kwargs)

    def delete_connection(self, **kwargs):
        attrs = {self.connection_attrs()[k]["attr"]: v for k, v in kwargs.items()}
        return generic_delete(self.session, "airflow.models.Connection", **attrs)

    @classmethod
    def dag_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement dag_attrs method")

    def get_dags(self):
        raise NotImplementedError("Subclasses must implement get_dags method")

    def set_dag_is_paused(self, **kwargs):
        raise NotImplementedError("Subclasses must implement set_dag_is_paused method")

    @classmethod
    def dag_runs_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement dag_runs_attrs method")

    def get_dag_runs(self):
        raise NotImplementedError("Subclasses must implement get_dag_runs method")

    def set_dag_runs(self, **kwargs):
        raise NotImplementedError("Subclasses must implement set_dag_runs method")

    def delete_dag_runs(self, **kwargs):
        raise NotImplementedError("Subclasses must implement delete_dag_runs method")

    @classmethod
    def task_instances_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement task_instances_attrs method")

    def get_task_instances(self):
        raise NotImplementedError("Subclasses must implement get_task_instances method")

    def set_task_instances(self, **kwargs):
        raise NotImplementedError("Subclasses must implement set_task_instances method")

    def get_task_instance_history(self):
        raise NotImplementedError("Subclasses must implement get_task_instance_history method")

    def set_task_instance_history(self, **kwargs):
        raise NotImplementedError("Subclasses must implement set_task_instance_history method")

    @classmethod
    def task_log_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement task_log_attrs method")

    def get_task_log(self):
        raise NotImplementedError("Subclasses must implement get_task_log method")

    def set_task_log(self, **kwargs):
        raise NotImplementedError("Subclasses must implement set_task_log method")

    def delete_task_log(self, **kwargs):
        raise NotImplementedError("Subclasses must implement delete_task_log method")

    @classmethod
    def xcom_attrs(cls) -> "Dict[str, AttrDesc]":
        raise NotImplementedError("Subclasses must implement xcom_attrs method")

    def get_xcom(self):
        raise NotImplementedError("Subclasses must implement get_xcom method")

    def set_xcom(self, **kwargs):
        raise NotImplementedError("Subclasses must implement set_xcom method")

    def delete_xcom(self, **kwargs):
        raise NotImplementedError("Subclasses must implement delete_xcom method")
