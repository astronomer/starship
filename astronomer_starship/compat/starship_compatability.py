import json
import logging
import os
from flask import jsonify, Response
from sqlalchemy.orm import Session
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, TypedDict, Tuple, Dict, List, Union
else:
    TypedDict = object
import datetime
import pytz


logger = logging.getLogger(__name__)


def get_from_request(args, json, key, required: bool = False) -> "Any":
    val = json.get(key, args.get(key))
    if val is None and required:
        raise RuntimeError(f"Missing required key: {key}")
    return val


def import_from_qualname(qualname: str) -> "Tuple[str, Any]":
    """Import a function or module from a qualified name
    :param qualname: The qualified name of the function or module to import (e.g. a.b.d.MyOperator or json)
    :return Tuple[str, Any]: The name of the function or module, and the function or module itself
    >>> import_from_qualname('json.loads') # doctest: +ELLIPSIS
    ('loads', <function loads at ...>)
    >>> import_from_qualname('json') # doctest: +ELLIPSIS
    ('json', <module 'json' from '...'>)
    """
    from importlib import import_module

    [module, name] = (
        qualname.rsplit(".", 1) if "." in qualname else [qualname, qualname]
    )
    imported_module = import_module(module)
    return (
        name,
        getattr(imported_module, name) if "." in qualname else imported_module,
    )


if TYPE_CHECKING:

    class AttrDesc(TypedDict):
        attr: str
        """the name in the ORM, likely the same as the key"""

        methods: List[Tuple[str, bool]]
        """e.g. [("POST", True)] - if a given method shouldn't mention it, then it's omitted"""

        test_value: Any
        """any test value, for unit tests"""


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
    ...   "POST", {}, {'key': 'key', 'val': 'val', 'description': 'description'}, StarshipAirflow.variable_attrs()
    ... )  # get from request.json
    {'key': 'key', 'val': 'val', 'description': 'description'}
    >>> get_kwargs_fn(
    ...   "GET", {"dag_id": "foo"}, {}, StarshipAirflow.dag_runs_attrs()
    ... )  # with optional request.args, that don't exist, don't get passed through
    {'dag_id': 'foo'}
    >>> get_kwargs_fn(
    ...   "GET", {"dag_id": "foo", "limit": 5}, {}, StarshipAirflow.dag_runs_attrs()
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


def results_to_list_via_attrs(
    results: "List[Any]", attrs: dict
) -> "List[Dict[str, Any]]":
    """

    >>> class Foo:
    ...   def __init__(self, key, val):
    ...     self.key = key
    ...     self.val = val
    >>> results_to_list_via_attrs(
    ...   [Foo("key", "val")],
    ...   {"key": {"attr": "key", "methods": [("POST", True)], "test_value": "key"}}
    ... )
    [{'key': 'key'}]
    """
    return json.loads(
        json.dumps(
            [
                {
                    attr: getattr(result, attr_desc["attr"], None)
                    if attr_desc["attr"]
                    else None
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


def generic_delete(session: Session, qualname: str, **kwargs) -> Response:
    from http import HTTPStatus
    from sqlalchemy import delete

    (_, thing_cls) = import_from_qualname(qualname)

    try:
        filters = [getattr(thing_cls, attr) == val for attr, val in kwargs.items()]
        deleted_rows = session.execute(delete(thing_cls).where(*filters)).rowcount
        session.commit()
        logger.info(f"Deleted {deleted_rows} rows for table {qualname}")
        return Response(status=HTTPStatus.NO_CONTENT)
    except Exception as e:
        logger.error(f"Error deleting row(s) for table {qualname}: {e}")
        session.rollback()
        raise e


def get_test_data(attrs: dict, method: "Union[str, None]" = None) -> "Dict[str, Any]":
    """
    >>> get_test_data(method="POST", attrs={"key": {"attr": "key", "methods": [("POST", True)], "test_value": "key"}})
    {'key': 'key'}
    >>> get_test_data(method="PATCH", attrs=StarshipAirflow.dag_attrs())
    {'dag_id': 'dag_0', 'is_paused': False}
    >>> get_test_data(attrs=StarshipAirflow.dag_attrs()) # doctest: +ELLIPSIS
    {'dag_id': 'dag_0', 'schedule_interval': '@once', 'is_paused': False, ... 'dag_run_count': 0}
    """

    if method:
        return {
            attr: attr_desc["test_value"]
            for attr, attr_desc in attrs.items()
            if any([method == _method for (_method, _) in attr_desc["methods"]])
        }
    else:
        return {attr: attr_desc["test_value"] for attr, attr_desc in attrs.items()}


class StarshipAirflow:
    """Base Class
    Contains methods that are expected to work across all Airflow versions
    When older versions require different behavior, they'll override this class
    and get created directly by StarshipCompatabilityLayer
    """

    def __init__(self):
        from airflow.settings import Session

        self.session = Session()

    @classmethod
    def get_airflow_version(cls):
        from airflow import __version__

        return __version__

    @classmethod
    def get_env_vars(cls):
        return dict(os.environ)

    @classmethod
    def set_env_vars(cls):
        """This is set directly at the Astro API, so return an error"""
        res = jsonify({"error": "Set via the Astro/Houston API"})
        res.status_code = 409
        raise NotImplementedError()

    @classmethod
    def delete_env_vars(cls):
        """This is not possible to do via API, so return an error"""
        res = jsonify({"error": "Not implemented"})
        res.status_code = 405
        raise NotImplementedError()

    @classmethod
    def variable_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "key": {
                "attr": "key",
                "methods": [("POST", True), ("DELETE", True)],
                "test_value": "key",
            },
            "val": {"attr": "val", "methods": [("POST", True)], "test_value": "val"},
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "description",
            },
        }

    def get_variables(self):
        return generic_get_all(
            self.session, "airflow.models.Variable", self.variable_attrs()
        )

    def set_variable(self, **kwargs):
        return generic_set_one(
            self.session, "airflow.models.Variable", self.variable_attrs(), **kwargs
        )

    def delete_variable(self, **kwargs):
        attrs = {self.variable_attrs()[k]["attr"]: v for k, v in kwargs.items()}
        return generic_delete(self.session, "airflow.models.Variable", **attrs)

    @classmethod
    def pool_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "name": {
                "attr": "pool",
                "methods": [("POST", True), ("DELETE", True)],
                "test_value": "test_name",
            },
            "slots": {"attr": "slots", "methods": [("POST", True)], "test_value": 1},
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "test_description",
            },
        }

    def get_pools(self):
        return generic_get_all(self.session, "airflow.models.Pool", self.pool_attrs())

    def set_pool(self, **kwargs):
        return generic_set_one(
            self.session, "airflow.models.Pool", self.pool_attrs(), **kwargs
        )

    def delete_pool(self, **kwargs):
        attrs = {
            self.pool_attrs()[k]["attr"]: v
            for k, v in kwargs.items()
            if k in self.pool_attrs()
        }
        return generic_delete(self.session, "airflow.models.Pool", **attrs)

    @classmethod
    def connection_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "conn_id": {
                "attr": "conn_id",
                "methods": [("POST", True), ("DELETE", True)],
                "test_value": "conn_id",
            },
            "conn_type": {
                "attr": "conn_type",
                "methods": [("POST", True)],
                "test_value": "conn_type",
            },
            "host": {
                "attr": "host",
                "methods": [("POST", False)],
                "test_value": "host",
            },
            "port": {
                "attr": "port",
                "methods": [("POST", False)],
                "test_value": 1234,
            },
            "schema": {
                "attr": "schema",
                "methods": [("POST", False)],
                "test_value": "schema",
            },
            "login": {
                "attr": "login",
                "methods": [("POST", False)],
                "test_value": "login",
            },
            "password": {  # pragma: allowlist secret
                "attr": "password",  # pragma: allowlist secret
                "methods": [("POST", False)],
                "test_value": "password",  # pragma: allowlist secret
            },
            "extra": {
                "attr": "extra",
                "methods": [("POST", False)],
                "test_value": "extra",
            },
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "description",
            },
        }

    def get_connections(self):
        return generic_get_all(
            self.session, "airflow.models.Connection", self.connection_attrs()
        )

    def set_connection(self, **kwargs):
        return generic_set_one(
            self.session, "airflow.models.Connection", self.connection_attrs(), **kwargs
        )

    def delete_connection(self, **kwargs):
        attrs = {self.connection_attrs()[k]["attr"]: v for k, v in kwargs.items()}
        return generic_delete(self.session, "airflow.models.Connection", **attrs)

    @classmethod
    def dag_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "dag_id": {
                "attr": "dag_id",
                "methods": [("PATCH", True)],
                "test_value": "dag_0",
            },
            "schedule_interval": {
                "attr": "schedule_interval",
                "methods": [],
                "test_value": "@once",
            },
            "is_paused": {
                "attr": "is_paused",
                "methods": [("PATCH", True)],
                "test_value": False,
            },
            "fileloc": {
                "attr": "fileloc",
                "methods": [],
                "test_value": "fileloc",
            },
            "description": {
                "attr": "description",
                "methods": [],
                "test_value": None,
            },
            "owners": {
                "attr": "owners",
                "methods": [],
                "test_value": "baz",
            },
            "tags": {
                "attr": None,
                "methods": [],
                "test_value": ["bar", "foo"],
            },
            "dag_run_count": {
                "attr": None,
                "methods": [],
                "test_value": 0,
            },
        }

    def get_dags(self):
        """Get all DAGs"""
        from airflow.models import DagModel

        try:
            fields = [
                getattr(DagModel, attr_desc["attr"])
                for attr_desc in self.dag_attrs().values()
                if attr_desc["attr"] is not None
            ]
            # py36/sqlalchemy1.3 doesn't like label?
            # noinspection PyUnresolvedReferences
            return json.loads(
                json.dumps(
                    [
                        {
                            attr: (
                                self._get_tags(result.dag_id)
                                if attr == "tags"
                                else self._get_dag_run_count(result.dag_id)
                                if attr == "dag_run_count"
                                else getattr(result, attr_desc["attr"], None)
                                # e.g. result.dag_id
                            )
                            for attr, attr_desc in self.dag_attrs().items()
                        }
                        for result in self.session.query(*fields).all()
                    ],
                    default=str,
                )
            )
        except Exception as e:
            self.session.rollback()
            raise e

    def set_dag_is_paused(self, dag_id: str, is_paused: bool):
        """Pause or unpause a DAG"""
        from airflow.models import DagModel
        from sqlalchemy import update

        try:
            self.session.execute(
                update(DagModel)
                .where(DagModel.dag_id == dag_id)
                .values(is_paused=is_paused)
            )
            self.session.commit()
            return {
                "dag_id": dag_id,
                "is_paused": is_paused,
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def _get_tags(self, dag_id: str):
        try:
            from airflow.models import DagTag

            # noinspection PyTypeChecker
            return [
                tag[0]
                for tag in self.session.query(DagTag.name)
                .filter(DagTag.dag_id == dag_id)
                .all()
            ]
        except ImportError:
            return []
        except Exception as e:
            self.session.rollback()
            raise e

    def _get_dag_run_count(self, dag_id: str):
        from airflow.models import DagRun
        from sqlalchemy.sql.functions import count
        from sqlalchemy import distinct

        try:
            # py36/sqlalchemy1.3 doesn't like label?
            # noinspection PyTypeChecker
            return (
                self.session.query(count(distinct(DagRun.run_id)))
                .filter(DagRun.dag_id == dag_id)
                .one()[0]
            )
        except Exception as e:
            self.session.rollback()
            raise e

    @classmethod
    def dag_runs_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch = epoch.replace(tzinfo=pytz.utc)
        return {
            "dag_id": {
                "attr": "dag_id",
                "methods": [("GET", True), ("DELETE", True)],
                "test_value": "dag_0",
            },
            # Limit is the number of rows to return.
            "limit": {
                "attr": None,
                "methods": [("GET", False)],
                "test_value": 10,
            },
            # Offset is the number of rows in the result set to skip before beginning to return rows.
            "offset": {
                "attr": None,
                "methods": [("GET", False)],
                "test_value": 0,
            },
            "dag_runs": {
                "attr": "dag_runs",
                "methods": [("POST", True)],
                "test_value": [
                    {
                        "dag_id": "dag_0",
                        "run_id": "manual__1970-01-01T00:00:00+00:00",
                        "queued_at": epoch,
                        "execution_date": epoch,
                        "start_date": epoch,
                        "end_date": epoch,
                        "state": "SUCCESS",
                        "creating_job_id": 123,
                        "external_trigger": True,
                        "run_type": "manual",
                        "conf": {"my_param": "my_value"},
                        "data_interval_start": epoch,
                        "data_interval_end": epoch,
                        "last_scheduling_decision": epoch,
                        "dag_hash": "dag_hash",
                    }
                ],
            },
        }

    @classmethod
    def dag_run_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch = epoch.replace(tzinfo=pytz.utc)
        # epoch = str(epoch).replace(' ', 'T')
        return {
            "dag_id": {
                "attr": "dag_id",
                "methods": [("POST", True)],
                "test_value": "dag_0",
            },
            "run_id": {
                "attr": "run_id",
                "methods": [("POST", True)],
                "test_value": "manual__1970-01-01T00:00:00+00:00",
            },
            "queued_at": {
                "attr": "queued_at",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "execution_date": {
                "attr": "execution_date",
                "methods": [("POST", True)],
                "test_value": epoch,
            },
            "start_date": {
                "attr": "start_date",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "end_date": {
                "attr": "end_date",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "state": {
                "attr": "state",
                "methods": [("POST", False)],
                "test_value": "SUCCESS",
            },
            "creating_job_id": {
                "attr": "creating_job_id",
                "methods": [("POST", False)],
                "test_value": 123,
            },
            "external_trigger": {
                "attr": "external_trigger",
                "methods": [("POST", False)],
                "test_value": True,
            },
            "run_type": {
                "attr": "run_type",
                "methods": [("POST", True)],
                "test_value": "manual",
            },
            "conf": {
                "attr": "conf",
                "methods": [("POST", False)],
                "test_value": {"my_param": "my_value"},
            },
            "data_interval_start": {
                "attr": "data_interval_start",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "data_interval_end": {
                "attr": "data_interval_end",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "last_scheduling_decision": {
                "attr": "last_scheduling_decision",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "dag_hash": {
                "attr": "dag_hash",
                "methods": [("POST", False)],
                "test_value": "dag_hash",
            },
        }

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        from sqlalchemy import desc
        from airflow.models import DagRun

        try:
            query = (
                self.session.query(DagRun)
                .filter(DagRun.dag_id == dag_id)
                .order_by(desc(DagRun.start_date))
            )
            if offset:
                query = query.offset(offset)
            results = query.limit(limit).all()
            return {
                "dag_runs": results_to_list_via_attrs(results, self.dag_run_attrs()),
                "dag_run_count": self._get_dag_run_count(dag_id),
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def set_dag_runs(self, dag_runs: list):
        dag_id = dag_runs[0]["dag_id"]
        dag_runs = self.insert_directly("dag_run", dag_runs)
        return {"dag_runs": dag_runs, "dag_run_count": self._get_dag_run_count(dag_id)}

    def delete_dag_runs(self, **kwargs):
        attrs = {self.dag_runs_attrs()[k]["attr"]: v for k, v in kwargs.items()}
        return generic_delete(self.session, "airflow.models.DagRun", **attrs)

    @classmethod
    def task_instances_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch_tz = epoch.replace(tzinfo=pytz.utc)
        # epoch = str(epoch).replace(' ', 'T')
        # epoch_tz = str(epoch_tz).replace(' ', 'T')
        return {
            "dag_id": {
                "attr": "dag_id",
                "methods": [("GET", True), ("DELETE", True)],
                "test_value": "dag_0",
            },
            # Limit is the number of rows to return.
            "limit": {
                "attr": None,
                "methods": [("GET", False)],
                "test_value": 10,
            },
            # Offset is the number of rows in the result set to skip before beginning to return rows.
            "offset": {
                "attr": None,
                "methods": [("GET", False)],
                "test_value": 0,
            },
            "task_instances": {
                "attr": None,
                "methods": [("POST", True)],
                "test_value": [
                    {
                        "dag_id": "dag_0",
                        "run_id": "manual__1970-01-01T00:00:00+00:00",
                        "task_id": "task_id",
                        "map_index": -1,
                        "try_number": 0,
                        "start_date": epoch_tz,
                        "end_date": epoch_tz,
                        "duration": 1.0,
                        "state": "SUCCESS",
                        "max_tries": 2,
                        "hostname": "hostname",
                        "unixname": "unixname",
                        "job_id": 3,
                        "pool": "pool",
                        "pool_slots": 4,
                        "queue": "queue",
                        "priority_weight": 5,
                        "operator": "operator",
                        "queued_dttm": epoch_tz,
                        "queued_by_job_id": 6,
                        "pid": 7,
                        "external_executor_id": "external_executor_id",
                        "trigger_id": None,
                        "trigger_timeout": epoch_tz,
                        "executor_config": "\x80\x04}\x94.",
                        # "next_method": "next_method",
                        # "next_kwargs": {},
                    }
                ],
            },
        }

    @classmethod
    def task_instance_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch_tz = epoch.replace(tzinfo=pytz.utc)
        # epoch = str(epoch).replace(' ', 'T')
        # epoch_tz = str(epoch_tz).replace(' ', 'T')
        return {
            "dag_id": {
                "attr": "dag_id",
                "methods": [("POST", True)],
                "test_value": "dag_0",
            },
            "run_id": {
                "attr": "run_id",
                "methods": [("POST", True)],
                "test_value": "manual__1970-01-01T00:00:00+00:00",
            },
            "task_id": {
                "attr": "task_id",
                "methods": [("POST", True)],
                "test_value": "task_id",
            },
            "map_index": {
                "attr": "map_index",
                "methods": [("POST", True)],
                "test_value": -1,
            },
            "try_number": {
                "attr": "_try_number",
                "methods": [("POST", True)],
                "test_value": 0,
            },
            "start_date": {
                "attr": "start_date",
                "methods": [("POST", False)],
                "test_value": epoch_tz,
            },
            "end_date": {
                "attr": "end_date",
                "methods": [("POST", False)],
                "test_value": epoch_tz,
            },
            "duration": {
                "attr": "duration",
                "methods": [("POST", False)],
                "test_value": 1.0,
            },
            "state": {
                "attr": "state",
                "methods": [("POST", False)],
                "test_value": "SUCCESS",
            },
            "max_tries": {
                "attr": "max_tries",
                "methods": [("POST", False)],
                "test_value": 2,
            },
            "hostname": {
                "attr": "hostname",
                "methods": [("POST", False)],
                "test_value": "hostname",
            },
            "unixname": {
                "attr": "unixname",
                "methods": [("POST", False)],
                "test_value": "unixname",
            },
            "job_id": {
                "attr": "job_id",
                "methods": [("POST", False)],
                "test_value": 3,
            },
            "pool": {
                "attr": "pool",
                "methods": [("POST", True)],
                "test_value": "pool",
            },
            "pool_slots": {
                "attr": "pool_slots",
                "methods": [("POST", True)],
                "test_value": 4,
            },
            "queue": {
                "attr": "queue",
                "methods": [("POST", False)],
                "test_value": "queue",
            },
            "priority_weight": {
                "attr": "priority_weight",
                "methods": [("POST", False)],
                "test_value": 5,
            },
            "operator": {
                "attr": "operator",
                "methods": [("POST", False)],
                "test_value": "operator",
            },
            "queued_dttm": {
                "attr": "queued_dttm",
                "methods": [("POST", False)],
                "test_value": epoch_tz,
            },
            "queued_by_job_id": {
                "attr": "queued_by_job_id",
                "methods": [("POST", False)],
                "test_value": 6,
            },
            "pid": {
                "attr": "pid",
                "methods": [("POST", False)],
                "test_value": 7,
            },
            "external_executor_id": {
                "attr": "external_executor_id",
                "methods": [("POST", False)],
                "test_value": "external_executor_id",
            },
            "trigger_id": {
                "attr": "trigger_id",
                "methods": [("POST", False)],
                "test_value": None,
            },
            "trigger_timeout": {
                "attr": "trigger_timeout",
                "methods": [("POST", False)],
                "test_value": epoch_tz,
            },
            "executor_config": {
                "attr": None,  # "executor_config",
                "methods": [("POST", False)],
                "test_value": "\x80\x04}\x94.",
            },
            # Exception:
            # /airflow/serialization/serialized_objects.py\", line 521, in deserialize
            # KeyError: <Encoding.VAR: '__var'>
            # "next_method": task_instance.next_method,
            # "next_kwargs": task_instance.next_kwargs,
            # ????
            # "next_method": {
            #     "attr": None,
            #     "methods": [("GET", False), ("POST", True)],
            #     "test_value": "next_method",
            # },
            # "next_kwargs": {
            #     "attr": None,
            #     "methods": [("GET", False), ("POST", True)],
            #     "test_value": {},
            # },
        }

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        from sqlalchemy import desc
        from airflow.models import DagRun, TaskInstance
        from sqlalchemy.orm import load_only

        try:
            # py36/sqlalchemy1.3 doesn't query(Table.column)
            # noinspection PyTypeChecker
            sub_query = (
                self.session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id)
                .order_by(desc(DagRun.start_date))
                .limit(limit)
            )
            if offset:
                sub_query = sub_query.offset(offset)
            sub_query = sub_query.subquery()

            # .in_ doesn't seem to get recognized by type checkers
            # noinspection PyUnresolvedReferences
            results = (
                self.session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_id)
                .filter(TaskInstance.run_id.in_(sub_query))
                .options(
                    load_only(
                        *[
                            attr_desc["attr"]
                            for attr, attr_desc in self.task_instance_attrs().items()
                            if attr_desc["attr"] is not None
                        ]
                    )
                )
                .order_by(desc(TaskInstance.start_date))
                .all()
            )
            return {
                "task_instances": results_to_list_via_attrs(
                    results, self.task_instance_attrs()
                ),
                "dag_run_count": self._get_dag_run_count(dag_id),
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def set_task_instances(self, task_instances: list):
        """These need to be inserted directly to skip TaskInstance.__init__"""
        task_instances = self.insert_directly("task_instance", task_instances)
        return {"task_instances": task_instances}

    def delete_task_instances(self, **kwargs):
        attrs = {self.task_instances_attrs()[k]["attr"]: v for k, v in kwargs.items()}
        return generic_delete(self.session, "airflow.models.TaskInstance", **attrs)

    def insert_directly(self, table_name, items):
        from sqlalchemy.exc import InvalidRequestError
        from sqlalchemy import MetaData
        import pickle

        if not items:
            return []

        # Clean data before inserting
        for item in items:
            for k in ["conf", "id", "executor_config"]:
                if k not in item:
                    continue
                # drop executor_config, because its original type may have gotten lost
                # and pickling it will not recover it
                if k == "executor_config":
                    item[k] = pickle.dumps({})
                # use pickle to insert conf as binary JSONB
                # this works because the dagrun conf is always a JSON-serializable dict
                elif k == "conf":
                    item[k] = pickle.dumps(item[k])
                else:
                    del item[k]
        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=[table_name])
            table = metadata.tables[table_name]
            self.session.execute(table.insert().values(items))
            self.session.commit()
            for item in items:
                if "conf" in item:
                    # we don't want to return conf in pickled form
                    # this also makes tests happy
                    item["conf"] = pickle.loads(item["conf"])
            return items
        except (InvalidRequestError, KeyError):
            return self.insert_directly(f"airflow.{table_name}", items)
        except Exception as e:
            self.session.rollback()
            raise e


class StarshipAirflow22(StarshipAirflow):
    def task_instance_attrs(self):
        attrs = super().task_instance_attrs()
        if "map_index" in attrs:
            del attrs["map_index"]
        return attrs

    def task_instances_attrs(self):
        attrs = super().task_instances_attrs()
        if "map_index" in attrs["task_instances"]["test_value"][0]:
            del attrs["task_instances"]["test_value"][0]["map_index"]
        return attrs


class StarshipAirflow21(StarshipAirflow22):
    def dag_runs_attrs(self):
        attrs = super().dag_runs_attrs()
        # data_interval_end, data_interval_start
        if "data_interval_start" in attrs["dag_runs"]["test_value"][0]:
            del attrs["dag_runs"]["test_value"][0]["data_interval_start"]
        if "data_interval_end" in attrs["dag_runs"]["test_value"][0]:
            del attrs["dag_runs"]["test_value"][0]["data_interval_end"]
        return attrs

    def dag_run_attrs(self):
        attrs = super().dag_run_attrs()
        if "data_interval_start" in attrs:
            del attrs["data_interval_start"]
        if "data_interval_end" in attrs:
            del attrs["data_interval_end"]
        return attrs

    def task_instances_attrs(self):
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch_tz = epoch.replace(tzinfo=pytz.utc)
        attrs = super().task_instances_attrs()
        if "trigger_id" in attrs["task_instances"]["test_value"][0]:
            del attrs["task_instances"]["test_value"][0]["trigger_id"]
        if "trigger_timeout" in attrs["task_instances"]["test_value"][0]:
            del attrs["task_instances"]["test_value"][0]["trigger_timeout"]
        if "run_id" in attrs["task_instances"]["test_value"][0]:
            del attrs["task_instances"]["test_value"][0]["run_id"]
        attrs["task_instances"]["test_value"][0]["execution_date"] = epoch_tz
        return attrs

    def task_instance_attrs(self):
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch_tz = epoch.replace(tzinfo=pytz.utc)
        attrs = super().task_instance_attrs()
        if "trigger_id" in attrs:
            del attrs["trigger_id"]
        if "trigger_timeout" in attrs:
            del attrs["trigger_timeout"]
        if "run_id" in attrs:
            del attrs["run_id"]
        attrs["execution_date"] = {
            "attr": "execution_date",
            "methods": [("POST", True)],
            "test_value": epoch_tz,
        }
        return attrs

    # noinspection DuplicatedCode
    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        """Task Instance doesn't have run_id in AF2.1"""
        from sqlalchemy import desc
        from airflow.models import DagRun, TaskInstance
        from sqlalchemy.orm import load_only

        try:
            # noinspection PyTypeChecker
            sub_query = (
                self.session.query(DagRun.execution_date)
                .filter(DagRun.dag_id == dag_id)
                .order_by(desc(DagRun.start_date))
                .limit(limit)
            )
            if offset:
                sub_query = sub_query.offset(offset)
            sub_query = sub_query.subquery()

            # noinspection PyUnresolvedReferences
            results = (
                self.session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_id)
                .filter(TaskInstance.execution_date.in_(sub_query))
                .options(
                    load_only(
                        *[
                            attr_desc["attr"]
                            for attr, attr_desc in self.task_instance_attrs().items()
                            if attr_desc["attr"] is not None
                        ]
                    )
                )
                .order_by(desc(TaskInstance.start_date))
                .all()
            )
            return {
                "task_instances": results_to_list_via_attrs(
                    results, self.task_instance_attrs()
                ),
                "dag_run_count": self._get_dag_run_count(dag_id),
            }
        except Exception as e:
            self.session.rollback()
            raise e


class StarshipAirflow20(StarshipAirflow21):
    """
    - description does not exist in variables
    - queued_at not on dag_run
    """

    def variable_attrs(self):
        attrs = super().variable_attrs()
        del attrs["description"]
        return attrs

    def dag_runs_attrs(self):
        attrs = super().dag_runs_attrs()
        if "queued_at" in attrs["dag_runs"]["test_value"][0]:
            del attrs["dag_runs"]["test_value"][0]["queued_at"]
        return attrs

    def dag_run_attrs(self):
        attrs = super().dag_run_attrs()
        if "queued_at" in attrs:
            del attrs["queued_at"]
        return attrs


class StarshipAirflow27(StarshipAirflow):
    """
    - include_deferred is required in pools
    """

    def pool_attrs(self):
        attrs = super().pool_attrs()
        attrs["include_deferred"] = {
            "attr": "include_deferred",
            "methods": [("POST", True)],
            "test_value": True,
        }
        return attrs

    def task_instance_attrs(self):
        attrs = super().task_instance_attrs()
        attrs["custom_operator_name"] = {
            "attr": "custom_operator_name",
            "methods": [("POST", True)],
            "test_value": None,
        }
        return attrs

    def task_instances_attrs(self):
        attrs = super().task_instances_attrs()
        attrs["task_instances"]["test_value"][0]["custom_operator_name"] = None
        return attrs


class StarshipAirflow28(StarshipAirflow27):
    """
    - clear_number is required in dag_run
    """

    def dag_runs_attrs(self):
        attrs = super().dag_runs_attrs()
        attrs["dag_runs"]["test_value"][0]["clear_number"] = 0
        return attrs

    def dag_run_attrs(self):
        attrs = super().dag_run_attrs()
        attrs["clear_number"] = {
            "attr": "clear_number",
            "methods": [("POST", True)],
            "test_value": 0,
        }
        return attrs


class StarshipAirflow29(StarshipAirflow28):
    """
    - rendered_map_index in task_instance
    - task_display_name in task_instance
    """

    def task_instance_attrs(self):
        attrs = super().task_instance_attrs()
        attrs["rendered_map_index"] = {
            "attr": "rendered_map_index",
            "methods": [("POST", True)],
            "test_value": "rendered_map_index",
        }
        attrs["task_display_name"] = {
            "attr": "task_display_name",
            "methods": [("POST", True)],
            "test_value": "task_display_name",
        }
        return attrs

    def task_instances_attrs(self):
        attrs = super().task_instances_attrs()
        attrs["task_instances"]["test_value"][0][
            "rendered_map_index"
        ] = "rendered_map_index"
        attrs["task_instances"]["test_value"][0][
            "task_display_name"
        ] = "task_display_name"
        return attrs


class StarshipAirflow210(StarshipAirflow29):
    """
    - _try_number to try_number in task_instance
    - executor in task_instance
    """

    # TODO: Identify any other compat issues that exist between 2.9-2.10

    def task_instance_attrs(self):
        attrs = super().task_instance_attrs()
        attrs["try_number"]["attr"] = "try_number"
        attrs["executor"] = {
            "attr": "executor",
            "methods": [("POST", True)],
            "test_value": "executor",
        }
        return attrs

    def task_instances_attrs(self):
        attrs = super().task_instances_attrs()
        attrs["task_instances"]["test_value"][0]["executor"] = "executor"
        return attrs


class StarshipCompatabilityLayer:
    """StarshipCompatabilityLayer is a factory class that returns the correct StarshipAirflow class for a version

    - 1.8 https://github.com/apache/airflow/blob/1.8.2/airflow/models.py
    - 1.10 https://github.com/apache/airflow/blob/1.10.15/airflow/models
    - 2.0 https://github.com/apache/airflow/tree/2.0.2/airflow/models
    - 2.1 https://github.com/apache/airflow/tree/2.1.4/airflow/models
    - 2.2 https://github.com/apache/airflow/tree/2.2.5/airflow/models
    - 2.3 https://github.com/apache/airflow/blob/2.3.4/airflow/models
    - 2.4 https://github.com/apache/airflow/blob/2.4.3/airflow/models
    - 2.5 https://github.com/apache/airflow/tree/2.5.3/airflow/models
    - 2.6 https://github.com/apache/airflow/tree/2.6.3/airflow/models
    - 2.7 https://github.com/apache/airflow/tree/2.7.3/airflow/models
    - 2.8 https://github.com/apache/airflow/tree/2.8.3/airflow/models
    - 2.9 https://github.com/apache/airflow/tree/2.9.3/airflow/models
    - 2.10 https://github.com/apache/airflow/tree/2.10.3/airflow/models

    >>> isinstance(StarshipCompatabilityLayer("2.8.1"), StarshipAirflow28)
    True
    >>> StarshipCompatabilityLayer("1.0.0")
    Traceback (most recent call last):
    RuntimeError: Unsupported Airflow Version: 1.0.0
    >>> StarshipCompatabilityLayer("2.0")
    Traceback (most recent call last):
    RuntimeError: Unsupported Airflow Version - must be semver x.y.z: 2.0
    >>> StarshipCompatabilityLayer("") # doctest: +ELLIPSIS
    Traceback (most recent call last):
    RuntimeError: Unsupported Airflow Version - must be semver x.y.z:...
    """

    def __new__(cls, airflow_version: "Union[str, None]" = None) -> StarshipAirflow:
        if airflow_version is None:
            from airflow import __version__

            airflow_version = __version__
            print("Got Airflow Version: " + airflow_version)
        try:
            [major, minor, _] = airflow_version.split(".", maxsplit=2)
        except ValueError:
            raise RuntimeError(
                f"Unsupported Airflow Version - must be semver x.y.z: {airflow_version}"
            )

        if int(major) == 2:
            if int(minor) == 10:
                return StarshipAirflow210()
            if int(minor) == 9:
                return StarshipAirflow29()
            if int(minor) == 8:
                return StarshipAirflow28()
            if int(minor) == 7:
                return StarshipAirflow27()
            if int(minor) == 2:
                return StarshipAirflow22()
            if int(minor) == 1:
                return StarshipAirflow21()
            if int(minor) == 0:
                return StarshipAirflow20()
            return StarshipAirflow()
        else:
            raise RuntimeError(f"Unsupported Airflow Version: {airflow_version}")
