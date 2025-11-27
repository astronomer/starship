import datetime
import json
import logging
from typing import TYPE_CHECKING

import pytz

from astronomer_starship.common import (
    BaseStarshipAirflow,
    generic_delete,
    results_to_list_via_attrs,
)

if TYPE_CHECKING:
    from typing import Dict, Union

    from astronomer_starship.common import AttrDesc


logger = logging.getLogger(__name__)


class StarshipAirflow(BaseStarshipAirflow):
    """Base class for Airflow 3.x compatibility layers.

    This class should not implement version-specific logic; that should be done in subclasses.
    """


class StarshipAirflow30(StarshipAirflow):
    """Airflow 3.0 compatibility layer."""

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
            "include_deferred": {
                "attr": "include_deferred",
                "methods": [("POST", True)],
                "test_value": True,
            },
        }

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
                "test_value": "{}",
            },
            "description": {
                "attr": "description",
                "methods": [("POST", False)],
                "test_value": "description",
            },
        }

    @classmethod
    def dag_attrs(cls) -> "Dict[str, AttrDesc]":
        return {
            "dag_id": {
                "attr": "dag_id",
                "methods": [("PATCH", True)],
                "test_value": "dag_0",
            },
            "timetable_summary": {
                "attr": "timetable_summary",
                "methods": [],
                "test_value": "@once",
            },
            "is_paused": {
                "attr": "is_paused",
                "methods": [("PATCH", True)],
                "test_value": True,
            },
            "fileloc": {
                "attr": "fileloc",
                "methods": [],
                "test_value": "fileloc",  # removed in test
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
                "test_value": ["bar", "foo"],  # Unpredictable order, removed in test
            },
            "dag_run_count": {
                "attr": None,
                "methods": [],
                "test_value": 0,
            },
        }

    def get_dags(self):
        from airflow.models import DagModel

        try:
            fields = [
                getattr(DagModel, attr_desc["attr"])
                for attr_desc in self.dag_attrs().values()
                if attr_desc["attr"] is not None
            ]
            
            return json.loads(
                json.dumps(
                    [
                        {
                            attr: (
                                self._get_tags(result.dag_id)
                                if attr == "tags"
                                else (
                                    self._get_dag_run_count(result.dag_id)
                                    if attr == "dag_run_count"
                                    else getattr(result, attr_desc["attr"], None)
                                )
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
        from airflow.models import DagModel
        from sqlalchemy import update

        try:
            self.session.execute(
                update(DagModel).where(DagModel.dag_id == dag_id).values(is_paused=is_paused)
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

            return [
                tag[0] 
                for tag in self.session.query(DagTag.name).filter(DagTag.dag_id == dag_id).all()
            ]
        except ImportError:
            # DagTag might not be available
            return []
        except Exception as e:
            self.session.rollback()
            raise e

    def _get_dag_run_count(self, dag_id: str):
        from airflow.models import DagRun
        from sqlalchemy import distinct
        from sqlalchemy.sql.functions import count

        try:
            return self.session.query(
                count(distinct(DagRun.run_id))
            ).filter(DagRun.dag_id == dag_id).one()[0]
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
            "limit": {
                "attr": None,
                "methods": [("GET", False)],
                "test_value": 10,
            },
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
                        "logical_date": epoch,  # AF3: renamed from execution_date
                        "run_after": epoch,  # AF3: new required field
                        "start_date": epoch,
                        "end_date": epoch,
                        "state": "success",
                        "run_type": "manual",
                        "creating_job_id": 123,
                        "conf": {"my_param": "my_value"},
                        "data_interval_start": epoch,
                        "data_interval_end": epoch,
                        "last_scheduling_decision": epoch,
                        "triggered_by": "cli",  # AF3: replaces external_trigger
                        "created_dag_version_id": None,  # AF3: new UUID field
                        "backfill_id": None,  # AF3: new field linking to backfill table
                        "bundle_version": None,  # AF3: new field for DAG bundle versioning
                    }
                ],
            },
        }

    @classmethod
    def dag_run_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch = epoch.replace(tzinfo=pytz.utc)
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
            "logical_date": {
                "attr": "logical_date",
                "methods": [("POST", False)],
                "test_value": epoch,
            },
            "run_after": {
                "attr": "run_after",
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
                "test_value": "success",
            },
            "run_type": {
                "attr": "run_type",
                "methods": [("POST", True)],
                "test_value": "manual",
            },
            "creating_job_id": {
                "attr": "creating_job_id",
                "methods": [("POST", False)],
                "test_value": 123,
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
            "triggered_by": {
                "attr": "triggered_by",
                "methods": [("POST", False)],
                "test_value": "cli",
            },
            "created_dag_version_id": {
                "attr": "created_dag_version_id",
                "methods": [("POST", False)],
                "test_value": None,
            },
            "backfill_id": {
                "attr": "backfill_id",
                "methods": [("POST", False)],
                "test_value": None,
            },
            "bundle_version": {
                "attr": "bundle_version",
                "methods": [("POST", False)],
                "test_value": None,
            },
        }

    def get_dag_runs(self, dag_id: str, offset: int = 0, limit: int = 10) -> dict:
        from airflow.models import DagRun
        from sqlalchemy import MetaData, String, desc, select

        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=['dag_run'])
            table = metadata.tables['dag_run']
            
            # enum validation
            if 'triggered_by' in table.c:
                table.c.triggered_by.type = String(50)
            
            stmt = select(table).where(table.c.dag_id == dag_id).order_by(desc(table.c.start_date))
            if offset:
                stmt = stmt.offset(offset)
            stmt = stmt.limit(limit)
            
            result = self.session.execute(stmt)
            dag_runs_data = [dict(row._mapping) for row in result]
            
            return json.loads(
                json.dumps(
                    {
                        "dag_runs": dag_runs_data,
                        "dag_run_count": self._get_dag_run_count(dag_id),
                    },
                    default=str,
                )
            )
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


class StarshipAirflow31(StarshipAirflow30):
    """Airflow 3.1 compatibility layer."""

    @classmethod
    def pool_attrs(cls) -> "Dict[str, AttrDesc]":
        attrs = super().pool_attrs()
        # TODO: add support for Teams? (added in 3.1)
        # attrs["team_id"] = {
        #     "attr": "team_id",
        #     "methods": [("POST", True)],
        #     "test_value": 123,
        # }
        return attrs

    @classmethod
    def variable_attrs(cls) -> "Dict[str, AttrDesc]":
        attrs = super().variable_attrs()
        # TODO: add support for Teams? (added in 3.1)
        # attrs["team_id"] = {
        #     "attr": "team_id",
        #     "methods": [("POST", True)],
        #     "test_value": 123,
        # }
        return attrs

    @classmethod
    def connection_attrs(cls) -> "Dict[str, AttrDesc]":
        attrs = super().connection_attrs()
        # TODO: add support for Teams? (added in 3.1)
        # attrs["team_id"] = {
        #     "attr": "team_id",
        #     "methods": [("POST", True)],
        #     "test_value": 123,
        # }
        return attrs


class StarshipCompatabilityLayer:
    """StarshipCompatabilityLayer is a factory class that returns the correct StarshipAirflow class for a version

    - 3.0 https://github.com/apache/airflow/tree/3.0.0/airflow-core/src/airflow/models
    - 3.1 https://github.com/apache/airflow/tree/3.1.0/airflow-core/src/airflow/models
    """

    def __new__(cls, airflow_version: "Union[str, None]" = None) -> StarshipAirflow:
        from airflow import __version__
        from packaging.version import Version

        if airflow_version is None:
            airflow_version = __version__

        version = Version(airflow_version)
        major, minor = version.major, version.minor

        if major == 3:
            if minor == 0:
                return StarshipAirflow30()
            elif minor == 1:
                return StarshipAirflow31()

        raise RuntimeError(f"Unsupported Airflow Version: {airflow_version}")
