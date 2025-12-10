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
            self.session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(is_paused=is_paused))
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

            return [tag[0] for tag in self.session.query(DagTag.name).filter(DagTag.dag_id == dag_id).all()]
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
            return self.session.query(count(distinct(DagRun.run_id))).filter(DagRun.dag_id == dag_id).one()[0]
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
        from sqlalchemy import MetaData, String, desc, select

        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=["dag_run"])
            table = metadata.tables["dag_run"]

            # enum validation
            if "triggered_by" in table.c:
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

    @classmethod
    def task_instances_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch_tz = epoch.replace(tzinfo=pytz.utc)
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
            "task_instances": {
                "attr": None,
                "methods": [("POST", True)],
                "test_value": [
                    {
                        "id": "00000000-0000-0000-0000-000000000000",  # AF3: UUID primary key
                        "dag_id": "dag_0",
                        "run_id": "manual__1970-01-01T00:00:00+00:00",
                        "task_id": "task_id",
                        "map_index": -1,
                        "try_number": 0,
                        "start_date": epoch_tz,
                        "end_date": epoch_tz,
                        "duration": 1.0,
                        "state": "success",
                        "max_tries": 2,
                        "hostname": "hostname",
                        "unixname": "unixname",
                        # AF3: job_id removed
                        "pool": "default_pool",
                        "pool_slots": 4,
                        "queue": "default",
                        "priority_weight": 5,
                        "operator": "BashOperator",
                        "custom_operator_name": None,
                        "queued_dttm": epoch_tz,
                        "scheduled_dttm": epoch_tz,  # AF3: new field
                        "queued_by_job_id": 6,
                        "pid": 7,
                        "executor": "LocalExecutor",  # AF3: executor field
                        "external_executor_id": "external_executor_id",
                        "trigger_id": None,
                        "trigger_timeout": epoch_tz,
                        "executor_config": "\x80\x04}\x94.",
                        "rendered_map_index": "0",
                        "task_display_name": "Task Display Name",
                        "dag_version_id": None,  # AF3: new UUID field linking to dag_version table
                    }
                ],
            },
        }

    @classmethod
    def task_instance_attrs(cls) -> "Dict[str, AttrDesc]":
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        epoch_tz = epoch.replace(tzinfo=pytz.utc)
        return {
            "id": {
                "attr": "id",
                "methods": [("POST", True)],
                "test_value": "00000000-0000-0000-0000-000000000000",
            },
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
                "attr": "try_number",
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
                "test_value": "success",
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
            "pool": {
                "attr": "pool",
                "methods": [("POST", True)],
                "test_value": "default_pool",
            },
            "pool_slots": {
                "attr": "pool_slots",
                "methods": [("POST", True)],
                "test_value": 4,
            },
            "queue": {
                "attr": "queue",
                "methods": [("POST", False)],
                "test_value": "default",
            },
            "priority_weight": {
                "attr": "priority_weight",
                "methods": [("POST", False)],
                "test_value": 5,
            },
            "operator": {
                "attr": "operator",
                "methods": [("POST", False)],
                "test_value": "BashOperator",
            },
            "custom_operator_name": {
                "attr": "custom_operator_name",
                "methods": [("POST", True)],
                "test_value": None,
            },
            "queued_dttm": {
                "attr": "queued_dttm",
                "methods": [("POST", False)],
                "test_value": epoch_tz,
            },
            "scheduled_dttm": {
                "attr": "scheduled_dttm",
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
            "executor": {
                "attr": "executor",
                "methods": [("POST", True)],
                "test_value": "LocalExecutor",
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
                "attr": None,  # Don't map directly - causes serialization issues
                "methods": [("POST", False)],
                "test_value": "\x80\x04}\x94.",
            },
            "rendered_map_index": {
                "attr": "rendered_map_index",
                "methods": [("POST", True)],
                "test_value": "0",
            },
            "task_display_name": {
                "attr": "task_display_name",
                "methods": [("POST", True)],
                "test_value": "Task Display Name",
            },
            "dag_version_id": {
                "attr": "dag_version_id",
                "methods": [("POST", False)],
                "test_value": None,
            },
            # Note: serialization issues: next_method and next_kwargs intentionally omitted
        }

    def get_task_instances(self, dag_id: str, offset: int = 0, limit: int = 10):
        import json

        from airflow.models import TaskInstance
        from sqlalchemy import MetaData, String, desc, select

        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)

            metadata.reflect(engine, only=["dag_run"])
            dag_run_table = metadata.tables["dag_run"]
            dag_run_table.c.triggered_by.type = String(50)

            sub_stmt = (
                select(dag_run_table.c.run_id)
                .where(dag_run_table.c.dag_id == dag_id)
                .order_by(desc(dag_run_table.c.start_date))
                .limit(limit)
            )
            if offset:
                sub_stmt = sub_stmt.offset(offset)

            run_ids_result = self.session.execute(sub_stmt)
            run_ids = [row[0] for row in run_ids_result]

            # Use noload() to prevent eager loading
            from sqlalchemy.orm import noload

            results = (
                self.session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_id)
                .filter(TaskInstance.run_id.in_(run_ids))
                .options(noload("*"))
                .order_by(desc(TaskInstance.start_date))
                .all()
            )

            return json.loads(
                json.dumps(
                    {
                        "task_instances": results_to_list_via_attrs(results, self.task_instance_attrs()),
                        "dag_run_count": self._get_dag_run_count(dag_id),
                    },
                    default=str,
                )
            )
        except Exception as e:
            self.session.rollback()
            raise e

    def set_task_instances(self, task_instances: list):
        task_instances = self.insert_directly("task_instance", task_instances)

        # populate dag_version_id
        dag_ids = {ti.get("dag_id") for ti in task_instances if ti.get("dag_id")}
        for dag_id in dag_ids:
            try:
                self.update_dag_version_id(dag_id)
                logger.info("Auto-populated dag_version_id for DAG: %s", dag_id)
            except Exception as e:
                logger.warning("Failed to auto-populate dag_version_id for %s: %s", dag_id, e)

        return {"task_instances": task_instances}

    def get_task_instance_history(self, dag_id: str, offset: int = 0, limit: int = 10):
        from airflow.models import DagRun
        from airflow.models.taskinstancehistory import TaskInstanceHistory
        from sqlalchemy import desc
        from sqlalchemy.orm import load_only

        try:
            sub_query = (
                self.session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id)
                .order_by(desc(DagRun.start_date))
                .limit(limit)
            )
            if offset:
                sub_query = sub_query.offset(offset)
            sub_query = sub_query.subquery()

            results = (
                self.session.query(TaskInstanceHistory)
                .filter(TaskInstanceHistory.dag_id == dag_id)
                .filter(TaskInstanceHistory.run_id.in_(sub_query))
                .options(
                    load_only(
                        *[
                            attr_desc["attr"]
                            for attr, attr_desc in self.task_instance_attrs().items()
                            if attr_desc["attr"] is not None and attr != "id"  # Exclude 'id' property
                        ]
                    )
                )
                .order_by(desc(TaskInstanceHistory.start_date))
                .all()
            )
            return {
                "task_instances": results_to_list_via_attrs(results, self.task_instance_attrs()),
                "dag_run_count": self._get_dag_run_count(dag_id),
            }
        except Exception as e:
            self.session.rollback()
            raise e

    def set_task_instance_history(self, task_instances: list):
        task_instances = self.insert_directly("task_instance_history", task_instances)
        return {"task_instances": task_instances}

    def insert_directly(self, table_name, items):  # noqa: C901
        import pickle

        from sqlalchemy import MetaData
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.exc import InvalidRequestError

        if not items:
            return []

        for item in items:
            if "id" in item and table_name not in ["task_instance", "task_instance_history"]:
                del item["id"]

            # Strip FK fields that reference source-specific UUIDs
            item.pop("dag_version_id", None)
            item.pop("created_dag_version_id", None)

            if "executor_config" in item:
                # Drop executor_config, because its original type may have gotten lost
                # and pickling it will not recover it
                item["executor_config"] = pickle.dumps({})
        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=[table_name])
            table = metadata.tables[table_name]

            valid_columns = {c.name for c in table.columns}
            filtered_items = [{k: v for k, v in item.items() if k in valid_columns} for item in items]

            # Use UPSERT with ON CONFLICT DO NOTHING for idempotent migrations
            stmt = insert(table).values(filtered_items)

            # Define conflict targets based on table's natural key
            if table_name == "dag_run":
                stmt = stmt.on_conflict_do_nothing(index_elements=["dag_id", "run_id"])
            elif table_name in ["task_instance", "task_instance_history"]:
                stmt = stmt.on_conflict_do_nothing(index_elements=["dag_id", "task_id", "run_id", "map_index"])
            else:
                stmt = stmt.on_conflict_do_nothing()

            self.session.execute(stmt)
            self.session.commit()

            # Remove non-JSON-serializable fields from return value
            for item in items:
                if "executor_config" in item and isinstance(item["executor_config"], bytes):
                    item["executor_config"] = None
            return items
        except (InvalidRequestError, KeyError):
            return self.insert_directly(f"airflow.{table_name}", items)
        except Exception as e:
            self.session.rollback()
            raise e

    def get_latest_dag_version_id(self, dag_id: str):
        from sqlalchemy import MetaData, desc, select

        try:
            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=["dag_version"])
            dag_version_table = metadata.tables["dag_version"]

            stmt = (
                select(dag_version_table.c.id)
                .where(dag_version_table.c.dag_id == dag_id)
                .order_by(desc(dag_version_table.c.version_number))
                .limit(1)
            )

            result = self.session.execute(stmt).first()
            return str(result[0]) if result else None
        except Exception as e:
            self.session.rollback()
            raise e

    def update_dag_version_id(self, dag_id: str, dag_version_id: str = None):
        """
        Update dag_version_id(FK) for task instances AND dag runs that have NULL values.
        Update created_dag_version_id on dag_run records so the UI can properly display them.
        """
        from sqlalchemy import MetaData, update

        try:
            if not dag_version_id:
                dag_version_id = self.get_latest_dag_version_id(dag_id)
                if not dag_version_id:
                    raise ValueError(
                        f"No dag_version found for dag_id: {dag_id}. "
                        f"Ensure the DAG has been parsed by Airflow on the target deployment."
                    )

            engine = self.session.get_bind()
            metadata = MetaData(bind=engine)
            metadata.reflect(engine, only=["dag_run", "task_instance", "task_instance_history"])

            dr_table = metadata.tables["dag_run"]
            dr_stmt = (
                update(dr_table)
                .where(dr_table.c.dag_id == dag_id)
                .where(dr_table.c.created_dag_version_id.is_(None))
                .values(created_dag_version_id=dag_version_id)
            )
            dr_result = self.session.execute(dr_stmt)

            ti_table = metadata.tables["task_instance"]
            ti_stmt = (
                update(ti_table)
                .where(ti_table.c.dag_id == dag_id)
                .where(ti_table.c.dag_version_id.is_(None))
                .values(dag_version_id=dag_version_id)
            )
            ti_result = self.session.execute(ti_stmt)

            tih_table = metadata.tables["task_instance_history"]
            tih_stmt = (
                update(tih_table)
                .where(tih_table.c.dag_id == dag_id)
                .where(tih_table.c.dag_version_id.is_(None))
                .values(dag_version_id=dag_version_id)
            )
            tih_result = self.session.execute(tih_stmt)

            self.session.commit()

            return {
                "dag_id": dag_id,
                "dag_version_id": dag_version_id,
                "dag_runs_updated": dr_result.rowcount,
                "task_instances_updated": ti_result.rowcount,
                "task_instance_history_updated": tih_result.rowcount,
            }
        except Exception as e:
            self.session.rollback()
            raise e


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
