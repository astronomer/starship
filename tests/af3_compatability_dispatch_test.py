import datetime

import pytest

from astronomer_starship._af3.starship_compatability import (
    StarshipAirflow30,
    StarshipAirflow31,
    StarshipAirflow32,
    StarshipAirflow33,
    StarshipCompatabilityLayer,
)

ALL_AF3_SUBCLASSES = [
    StarshipAirflow30,
    StarshipAirflow31,
    StarshipAirflow32,
    StarshipAirflow33,
]

ATTR_METHODS = [
    "pool_attrs",
    "variable_attrs",
    "connection_attrs",
    "dag_attrs",
    "dag_run_attrs",
    "task_instance_attrs",
    "dag_runs_attrs",
    "task_instances_attrs",
]

# (list-endpoint method, key holding the row payload, item-endpoint method).
LIST_TO_ITEM = [
    ("dag_runs_attrs", "dag_runs", "dag_run_attrs"),
    ("task_instances_attrs", "task_instances", "task_instance_attrs"),
]


def _walk_datetimes(value):
    if isinstance(value, datetime.datetime):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _walk_datetimes(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _walk_datetimes(v)


@pytest.mark.parametrize(
    "airflow_version,expected_cls",
    [
        ("3.0.0", StarshipAirflow30),
        ("3.0.6", StarshipAirflow30),
        ("3.1.0", StarshipAirflow31),
        ("3.1.8", StarshipAirflow31),
        ("3.2.0", StarshipAirflow32),
        ("3.2.2", StarshipAirflow32),
        ("3.3.0", StarshipAirflow33),
        ("3.3.1", StarshipAirflow33),
    ],
)
def test_compatability_layer_dispatches_to_correct_subclass(airflow_version, expected_cls):
    instance = StarshipCompatabilityLayer(airflow_version=airflow_version)
    assert isinstance(instance, expected_cls)


@pytest.mark.parametrize("airflow_version", ["3.4.0", "4.0.0"])
def test_compatability_layer_raises_for_unsupported_versions(airflow_version):
    with pytest.raises(RuntimeError, match="Unsupported Airflow Version"):
        StarshipCompatabilityLayer(airflow_version=airflow_version)


def test_starship_airflow31_exposes_team_id_read_only():
    for attrs_fn in (
        StarshipAirflow31.pool_attrs,
        StarshipAirflow31.variable_attrs,
        StarshipAirflow31.connection_attrs,
    ):
        # team UUIDs are deployment-local, so team_id can't be POSTed cross-deployment.
        assert attrs_fn()["team_id"]["methods"] == []


def test_starship_airflow32_exposes_team_name_read_only():
    for attrs_fn in (
        StarshipAirflow32.pool_attrs,
        StarshipAirflow32.variable_attrs,
        StarshipAirflow32.connection_attrs,
    ):
        # team names must be pre-created on target; leaving this writable would FK-violate.
        assert attrs_fn()["team_name"]["methods"] == []


def test_starship_airflow32_drops_team_id_after_rename():
    # 3.2 renames team_id -> team_name; leaving team_id in attrs would surface
    # a phantom column on GET (no such column in the 3.2 DB).
    for attrs_fn in (
        StarshipAirflow32.pool_attrs,
        StarshipAirflow32.variable_attrs,
        StarshipAirflow32.connection_attrs,
    ):
        attrs = attrs_fn()
        assert "team_id" not in attrs
        assert "team_name" in attrs


def test_starship_airflow32_exposes_new_dag_run_columns():
    dag_run_attrs = StarshipAirflow32.dag_run_attrs()
    for col in ("created_at", "partition_key", "partition_date"):
        assert col in dag_run_attrs, f"{col} missing from StarshipAirflow32.dag_run_attrs"


def test_starship_airflow33_exposes_new_task_instance_columns():
    task_instance_attrs = StarshipAirflow33.task_instance_attrs()
    for col in ("retry_delay_override", "retry_reason"):
        assert col in task_instance_attrs, f"{col} missing from StarshipAirflow33.task_instance_attrs"


def test_starship_airflow33_inherits_all_32_additions():
    for attrs_fn in (
        StarshipAirflow33.pool_attrs,
        StarshipAirflow33.variable_attrs,
        StarshipAirflow33.connection_attrs,
    ):
        assert "team_name" in attrs_fn()

    dag_run_attrs = StarshipAirflow33.dag_run_attrs()
    for col in ("created_at", "partition_key", "partition_date"):
        assert col in dag_run_attrs


def test_dag_runs_test_value_payload_carries_new_columns():
    row_32 = StarshipAirflow32.dag_runs_attrs()["dag_runs"]["test_value"][0]
    for col in ("created_at", "partition_key", "partition_date"):
        assert col in row_32, f"{col} missing from StarshipAirflow32.dag_runs_attrs payload"

    row_33 = StarshipAirflow33.task_instances_attrs()["task_instances"]["test_value"][0]
    for col in ("retry_delay_override", "retry_reason"):
        assert col in row_33, f"{col} missing from StarshipAirflow33.task_instances_attrs payload"


def test_starship_airflow31_does_not_have_32_or_33_additions():
    assert "team_name" not in StarshipAirflow31.pool_attrs()
    assert "created_at" not in StarshipAirflow31.dag_run_attrs()
    assert "retry_delay_override" not in StarshipAirflow31.task_instance_attrs()


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
@pytest.mark.parametrize("method_name", ATTR_METHODS)
def test_attrs_method_returns_wellformed_desc(cls, method_name):
    attrs = getattr(cls, method_name)()
    assert isinstance(attrs, dict) and attrs, f"{cls.__name__}.{method_name} returned empty"

    for key, desc in attrs.items():
        assert {
            "attr",
            "methods",
            "test_value",
        } <= desc.keys(), f"{cls.__name__}.{method_name}[{key!r}] missing keys: got {set(desc)}"
        assert isinstance(desc["methods"], list)
        for entry in desc["methods"]:
            assert isinstance(entry, tuple) and len(entry) == 2, entry
            verb, required = entry
            assert verb in {"GET", "POST", "PUT", "DELETE", "PATCH"}, verb
            assert isinstance(required, bool)


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
@pytest.mark.parametrize("list_method,list_key,item_method", LIST_TO_ITEM)
def test_list_payload_row_matches_item_attrs(cls, list_method, list_key, item_method):
    row = getattr(cls, list_method)()[list_key]["test_value"][0]
    item_keys = set(getattr(cls, item_method)().keys())
    assert set(row.keys()) == item_keys, (
        f"{cls.__name__}: {list_method}[{list_key!r}] row keys diverge from {item_method}: "
        f"symmetric diff = {set(row.keys()) ^ item_keys}"
    )


# Columns a subclass is allowed to drop relative to its parent (documented DB renames).
KNOWN_DROPPED_COLUMNS = {
    # 3.2 renames team_id -> team_name
    (StarshipAirflow32, "pool_attrs"): {"team_id"},
    (StarshipAirflow32, "variable_attrs"): {"team_id"},
    (StarshipAirflow32, "connection_attrs"): {"team_id"},
}


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
@pytest.mark.parametrize("method_name", ATTR_METHODS)
def test_subclass_never_shrinks_parent_attrs(cls, method_name):
    parent = cls.__mro__[1]
    if parent not in ALL_AF3_SUBCLASSES:
        pytest.skip(f"{parent.__name__} is the abstract base; no parent attrs to compare")
    allowed = KNOWN_DROPPED_COLUMNS.get((cls, method_name), set())
    dropped = (set(getattr(parent, method_name)()) - set(getattr(cls, method_name)())) - allowed
    assert not dropped, f"{cls.__name__}.{method_name} dropped inherited columns: {dropped}"


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
@pytest.mark.parametrize("method_name", ATTR_METHODS)
def test_datetime_test_values_are_timezone_aware(cls, method_name):
    # Naive datetimes round-trip through Airflow's UtcDateTime naive, but API
    # responses come back tz-aware -- equality then breaks.
    attrs = getattr(cls, method_name)()
    naive = [dt for desc in attrs.values() for dt in _walk_datetimes(desc["test_value"]) if dt.tzinfo is None]
    assert not naive, f"{cls.__name__}.{method_name} has naive datetimes: {naive}"


def test_compatability_layer_defaults_to_installed_airflow_version(monkeypatch):
    # Exercises the `airflow_version is None` branch in __new__.
    import airflow

    monkeypatch.setattr(airflow, "__version__", "3.2.2", raising=False)
    assert isinstance(StarshipCompatabilityLayer(), StarshipAirflow32)

    monkeypatch.setattr(airflow, "__version__", "3.3.1", raising=False)
    assert isinstance(StarshipCompatabilityLayer(), StarshipAirflow33)


DAG_PAGINATION_PARAMS = ("limit", "offset", "search", "search_field")

DAG_ROW_KEYS = (
    "dag_id",
    "timetable_summary",
    "is_paused",
    "fileloc",
    "description",
    "owners",
    "tags",
    "dag_run_count",
)


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
@pytest.mark.parametrize("param", DAG_PAGINATION_PARAMS)
def test_dag_attrs_pagination_params_are_get_only(cls, param):
    # Params are consumed as query args by get_kwargs_fn and must not surface as row fields.
    desc = cls.dag_attrs()[param]
    assert desc["methods"] == [("GET", False)], desc


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
@pytest.mark.parametrize("row_key", DAG_ROW_KEYS)
def test_dag_attrs_row_shape_regression(cls, row_key):
    # Guards against accidentally dropping any existing dag_attrs row field when
    # adding new params or subclass overrides.
    assert row_key in cls.dag_attrs(), f"{cls.__name__}.dag_attrs missing row field {row_key!r}"


@pytest.mark.parametrize("cls", ALL_AF3_SUBCLASSES)
def test_dag_attrs_pagination_params_are_not_row_fields(cls):
    # get_dags() must skip GET-only descriptors when building each row so the
    # response doesn't contain phantom `limit`/`offset`/`search` keys.
    row_attrs = {
        attr: desc
        for attr, desc in cls.dag_attrs().items()
        if not (desc["methods"] and all(m[0] == "GET" for m in desc["methods"]))
    }
    for param in DAG_PAGINATION_PARAMS:
        assert param not in row_attrs, f"{cls.__name__}.dag_attrs: {param!r} leaked into row shape"
