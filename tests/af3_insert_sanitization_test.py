import pytest
import sqlalchemy as sa

from astronomer_starship._af3.starship_compatability import (
    StarshipAirflow30,
    StarshipAirflow31,
    StarshipAirflow32,
    StarshipAirflow33,
)

# insert_directly is defined on StarshipAirflow30 and inherited unchanged --
# parametrize FK-stripping tests over every subclass to guard against a future override
# silently dropping the stripping logic.
STARSHIP_SUBCLASSES = [StarshipAirflow31, StarshipAirflow32, StarshipAirflow33]

ALL_STARSHIP_SUBCLASSES = [StarshipAirflow30, StarshipAirflow31, StarshipAirflow32, StarshipAirflow33]


class FakeColumn:
    def __init__(self, name):
        self.name = name


class FakeTable:
    columns = [
        FakeColumn("dag_id"),
        FakeColumn("task_id"),
        FakeColumn("run_id"),
        FakeColumn("map_index"),
        FakeColumn("log_template_id"),
        FakeColumn("backfill_id"),
        FakeColumn("trigger_id"),
        FakeColumn("dag_version_id"),
        FakeColumn("created_dag_version_id"),
    ]


class FakeMetaData:
    def __init__(self, bind=None):
        self.tables = {}

    def reflect(self, engine, only):
        self.tables[only[0]] = FakeTable()


class FakeInsert:
    def __init__(self, table):
        self.table = table
        self.items = None
        self.conflict_target = None

    def values(self, items):
        self.items = items
        return self

    def on_conflict_do_nothing(self, index_elements=None):
        self.conflict_target = index_elements
        return self


class FakeSession:
    def __init__(self):
        self.statement = None
        self.committed = False

    def get_bind(self):
        return object()

    def execute(self, statement):
        self.statement = statement

    def commit(self):
        self.committed = True

    def rollback(self):
        raise AssertionError("rollback should not be called")


class FakeQuery:
    """Minimal fake for a SQLAlchemy query chain: ``filter``/``group_by``/``distinct`` are no-ops; iteration yields ``rows``."""

    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def group_by(self, *args, **kwargs):
        return self

    def distinct(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter(self._rows)


class FakeQuerySession:
    """Minimal fake for the SQLAlchemy session: ``.query(*cols)`` returns a FakeQuery over the pre-canned rows."""

    def __init__(self, rows):
        self._rows = rows

    def query(self, *columns):
        return FakeQuery(self._rows)


class RecordingQuery:
    """Fake target query for ``_search_dag_query``: records the clause passed to ``filter`` instead of applying it."""

    def __init__(self):
        self.filtered_with = None

    def filter(self, clause):
        self.filtered_with = clause
        return self


class SelectBackedQuery:
    """Like ``FakeQuery``, but backed by a real Core ``select()`` so it's still
    a valid ``in_(...)`` target -- ``FakeQuery``'s plain rows iterator isn't."""

    def __init__(self, stmt):
        self._stmt = stmt

    def filter(self, *clauses):
        return SelectBackedQuery(self._stmt.where(*clauses))

    def distinct(self):
        return SelectBackedQuery(self._stmt.distinct())

    def __clause_element__(self):
        return self._stmt


class SelectBackedSession:
    def query(self, *columns):
        return SelectBackedQuery(sa.select(*columns))


@pytest.mark.parametrize("starship_cls", STARSHIP_SUBCLASSES)
def test_dag_run_direct_insert_strips_source_log_template_id(monkeypatch, starship_cls):
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession()
    starship = starship_cls()
    starship._session = fake_session

    monkeypatch.setattr(sqlalchemy, "MetaData", FakeMetaData)
    monkeypatch.setattr(sqlalchemy.dialects.postgresql, "insert", FakeInsert)

    items = [
        {
            "dag_id": "example_dag",
            "run_id": "scheduled__2026-08-01T00:00:00+00:00",
            "log_template_id": 3,
            "dag_version_id": "source-task-version-id",
            "created_dag_version_id": "source-created-version-id",
        }
    ]

    result = starship.insert_directly("dag_run", items)

    inserted = fake_session.statement.items[0]
    assert "log_template_id" not in inserted
    assert "dag_version_id" not in inserted
    assert "created_dag_version_id" not in inserted
    assert fake_session.statement.conflict_target == ["dag_id", "run_id"]
    assert fake_session.committed is True
    assert result == [{"dag_id": "example_dag", "run_id": "scheduled__2026-08-01T00:00:00+00:00"}]


@pytest.mark.parametrize("starship_cls", STARSHIP_SUBCLASSES)
def test_dag_run_direct_insert_strips_source_backfill_id(monkeypatch, starship_cls):
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession()
    starship = starship_cls()
    starship._session = fake_session

    monkeypatch.setattr(sqlalchemy, "MetaData", FakeMetaData)
    monkeypatch.setattr(sqlalchemy.dialects.postgresql, "insert", FakeInsert)

    items = [
        {
            "dag_id": "example_dag",
            "run_id": "backfill__2026-08-01T00:00:00+00:00",
            "backfill_id": 7,
            "dag_version_id": "source-task-version-id",
            "created_dag_version_id": "source-created-version-id",
        }
    ]

    result = starship.insert_directly("dag_run", items)

    inserted = fake_session.statement.items[0]
    assert "backfill_id" not in inserted
    assert "dag_version_id" not in inserted
    assert "created_dag_version_id" not in inserted
    assert fake_session.statement.conflict_target == ["dag_id", "run_id"]
    assert fake_session.committed is True
    assert result == [{"dag_id": "example_dag", "run_id": "backfill__2026-08-01T00:00:00+00:00"}]


@pytest.mark.parametrize("starship_cls", STARSHIP_SUBCLASSES)
def test_task_instance_direct_insert_strips_source_trigger_id(monkeypatch, starship_cls):
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession()
    starship = starship_cls()
    starship._session = fake_session

    monkeypatch.setattr(sqlalchemy, "MetaData", FakeMetaData)
    monkeypatch.setattr(sqlalchemy.dialects.postgresql, "insert", FakeInsert)

    items = [
        {
            "dag_id": "example_dag",
            "task_id": "example_task",
            "run_id": "scheduled__2026-08-01T00:00:00+00:00",
            "map_index": -1,
            "trigger_id": 5,
            "dag_version_id": "source-task-version-id",
        }
    ]

    result = starship.insert_directly("task_instance", items)

    inserted = fake_session.statement.items[0]
    assert "trigger_id" not in inserted
    assert "dag_version_id" not in inserted
    assert fake_session.statement.conflict_target == ["dag_id", "task_id", "run_id", "map_index"]
    assert fake_session.committed is True
    assert result == [
        {
            "dag_id": "example_dag",
            "task_id": "example_task",
            "run_id": "scheduled__2026-08-01T00:00:00+00:00",
            "map_index": -1,
        }
    ]


# ---------------------------------------------------------------------------
# Batched DAG-metadata helpers on BaseStarshipAirflow
#
# _fetch_tags_by_dag_id and _fetch_dag_run_counts back get_dags. Testing them
# in isolation localizes regressions without going through the full pagination
# code path.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_fetch_tags_empty_input_returns_empty_dict(starship_cls):
    starship = starship_cls()
    # Contract: empty input short-circuits without touching the session.
    starship._session = None
    assert starship._fetch_tags_by_dag_id([]) == {}


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_fetch_tags_groups_multiple_tags_per_dag(starship_cls):
    starship = starship_cls()
    starship._session = FakeQuerySession(
        [
            ("dag_a", "tag1"),
            ("dag_a", "tag2"),
            ("dag_b", "tag3"),
        ]
    )
    assert starship._fetch_tags_by_dag_id(["dag_a", "dag_b"]) == {
        "dag_a": ["tag1", "tag2"],
        "dag_b": ["tag3"],
    }


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_fetch_tags_missing_dag_absent_from_result(starship_cls):
    # DAGs without any tags don't appear as empty lists -- callers use
    # dict.get(dag_id, []) at the read site.
    starship = starship_cls()
    starship._session = FakeQuerySession([("dag_a", "only_tag")])
    assert starship._fetch_tags_by_dag_id(["dag_a", "dag_b_no_tags"]) == {"dag_a": ["only_tag"]}


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_fetch_run_counts_empty_input_returns_empty_dict(starship_cls):
    starship = starship_cls()
    starship._session = None
    assert starship._fetch_dag_run_counts([]) == {}


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_fetch_run_counts_populates_from_group_by(starship_cls):
    starship = starship_cls()
    starship._session = FakeQuerySession(
        [
            ("dag_a", 5),
            ("dag_b", 2),
        ]
    )
    assert starship._fetch_dag_run_counts(["dag_a", "dag_b"]) == {"dag_a": 5, "dag_b": 2}


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_fetch_run_counts_zero_fills_missing_dags(starship_cls):
    # Contract: dag_ids requested but absent from the GROUP BY result still
    # appear with count 0. Row builders rely on this to produce complete rows.
    starship = starship_cls()
    starship._session = FakeQuerySession([("dag_a", 3)])
    assert starship._fetch_dag_run_counts(["dag_a", "dag_never_ran"]) == {"dag_a": 3, "dag_never_ran": 0}


# ---------------------------------------------------------------------------
# _search_dag_query on BaseStarshipAirflow (backs get_dags' search_field param)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_search_dag_query_no_search_returns_query_unchanged(starship_cls):
    starship = starship_cls()
    starship._session = None
    query = RecordingQuery()
    assert starship._search_dag_query(query, None, None) is query
    assert query.filtered_with is None


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_search_dag_query_empty_string_search_returns_query_unchanged(starship_cls):
    starship = starship_cls()
    starship._session = None
    query = RecordingQuery()
    assert starship._search_dag_query(query, "", "dag_id") is query
    assert query.filtered_with is None


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_search_dag_query_dag_id_field_filters_only_dag_id(starship_cls):
    starship = starship_cls()
    starship._session = SelectBackedSession()
    query = RecordingQuery()
    starship._search_dag_query(query, "foo", "dag_id")
    clause = str(query.filtered_with)
    assert "dag.dag_id" in clause
    assert "dag.owners" not in clause
    assert "dag_tag" not in clause


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_search_dag_query_owner_field_filters_only_owners(starship_cls):
    starship = starship_cls()
    starship._session = SelectBackedSession()
    query = RecordingQuery()
    starship._search_dag_query(query, "foo", "owner")
    clause = str(query.filtered_with)
    assert "dag.owners" in clause
    assert "dag.dag_id" not in clause
    assert "dag_tag" not in clause


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
def test_search_dag_query_tag_field_filters_via_tag_subquery(starship_cls):
    starship = starship_cls()
    starship._session = SelectBackedSession()
    query = RecordingQuery()
    starship._search_dag_query(query, "foo", "tag")
    clause = str(query.filtered_with)
    assert "dag.dag_id IN" in clause
    assert "dag_tag" in clause
    assert "LIKE" not in clause.split("IN")[0]
    assert "dag.owners" not in clause


@pytest.mark.parametrize("starship_cls", ALL_STARSHIP_SUBCLASSES)
@pytest.mark.parametrize("search_field", [None, "", "bogus"])
def test_search_dag_query_unset_or_unknown_field_matches_all_columns(starship_cls, search_field):
    # Regression coverage for 3a49e05 (SQLAlchemy clauses raise on __bool__,
    # so this can't be a truthy check on field_filters.get(...)).
    starship = starship_cls()
    starship._session = SelectBackedSession()
    query = RecordingQuery()
    starship._search_dag_query(query, "foo", search_field)
    clause = str(query.filtered_with)
    assert "dag.dag_id" in clause
    assert "dag.owners" in clause
    assert "dag_tag" in clause
    assert clause.count(" OR ") == 2
