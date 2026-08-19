from astronomer_starship._af3.starship_compatability import StarshipAirflow31


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
    def __init__(self, bind):
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


class FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class FakeSession:
    def __init__(self, log_template_id=2):
        self.statement = None
        self.committed = False
        self.log_template_id = log_template_id
        self.log_template_lookups = 0

    def get_bind(self):
        return object()

    def execute(self, statement):
        # The real code only ever hands this fake two kinds of statement: the final
        # FakeInsert (recorded for assertions), or a raw `text()` select used to resolve
        # the target's active log_template id (answered from `self.log_template_id`).
        if isinstance(statement, FakeInsert):
            self.statement = statement
            return None
        self.log_template_lookups += 1
        return FakeScalarResult(self.log_template_id)

    def commit(self):
        self.committed = True

    def rollback(self):
        raise AssertionError("rollback should not be called")


def test_dag_run_direct_insert_resolves_log_template_id_to_target_active_template(monkeypatch):
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession(log_template_id=2)
    starship = StarshipAirflow31()
    starship._session = fake_session

    monkeypatch.setattr(sqlalchemy, "MetaData", FakeMetaData)
    monkeypatch.setattr(sqlalchemy.dialects.postgresql, "insert", FakeInsert)

    items = [
        {
            "dag_id": "example_dag",
            "run_id": "scheduled__2026-08-01T00:00:00+00:00",
            "log_template_id": 3,  # source-local id -- must NOT survive onto the target
            "dag_version_id": "source-task-version-id",
            "created_dag_version_id": "source-created-version-id",
        }
    ]

    result = starship.insert_directly("dag_run", items)

    inserted = fake_session.statement.items[0]
    # resolved to the target's own active template, not the source's id (3)
    assert inserted["log_template_id"] == 2
    assert "dag_version_id" not in inserted
    assert "created_dag_version_id" not in inserted
    assert fake_session.statement.conflict_target == ["dag_id", "run_id"]
    assert fake_session.committed is True
    assert result == [
        {
            "dag_id": "example_dag",
            "run_id": "scheduled__2026-08-01T00:00:00+00:00",
            "log_template_id": 2,
        }
    ]


def test_dag_run_direct_insert_resolves_log_template_id_once_per_batch(monkeypatch):
    """The target's active log_template id shouldn't be looked up once per row."""
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession(log_template_id=2)
    starship = StarshipAirflow31()
    starship._session = fake_session

    monkeypatch.setattr(sqlalchemy, "MetaData", FakeMetaData)
    monkeypatch.setattr(sqlalchemy.dialects.postgresql, "insert", FakeInsert)

    items = [{"dag_id": "example_dag", "run_id": f"scheduled__{i}", "log_template_id": 3} for i in range(5)]

    starship.insert_directly("dag_run", items)

    assert all(item["log_template_id"] == 2 for item in fake_session.statement.items)
    assert fake_session.log_template_lookups == 1


def test_dag_run_direct_insert_leaves_log_template_id_null_when_target_has_none(monkeypatch, caplog):
    """Target log_template table with zero rows shouldn't crash the whole batch -- and should
    say so loudly instead of silently repeating the #181 gap."""
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession(log_template_id=None)
    starship = StarshipAirflow31()
    starship._session = fake_session

    monkeypatch.setattr(sqlalchemy, "MetaData", FakeMetaData)
    monkeypatch.setattr(sqlalchemy.dialects.postgresql, "insert", FakeInsert)

    items = [
        {
            "dag_id": "example_dag",
            "run_id": "scheduled__2026-08-01T00:00:00+00:00",
            "log_template_id": 3,
        }
    ]

    starship.insert_directly("dag_run", items)

    inserted = fake_session.statement.items[0]
    assert inserted["log_template_id"] is None
    assert "no rows in its log_template table" in caplog.text


def test_dag_run_direct_insert_strips_source_backfill_id(monkeypatch):
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession()
    starship = StarshipAirflow31()
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


def test_task_instance_direct_insert_strips_source_trigger_id(monkeypatch):
    import sqlalchemy
    import sqlalchemy.dialects.postgresql

    fake_session = FakeSession()
    starship = StarshipAirflow31()
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
