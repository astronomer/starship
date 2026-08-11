from astronomer_starship._af3.starship_compatability import StarshipAirflow31


class FakeColumn:
    def __init__(self, name):
        self.name = name


class FakeTable:
    columns = [
        FakeColumn("dag_id"),
        FakeColumn("run_id"),
        FakeColumn("log_template_id"),
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


def test_dag_run_direct_insert_strips_source_log_template_id(monkeypatch):
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
