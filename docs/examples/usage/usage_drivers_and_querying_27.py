"""execute_many with dictionaries example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_27_execute_many_dict",)


def test_example_27_execute_many_dict() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, action TEXT, user_id INTEGER)")

        payload = ({"action": "create", "user_id": 1}, {"action": "delete", "user_id": 2})

        # start-example
        session.execute_many("INSERT INTO audit (action, user_id) VALUES (:action, :user_id)", payload)

        rows = session.select("SELECT COUNT(*) AS total FROM audit")
        assert rows == [{"total": 2}]
        # end-example
