from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlspec.builder import Select

__all__ = ("test_example_24",)


def test_example_24(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example24.db"
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        session.execute(
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, status text)"""
        )

        # Insert test data
        session.execute(
            "INSERT INTO users (name, email, status) VALUES ('Alice', 'alice@example.com', 'active'), ('Bob', 'bob@example.com', 'inactive')"
        )

        # start-example
        class UserQueries:
            @staticmethod
            def by_id(user_id: int) -> "Select":
                return sql.select("*").from_("users").where(f"id = {user_id}")

            @staticmethod
            def by_email(email: str) -> "Select":
                return sql.select("*").from_("users").where(f"email = '{email}'")

            @staticmethod
            def search(filters: dict[str, Any]) -> "Select":
                query = sql.select("*").from_("users")

                if "name" in filters:
                    query = query.where(f"name LIKE '%{filters['name']}%'")

                if "status" in filters:
                    query = query.where(f"status = '{filters['status']}'")

                return query

        # Usage
        user = session.select_one(UserQueries.by_id(1))
        print(user)

        query = UserQueries.search({"name": "Alice", "status": "active"})
        users = session.select(query)
        print(len(users))
        # end-example
