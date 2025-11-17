from pathlib import Path

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
        session.execute("""CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, status text)""")
        # start-example
        class UserQueries:
            @staticmethod
            def by_id():
                return sql.select("*").from_("users").where("id = ?")

            @staticmethod
            def by_email():
                return sql.select("*").from_("users").where("email = ?")

            @staticmethod
            def search(filters):
                query = sql.select("*").from_("users")
                params = []

                if "name" in filters:
                    query = query.where("name LIKE ?")
                    params.append(f"%{filters['name']}%")

                if "status" in filters:
                    query = query.where("status = ?")
                    params.append(filters["status"])

                return query, params

        # Usage
        user = session.execute(UserQueries.by_id(), 1).one()
        query, params = UserQueries.search({"name": "Alice", "status": "active"})
        result = session.execute(query, *params)
        users = result.all()
        # end-example

