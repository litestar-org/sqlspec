from pathlib import Path

from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLFileLoader
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_multi_database_setup_example",)


async def test_multi_database_setup_example(tmp_path: Path, postgres_service: PostgresService) -> None:
    user_sql_path_pg = tmp_path / "sql" / "postgres"
    user_sql_path_pg.mkdir(parents=True, exist_ok=True)
    user_sql_file_pg = user_sql_path_pg / "users.sql"
    user_sql_file_pg.write_text(
        """-- name: upsert_user
        INSERT INTO users_sf1 (id, username, email) VALUES (:id, :username, :email)
        ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, email = EXCLUDED.email;
        """
    )
    user_sql_path_sqlite = tmp_path / "sql" / "sqlite"
    user_sql_path_sqlite.mkdir(parents=True, exist_ok=True)
    user_sql_file_sqlite = user_sql_path_sqlite / "users.sql"
    user_sql_file_sqlite.write_text(
        """-- name: get_user
        SELECT id, username, email FROM users_sf1 WHERE id = :user_id;
        """
    )
    shared_sql_path = tmp_path / "sql" / "shared"
    shared_sql_path.mkdir(parents=True, exist_ok=True)
    shared_sql_file = shared_sql_path / "common.sql"
    shared_sql_file.write_text(
        """-- name: delete_user
        DELETE FROM users_sf1 WHERE id = :user_id;
        """
    )
    params = {"id": 1, "username": "john_doe", "email": "jd@example.com"}

    # start-example
    # Different SQL files for different databases
    loader = SQLFileLoader()
    loader.load_sql(tmp_path / "sql/postgres/", tmp_path / "sql/sqlite/", tmp_path / "sql/shared/")

    # Queries automatically select correct dialect
    pg_query = loader.get_sql("upsert_user")  # Uses Postgres ON CONFLICT
    sqlite_query = loader.get_sql("get_user")  # Uses shared query

    from sqlspec import SQLSpec

    spec = SQLSpec()
    postgres_config = AsyncpgConfig(
        connection_config={
            "user": postgres_service.user,
            "password": postgres_service.password,
            "host": postgres_service.host,
            "port": postgres_service.port,
            "database": postgres_service.database,
        }
    )
    sqlite_config = SqliteConfig()
    # Execute on appropriate database
    async with spec.provide_session(postgres_config) as pg_session:
        await pg_session.execute("""CREATE TABLE users_sf1 ( id INTEGER PRIMARY KEY, username TEXT, email TEXT)""")
        await pg_session.execute(
            """ INSERT INTO users_sf1 (id, username, email) VALUES (1, 'old_name', 'old@example.com');"""
        )

        await pg_session.execute(pg_query, **params)

    with spec.provide_session(sqlite_config) as sqlite_session:
        sqlite_session.execute("""CREATE TABLE users_sf1 ( id INTEGER PRIMARY KEY, username TEXT, email TEXT)""")
        sqlite_session.execute(
            """ INSERT INTO users_sf1 (id, username, email) VALUES (1, 'john_doe', 'jd@example.com');"""
        )
        sqlite_session.execute(sqlite_query, user_id=1)
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
