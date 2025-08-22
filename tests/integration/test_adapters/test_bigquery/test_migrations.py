"""Integration tests for BigQuery migration workflow."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.migrations.commands import MigrationCommands

pytestmark = [pytest.mark.xdist_group("bigquery")]


def test_bigquery_migration_full_workflow(bigquery_service: BigQueryService) -> None:
    """Test full BigQuery migration workflow: init -> create -> upgrade -> downgrade."""
    pytest.skip("BigQuery migration tests require real BigQuery backend (emulator has SQL syntax limitations)")

    test_id = "bigquery_full_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": bigquery_service.dataset,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = MigrationCommands(config)

        commands.init(str(migration_dir), package=True)

        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE OR REPLACE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` (
            id INT64,
            name STRING NOT NULL,
            email STRING,
            created_at TIMESTAMP
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}`"]
'''

        migration_file = migration_dir / "0001_create_users.py"
        migration_file.write_text(migration_content)

        try:
            commands.upgrade()

            with config.provide_session() as driver:
                result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{users_table}'"
                )
                assert len(result.data) == 1

                driver.execute(
                    f"INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` (id, name, email) VALUES (@id, @name, @email)",
                    {"id": 1, "name": "John Doe", "email": "john@example.com"},
                )

                users_result = driver.execute(
                    f"SELECT * FROM `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}`"
                )
                assert len(users_result.data) == 1
                assert users_result.data[0]["name"] == "John Doe"
                assert users_result.data[0]["email"] == "john@example.com"

            commands.downgrade("base")

            with config.provide_session() as driver:
                result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{users_table}'"
                )
                assert len(result.data) == 0
        finally:
            if config.pool_instance:
                config.close_pool()


def test_bigquery_multiple_migrations_workflow(bigquery_service: BigQueryService) -> None:
    """Test BigQuery workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    pytest.skip("BigQuery migration tests require real BigQuery backend (emulator has SQL syntax limitations)")

    test_id = "bigquery_multi_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": bigquery_service.dataset,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = MigrationCommands(config)

        commands.init(str(migration_dir), package=True)

        migration1_content = f'''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE OR REPLACE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` (
            id INT64,
            name STRING NOT NULL,
            email STRING
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}`"]
'''
        (migration_dir / "0001_create_users.py").write_text(migration1_content)

        migration2_content = f'''"""Create posts table."""


def up():
    """Create posts table."""
    return ["""
        CREATE OR REPLACE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.{posts_table}` (
            id INT64,
            title STRING NOT NULL,
            content STRING,
            user_id INT64
        )
    """]


def down():
    """Drop posts table."""
    return ["DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.{posts_table}`"]
'''
        (migration_dir / "0002_create_posts.py").write_text(migration2_content)

        try:
            commands.upgrade()

            with config.provide_session() as driver:
                users_result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{users_table}'"
                )
                posts_result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{posts_table}'"
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 1

            commands.downgrade("0001")

            with config.provide_session() as driver:
                users_result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{users_table}'"
                )
                posts_result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{posts_table}'"
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 0

            commands.downgrade("base")

            with config.provide_session() as driver:
                users_result = driver.execute(
                    f"SELECT table_name FROM `{bigquery_service.project}.{bigquery_service.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name IN ('{users_table}', '{posts_table}')"
                )
                assert len(users_result.data) == 0
        finally:
            if config.pool_instance:
                config.close_pool()


def test_bigquery_migration_current_command(bigquery_service: BigQueryService) -> None:
    """Test the current migration command shows correct version for BigQuery."""
    pytest.skip("BigQuery migration tests require real BigQuery backend (emulator has SQL syntax limitations)")

    test_id = "bigquery_current_cmd"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": bigquery_service.dataset,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = MigrationCommands(config)

        try:
            commands.init(str(migration_dir), package=True)

            current_version = commands.current()
            assert current_version is None or current_version == "base"

            migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE OR REPLACE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` (
            id INT64,
            name STRING NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}`"]
'''
            (migration_dir / "0001_create_users.py").write_text(migration_content)

            commands.upgrade()

            current_version = commands.current()
            assert current_version == "0001"

            commands.downgrade("base")

            current_version = commands.current()
            assert current_version is None or current_version == "base"
        finally:
            if config.pool_instance:
                config.close_pool()


def test_bigquery_migration_error_handling(bigquery_service: BigQueryService) -> None:
    """Test BigQuery migration error handling."""
    pytest.skip("BigQuery migration tests require real BigQuery backend (emulator has SQL syntax limitations)")
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": bigquery_service.dataset,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_bigquery_error",
            },
        )
        commands = MigrationCommands(config)

        try:
            commands.init(str(migration_dir), package=True)

            migration_content = '''"""Migration with invalid SQL."""


def up():
    """Create table with invalid SQL."""
    return ["CREATE INVALID SQL STATEMENT"]


def down():
    """Drop table."""
    return ["DROP TABLE IF EXISTS invalid_table"]
'''
            (migration_dir / "0001_invalid.py").write_text(migration_content)

            with pytest.raises(Exception):
                commands.upgrade()

            with config.provide_session() as driver:
                try:
                    result = driver.execute("SELECT COUNT(*) as count FROM sqlspec_migrations_bigquery_error")
                    assert result.data[0]["count"] == 0
                except Exception:
                    pass
        finally:
            if config.pool_instance:
                config.close_pool()


def test_bigquery_migration_with_transactions(bigquery_service: BigQueryService) -> None:
    """Test BigQuery migrations work properly with transactions."""
    pytest.skip("BigQuery migration tests require real BigQuery backend (emulator has SQL syntax limitations)")

    test_id = "bigquery_transactions"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": bigquery_service.dataset,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = MigrationCommands(config)

        try:
            commands.init(str(migration_dir), package=True)

            migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE OR REPLACE TABLE `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` (
            id INT64,
            name STRING NOT NULL,
            email STRING
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}`"]
'''
            (migration_dir / "0001_create_users.py").write_text(migration_content)

            commands.upgrade()

            with config.provide_session() as driver:
                driver.execute(
                    f"INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` (id, name, email) VALUES (@id, @name, @email)",
                    {"id": 1, "name": "Transaction User", "email": "trans@example.com"},
                )

                result = driver.execute(
                    f"SELECT * FROM `{bigquery_service.project}.{bigquery_service.dataset}.{users_table}` WHERE name = 'Transaction User'"
                )
                assert len(result.data) == 1
        finally:
            if config.pool_instance:
                config.close_pool()
