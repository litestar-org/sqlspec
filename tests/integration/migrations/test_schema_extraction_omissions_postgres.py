"""Live PostgreSQL check that schema extraction names the objects it does not emit."""

import logging
from uuid import uuid4

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


def test_pg_extraction_warns_for_function_trigger_domain_grant(
    postgres_service: PostgresService, tmp_path_factory: pytest.TempPathFactory, caplog: pytest.LogCaptureFixture
) -> None:
    suffix = uuid4().hex[:8]
    schema = f"extract_{suffix}"
    migrations_dir = tmp_path_factory.mktemp("migrations")
    config = PsycopgSyncConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={
            "script_location": str(migrations_dir),
            "version_table_name": f"ddl_{suffix}",
            "enabled": True,
        },
    )
    try:
        with config.provide_session() as session:
            session.execute_script(
                f"""
                CREATE SCHEMA {schema};
                CREATE DOMAIN {schema}.email AS text;
                CREATE TABLE {schema}.orders (id int PRIMARY KEY, contact {schema}.email);
                CREATE FUNCTION {schema}.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
                CREATE TRIGGER audit BEFORE INSERT ON {schema}.orders FOR EACH ROW EXECUTE FUNCTION {schema}.touch();
                GRANT SELECT ON {schema}.orders TO PUBLIC;
                """
            )
            session.commit()
        commands = SyncMigrationCommands(config)

        with caplog.at_level(logging.WARNING):
            commands.dump_schema(schema=schema, echo=False)

        records = [record for record in caplog.records if "migration.schema_extraction.omitted" in record.getMessage()]
        assert len(records) == 1
        fields = records[0].__dict__["extra_fields"]
        assert set(fields["object_kinds"]) == {"grant", "function", "trigger", "domain"}
    finally:
        with config.provide_session() as session:
            session.execute_script(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            session.commit()
        config.close_pool()
