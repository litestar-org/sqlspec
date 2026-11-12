"""Non-bypass PostgreSQL role and policies for framework tenant acceptance."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import psycopg
import pytest
from psycopg.sql import SQL, Identifier, Literal

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService


@pytest.fixture
def tenant_policy_database(postgres_service: "PostgresService") -> Iterator[tuple[dict[str, Any], str]]:
    """Provision a unique table protected from the application role by RLS."""
    credentials = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "dbname": postgres_service.database,
    }
    suffix = uuid4().hex[:12]
    role, table = "tenant_framework_" + suffix, "tenant_rows_" + suffix
    with psycopg.connect(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,
    ) as admin:
        admin.execute(
            SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD {}").format(
                Identifier(role), Literal(postgres_service.password)
            )
        )
        try:
            admin.execute(
                SQL("CREATE TABLE {} (tenant_id text NOT NULL, value integer NOT NULL)").format(Identifier(table))
            )
            admin.execute(SQL("INSERT INTO {} VALUES ('a', 1), ('b', 2)").format(Identifier(table)))
            admin.execute(SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(Identifier(table)))
            admin.execute(SQL("ALTER TABLE {} FORCE ROW LEVEL SECURITY").format(Identifier(table)))
            admin.execute(
                SQL(
                    "CREATE POLICY tenant_policy ON {} USING (tenant_id = current_setting('app.current_tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true))"
                ).format(Identifier(table))
            )
            admin.execute(SQL("GRANT SELECT, INSERT ON {} TO {}").format(Identifier(table), Identifier(role)))
            yield {**credentials, "user": role}, table
        finally:
            admin.execute(SQL("DROP TABLE IF EXISTS {}").format(Identifier(table)))
            admin.execute(SQL("DROP ROLE {}").format(Identifier(role)))
