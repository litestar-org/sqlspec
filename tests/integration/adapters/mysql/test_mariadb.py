"""Live MariaDB 11.8 compatibility integration tests across MySQL drivers."""

from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from tests.integration.fixtures.mysql import MariadbDriverSession

pytestmark = [pytest.mark.mysql, pytest.mark.integration, pytest.mark.xdist_group("mariadb")]


async def test_mariadb_118_version_and_parameterized_crud(mariadb_driver_session: "MariadbDriverSession") -> None:
    """Verify MariaDB 11.8 version banner and parameterized CRUD operations across MySQL drivers."""
    version_banner = await mariadb_driver_session.select_version()
    assert "11.8" in version_banner
    assert "mariadb" in version_banner.lower()

    table_name = mariadb_driver_session.table_name
    await mariadb_driver_session.execute_ddl(
        f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            name VARCHAR(128) NOT NULL,
            payload JSON NOT NULL
        ) ENGINE=InnoDB
        """
    )

    await mariadb_driver_session.execute_dml(
        f"INSERT INTO {table_name} (id, name, payload) VALUES (:id, :name, :payload)",
        id=1,
        name="initial",
        payload={"tier": "primary", "count": 1},
    )
    await mariadb_driver_session.execute_dml(
        f"UPDATE {table_name} SET name = :name WHERE id = :id", id=1, name="updated"
    )

    rows = await mariadb_driver_session.select_rows(
        f"SELECT id AS id, name AS name FROM {table_name} WHERE id = :id", id=1
    )
    assert rows == [{"id": 1, "name": "updated"}]

    await mariadb_driver_session.execute_dml(f"DELETE FROM {table_name} WHERE id = :id", id=1)
    remaining = await mariadb_driver_session.select_rows(f"SELECT id AS id FROM {table_name}")
    assert remaining == []


async def test_mariadb_118_transaction_commit_and_rollback(mariadb_driver_session: "MariadbDriverSession") -> None:
    """Verify explicit transaction rollback and commit semantics against MariaDB 11.8."""
    table_name = mariadb_driver_session.table_name
    await mariadb_driver_session.execute_ddl(
        f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            name VARCHAR(128) NOT NULL
        ) ENGINE=InnoDB
        """
    )

    await mariadb_driver_session.insert_and_rollback(table_name, row_id=10, name="discarded")
    after_rollback = await mariadb_driver_session.select_rows(f"SELECT id AS id, name AS name FROM {table_name}")
    assert after_rollback == []

    await mariadb_driver_session.insert_and_commit(table_name, row_id=20, name="persisted")
    after_commit = await mariadb_driver_session.select_rows(f"SELECT id AS id, name AS name FROM {table_name}")
    assert after_commit == [{"id": 20, "name": "persisted"}]


async def test_mariadb_118_json_roundtrip_and_on_duplicate_key_upsert(
    mariadb_driver_session: "MariadbDriverSession",
) -> None:
    """Verify JSON payload round-trip and ON DUPLICATE KEY UPDATE VALUES(col) upsert on MariaDB 11.8."""
    table_name = mariadb_driver_session.table_name
    await mariadb_driver_session.execute_ddl(
        f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            name VARCHAR(128) NOT NULL,
            payload JSON NOT NULL
        ) ENGINE=InnoDB
        """
    )

    upsert_sql = f"""
        INSERT INTO {table_name} (id, name, payload)
        VALUES (:id, :name, :payload)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            payload = VALUES(payload)
    """
    first_payload: dict[str, Any] = {"engine": "mariadb", "tags": ["lts", "11.8"], "revision": 1}
    second_payload: dict[str, Any] = {"engine": "mariadb", "tags": ["lts", "11.8", "upsert"], "revision": 2}

    await mariadb_driver_session.execute_dml(upsert_sql, id=7, name="v1", payload=first_payload)
    await mariadb_driver_session.execute_dml(upsert_sql, id=7, name="v2", payload=second_payload)

    rows = await mariadb_driver_session.select_rows(
        f"""
        SELECT
            id AS id,
            name AS name,
            payload AS payload,
            JSON_VALUE(payload, '$.engine') AS engine_name,
            CAST(JSON_VALUE(payload, '$.revision') AS SIGNED) AS revision_num
        FROM {table_name}
        WHERE id = :id
        """,
        id=7,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == 7
    assert row["name"] == "v2"
    assert row["engine_name"] == "mariadb"
    assert int(row["revision_num"]) == 2

    raw_payload = row["payload"]
    decoded_payload = from_json(raw_payload) if isinstance(raw_payload, (str, bytes)) else raw_payload
    assert decoded_payload == second_payload
