"""Tests for asyncpg column-type reporting used by Arrow null typing."""

from typing import Any

from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.core.statement import SQL


class _Type:
    def __init__(self, oid: int) -> None:
        self.oid = oid


class _Attribute:
    def __init__(self, name: str, oid: int) -> None:
        self.name = name
        self.type = _Type(oid)


class _Prepared:
    def __init__(self, attributes: "tuple[_Attribute, ...]", rows: "list[Any]") -> None:
        self._attributes = attributes
        self._rows = rows
        self.fetch_calls = 0

    def get_attributes(self) -> "tuple[_Attribute, ...]":
        return self._attributes

    async def fetch(self, *args: Any) -> "list[Any]":
        self.fetch_calls += 1
        return self._rows


class _Connection:
    def __init__(self, prepared: _Prepared) -> None:
        self._prepared = prepared
        self.prepare_calls = 0

    async def prepare(self, sql: str) -> _Prepared:
        self.prepare_calls += 1
        return self._prepared


class _NoAttributes(_Prepared):
    def get_attributes(self) -> "tuple[_Attribute, ...]":
        msg = "attributes unavailable"
        raise RuntimeError(msg)


async def test_asyncpg_select_reports_column_types_from_prepared_attributes() -> None:
    """A SELECT reports declared column types taken from the prepared statement."""
    prepared = _Prepared((_Attribute("id", 23), _Attribute("amount", 1700), _Attribute("note", 25)), [])
    connection = _Connection(prepared)
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    result = await driver.dispatch_execute(connection, SQL("SELECT id, amount, note FROM t"))  # type: ignore[arg-type]

    assert result.column_types == {"id": "int32", "amount": "decimal", "note": "string"}
    assert connection.prepare_calls == 1
    assert prepared.fetch_calls == 1


async def test_asyncpg_select_omits_unmapped_oids() -> None:
    """Columns whose OID is not modeled are left out rather than guessed."""
    prepared = _Prepared((_Attribute("id", 23), _Attribute("exotic", 999999)), [])
    connection = _Connection(prepared)
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    result = await driver.dispatch_execute(connection, SQL("SELECT id, exotic FROM t"))  # type: ignore[arg-type]

    assert result.column_types == {"id": "int32"}


async def test_asyncpg_select_survives_missing_attributes() -> None:
    """A statement that cannot report attributes falls back to no reported types."""
    prepared = _NoAttributes((), [])
    connection = _Connection(prepared)
    driver = AsyncpgDriver(connection)  # type: ignore[arg-type]

    result = await driver.dispatch_execute(connection, SQL("SELECT 1"))  # type: ignore[arg-type]

    assert result.column_types is None
