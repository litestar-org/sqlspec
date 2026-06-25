"""Oracle direct path load opt-in bulk ingest (Thin-mode gated)."""

from typing import Any, cast

import pyarrow as pa

from sqlspec.adapters.oracledb.driver import OracleSyncDriver

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _FakeRawCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, Any]] = []
        self.rowcount = 0
        self.description = None

    def executemany(self, sql: str, records: Any, **_kwargs: Any) -> None:
        self.executemany_calls.append((sql, records))

    def close(self) -> None:
        pass


class _DPLConnection:
    def __init__(self, *, thin: bool = True, username: str = "SCOTT") -> None:
        self.thin = thin
        self.username = username
        self.dpl_calls: list[dict[str, Any]] = []
        self._cursor = _FakeRawCursor()

    def direct_path_load(
        self, *, schema_name: str, table_name: str, column_names: Any, data: Any, **_kwargs: Any
    ) -> None:
        self.dpl_calls.append({
            "schema_name": schema_name,
            "table_name": table_name,
            "column_names": column_names,
            "data": data,
        })

    def cursor(self) -> _FakeRawCursor:
        return self._cursor


class _NoDPLConnection:
    def __init__(self) -> None:
        self.thin = True
        self.username = "SCOTT"
        self._cursor = _FakeRawCursor()

    def cursor(self) -> _FakeRawCursor:
        return self._cursor


def _arrow() -> pa.Table:
    return pa.table({"id": [1, 2], "name": ["a", "b"]})


def test_direct_path_load_used_when_thin_and_enabled() -> None:
    conn = _DPLConnection(thin=True, username="SCOTT")
    driver = OracleSyncDriver(
        cast("Any", conn), driver_features={"storage_capabilities": _CAPS, "enable_direct_path_load": True}
    )

    driver.load_from_arrow("MYTAB", _arrow())

    assert len(conn.dpl_calls) == 1
    call = conn.dpl_calls[0]
    assert call["schema_name"] == "SCOTT"
    assert call["table_name"] == "MYTAB"
    assert call["column_names"] == ["id", "name"]
    assert len(call["data"]) == 2
    assert conn._cursor.executemany_calls == []


def test_direct_path_load_splits_qualified_table() -> None:
    conn = _DPLConnection(thin=True, username="SCOTT")
    driver = OracleSyncDriver(
        cast("Any", conn), driver_features={"storage_capabilities": _CAPS, "enable_direct_path_load": True}
    )

    driver.load_from_arrow("MYSCHEMA.MYTAB", _arrow())

    assert conn.dpl_calls[0]["schema_name"] == "MYSCHEMA"
    assert conn.dpl_calls[0]["table_name"] == "MYTAB"


def test_thick_mode_falls_back_to_executemany() -> None:
    conn = _DPLConnection(thin=False)
    driver = OracleSyncDriver(
        cast("Any", conn), driver_features={"storage_capabilities": _CAPS, "enable_direct_path_load": True}
    )

    driver.load_from_arrow("MYTAB", _arrow())

    assert conn.dpl_calls == []
    assert len(conn._cursor.executemany_calls) == 1


def test_feature_off_falls_back_to_executemany() -> None:
    conn = _DPLConnection(thin=True)
    driver = OracleSyncDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow("MYTAB", _arrow())

    assert conn.dpl_calls == []
    assert len(conn._cursor.executemany_calls) == 1


def test_missing_direct_path_load_api_falls_back() -> None:
    conn = _NoDPLConnection()
    driver = OracleSyncDriver(
        cast("Any", conn), driver_features={"storage_capabilities": _CAPS, "enable_direct_path_load": True}
    )

    driver.load_from_arrow("MYTAB", _arrow())

    assert len(conn._cursor.executemany_calls) == 1
