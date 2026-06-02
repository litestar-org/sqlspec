"""Oracle-specific parameter variant coverage."""

import inspect
from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("oracle")

OracleParameterPayload = tuple[object, ...] | list[object] | dict[str, object]


def _lower_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key.lower(): value for key, value in row.items()}


def _read_lob_sync(value: object) -> object:
    if hasattr(value, "read"):
        return cast(Any, value).read()
    return value


async def _read_lob_async(value: object) -> object:
    if not hasattr(value, "read"):
        return value
    maybe_value = cast(Any, value).read()
    if inspect.isawaitable(maybe_value):
        return await maybe_value
    return maybe_value


def _drop_typed_sync_table(driver: OracleSyncDriver) -> None:
    driver.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ora_param_typed_sync'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def _drop_typed_async_table(driver: OracleAsyncDriver) -> None:
    await driver.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ora_param_typed_async'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def _drop_json_null_table(driver: OracleAsyncDriver) -> None:
    await driver.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ora_param_json_null'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.parametrize(
    ("statement", "parameters", "expected_value"),
    [
        pytest.param("SELECT :value AS bound_value FROM dual", {"value": "named"}, "named", id="named_colon"),
        pytest.param("SELECT :1 AS bound_value FROM dual", ("positional",), "positional", id="numeric_colon"),
        pytest.param(
            SQL("SELECT :1 AS bound_value FROM dual", ["sql-object"]),
            None,
            "sql-object",
            id="sql_object_numeric_colon",
        ),
    ],
)
def test_sync_oracle_native_bind_syntax_variants(
    oracle_sync_session: OracleSyncDriver,
    statement: str | SQL,
    parameters: OracleParameterPayload | None,
    expected_value: str,
) -> None:
    """Sync Oracle accepts native named and numeric colon binds."""
    if parameters is None:
        result = oracle_sync_session.execute(statement)
    else:
        result = oracle_sync_session.execute(statement, parameters)

    assert isinstance(result, SQLResult)
    assert _lower_row(result.get_data()[0]) == {"bound_value": expected_value}


@pytest.mark.parametrize(
    ("statement", "parameters", "expected_value"),
    [
        pytest.param("SELECT :value AS bound_value FROM dual", {"value": "async-named"}, "async-named", id="named"),
        pytest.param("SELECT :1 AS bound_value FROM dual", ["async-positional"], "async-positional", id="numeric"),
    ],
)
async def test_async_oracle_native_bind_syntax_variants(
    oracle_async_session: OracleAsyncDriver,
    statement: str,
    parameters: OracleParameterPayload,
    expected_value: str,
) -> None:
    """Async Oracle accepts native named and numeric colon binds."""
    result = await oracle_async_session.execute(statement, parameters)

    assert isinstance(result, SQLResult)
    assert _lower_row(result.get_data()[0]) == {"bound_value": expected_value}


def test_sync_oracle_identifier_case_with_bound_values(oracle_sync_session: OracleSyncDriver) -> None:
    """Oracle implicit uppercase aliases normalize while quoted aliases keep case."""
    result = oracle_sync_session.execute(
        'SELECT :upper_value AS UPPER_VALUE, :mixed_value AS "MixedCaseValue" FROM dual',
        {"upper_value": "upper", "mixed_value": "mixed"},
    )

    row = result.get_data()[0]
    assert row["upper_value"] == "upper"
    assert row["MixedCaseValue"] == "mixed"


def test_sync_oracle_typed_null_date_clob_and_raw_binds(oracle_sync_session: OracleSyncDriver) -> None:
    """Sync Oracle binds NULL and values across DATE, CLOB, and RAW columns."""
    _drop_typed_sync_table(oracle_sync_session)
    oracle_sync_session.execute_script("""
        CREATE TABLE ora_param_typed_sync (
            id NUMBER PRIMARY KEY,
            text_field VARCHAR2(100),
            number_field NUMBER,
            date_field DATE,
            clob_field CLOB,
            raw_field RAW(16)
        )
    """)
    try:
        insert_sql = """
            INSERT INTO ora_param_typed_sync (
                id, text_field, number_field, date_field, clob_field, raw_field
            )
            VALUES (:id, :text_field, :number_field, TO_DATE(:date_text, 'YYYY-MM-DD'), :clob_field, HEXTORAW(:raw_hex))
        """
        oracle_sync_session.execute(
            insert_sql,
            {
                "id": 1,
                "text_field": "typed values",
                "number_field": 42,
                "date_text": "2024-06-15",
                "clob_field": "CLOB content",
                "raw_hex": "DEADBEEF",
            },
        )
        oracle_sync_session.execute(
            insert_sql,
            {
                "id": 2,
                "text_field": None,
                "number_field": None,
                "date_text": None,
                "clob_field": None,
                "raw_hex": None,
            },
        )

        result = oracle_sync_session.execute("""
            SELECT
                id,
                text_field,
                number_field,
                TO_CHAR(date_field, 'YYYY-MM-DD') AS date_text,
                clob_field,
                RAWTOHEX(raw_field) AS raw_hex
            FROM ora_param_typed_sync
            ORDER BY id
        """)
        rows = [_lower_row(row) for row in result.get_data()]

        assert rows[0]["text_field"] == "typed values"
        assert rows[0]["number_field"] == 42
        assert rows[0]["date_text"] == "2024-06-15"
        assert _read_lob_sync(rows[0]["clob_field"]) == "CLOB content"
        assert rows[0]["raw_hex"] == "DEADBEEF"
        assert rows[1] == {
            "id": 2,
            "text_field": None,
            "number_field": None,
            "date_text": None,
            "clob_field": None,
            "raw_hex": None,
        }
    finally:
        _drop_typed_sync_table(oracle_sync_session)


async def test_async_oracle_typed_null_date_clob_and_raw_binds(oracle_async_session: OracleAsyncDriver) -> None:
    """Async Oracle binds NULL and values across DATE, CLOB, and RAW columns."""
    await _drop_typed_async_table(oracle_async_session)
    await oracle_async_session.execute_script("""
        CREATE TABLE ora_param_typed_async (
            id NUMBER PRIMARY KEY,
            text_field VARCHAR2(100),
            number_field NUMBER,
            date_field DATE,
            clob_field CLOB,
            raw_field RAW(16)
        )
    """)
    try:
        insert_sql = """
            INSERT INTO ora_param_typed_async (
                id, text_field, number_field, date_field, clob_field, raw_field
            )
            VALUES (:id, :text_field, :number_field, TO_DATE(:date_text, 'YYYY-MM-DD'), :clob_field, HEXTORAW(:raw_hex))
        """
        await oracle_async_session.execute(
            insert_sql,
            {
                "id": 1,
                "text_field": "async typed values",
                "number_field": 84,
                "date_text": "2025-01-21",
                "clob_field": "Async CLOB content",
                "raw_hex": "FEEDFACE",
            },
        )
        await oracle_async_session.execute(
            insert_sql,
            {
                "id": 2,
                "text_field": None,
                "number_field": None,
                "date_text": None,
                "clob_field": None,
                "raw_hex": None,
            },
        )

        result = await oracle_async_session.execute("""
            SELECT
                id,
                text_field,
                number_field,
                TO_CHAR(date_field, 'YYYY-MM-DD') AS date_text,
                clob_field,
                RAWTOHEX(raw_field) AS raw_hex
            FROM ora_param_typed_async
            ORDER BY id
        """)
        rows = [_lower_row(row) for row in result.get_data()]

        assert rows[0]["text_field"] == "async typed values"
        assert rows[0]["number_field"] == 84
        assert rows[0]["date_text"] == "2025-01-21"
        assert await _read_lob_async(rows[0]["clob_field"]) == "Async CLOB content"
        assert rows[0]["raw_hex"] == "FEEDFACE"
        assert rows[1] == {
            "id": 2,
            "text_field": None,
            "number_field": None,
            "date_text": None,
            "clob_field": None,
            "raw_hex": None,
        }
    finally:
        await _drop_typed_async_table(oracle_async_session)


async def test_async_oracle_json_column_null_bind(oracle_async_session: OracleAsyncDriver) -> None:
    """Oracle native JSON columns accept NULL as a bound parameter value."""
    await _drop_json_null_table(oracle_async_session)
    await oracle_async_session.execute_script("""
        CREATE TABLE ora_param_json_null (
            id NUMBER PRIMARY KEY,
            payload JSON
        )
    """)
    try:
        await oracle_async_session.execute(
            "INSERT INTO ora_param_json_null (id, payload) VALUES (:id, :payload)",
            {"id": 1, "payload": None},
        )
        await oracle_async_session.execute(
            "INSERT INTO ora_param_json_null (id, payload) VALUES (:id, :payload)",
            {"id": 2, "payload": {"status": "ok", "missing": None}},
        )

        result = await oracle_async_session.execute("SELECT id, payload FROM ora_param_json_null ORDER BY id")
        rows = [_lower_row(row) for row in result.get_data()]

        assert rows[0] == {"id": 1, "payload": None}
        assert rows[1]["payload"] == {"status": "ok", "missing": None}
    finally:
        await _drop_json_null_table(oracle_async_session)


def test_sync_oracle_bind_count_mismatch_with_none_raises(oracle_sync_session: OracleSyncDriver) -> None:
    """Oracle still rejects missing native numeric binds when provided values include NULL."""
    with pytest.raises(Exception) as exc_info:
        oracle_sync_session.execute("SELECT :1 AS first_value, :2 AS second_value FROM dual", (None,))

    error_message = str(exc_info.value).lower()
    assert "bind" in error_message or "variable" in error_message or "parameter" in error_message
