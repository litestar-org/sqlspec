"""Native Snowflake Arrow results and cursor ownership through public APIs."""

from typing import Any, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.snowflake import SnowflakeDriver


@pytest.mark.parametrize("return_format", ["table", "reader", "batch", "batches"])
def test_empty_arrow_results_preserve_typed_schema(snowflake_connection: Any, return_format: Any) -> None:
    snowflake_connection.default_result = {
        "description": [
            ("ID", 0, None, None, 18, 0, False),
            ("AMOUNT", 0, None, None, 38, 4, True),
            ("camelCase", 2, None, None, None, None, True),
            ("STAMP", 8, None, None, None, None, True),
        ],
        "rows": [],
    }
    result = SnowflakeDriver(snowflake_connection).select_to_arrow(
        "SELECT ID, AMOUNT, camelCase, STAMP FROM data WHERE 1=0", return_format=return_format
    )
    data = result.get_data()
    if return_format == "reader":
        table = data.read_all()
        data.close()
    elif return_format == "batch":
        table = pa.Table.from_batches([data])
    elif return_format == "batches":
        table = pa.Table.from_batches(
            data,
            schema=pa.schema([
                pa.field("id", pa.int64(), nullable=False),
                pa.field("amount", pa.decimal128(38, 4)),
                pa.field("camelCase", pa.string()),
                pa.field("stamp", pa.timestamp("ns", tz="UTC")),
            ]),
        )
    else:
        table = data
    assert table.num_rows == 0
    assert table.column_names == ["id", "amount", "camelCase", "stamp"]
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("amount").type == pa.decimal128(38, 4)
    assert table.schema.field("stamp").type == pa.timestamp("ns", tz="UTC")
    assert snowflake_connection.cursors[-1].closed


def test_native_arrow_batches_normalize_names_without_copying_buffers(snowflake_connection: Any) -> None:
    first = pa.table({"ID": [1, 2], "camelCase": ["a", "b"]})
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, False), ("camelCase", 2, None, None, None, None, False)],
        "tables": [first],
    }
    table = SnowflakeDriver(snowflake_connection).select_to_arrow("SELECT ID, camelCase FROM data").get_data()
    assert table.to_pylist() == [{"id": 1, "camelCase": "a"}, {"id": 2, "camelCase": "b"}]
    original_buffer = cast("pa.Buffer", first.column("ID").chunks[0].buffers()[1])
    assert table.column("id").chunks[0].buffers()[1].address == original_buffer.address


def test_multiple_arrow_batches_and_early_reader_close(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, False)],
        "tables": [pa.table({"ID": [1, 2]}), pa.table({"ID": [3]})],
    }
    driver = SnowflakeDriver(snowflake_connection)
    assert driver.select_to_arrow("SELECT ID FROM data").get_data().to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]
    reader = driver.select_to_arrow("SELECT ID FROM data", return_format="reader", batch_size=1).get_data()
    assert reader.read_next_batch().to_pylist() == [{"id": 1}]
    assert snowflake_connection.cursors[-1].closed is False
    reader.close()
    assert snowflake_connection.cursors[-1].closed


def test_driver_close_closes_outstanding_arrow_readers(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, False)],
        "tables": [pa.table({"ID": [1]})],
    }
    driver = SnowflakeDriver(snowflake_connection)
    result = driver.select_to_arrow("SELECT ID FROM data", return_format="reader")
    driver.close()
    assert snowflake_connection.cursors[-1].closed
    result.get_data().close()


@pytest.mark.parametrize("return_format", ["table", "reader"])
def test_json_format_commands_fall_back_from_not_supported(snowflake_connection: Any, return_format: Any) -> None:
    from snowflake.connector.errors import NotSupportedError

    snowflake_connection.default_result = {
        "description": [("NAME", 2, None, None, None, None, False)],
        "rows": [("DATA",)],
        "arrow_error": NotSupportedError(msg="JSON result format"),
    }
    result = (
        SnowflakeDriver(snowflake_connection).select_to_arrow("SHOW TABLES", return_format=return_format).get_data()
    )
    table = result.read_all() if return_format == "reader" else result
    assert table.to_pylist() == [{"name": "DATA"}]
    if return_format == "reader":
        result.close()
    assert snowflake_connection.cursors[-1].closed


def test_arrow_backed_row_stream_decodes_variant_and_closes_on_break(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, False), ("PAYLOAD", 5, None, None, None, None, True)],
        "tables": [pa.table({"ID": [1, 2], "PAYLOAD": ['{"x":1}', '{"x":2}']})],
    }
    driver = SnowflakeDriver(snowflake_connection)
    with driver.select_stream("SELECT ID, PAYLOAD FROM data", chunk_size=1) as stream:
        assert next(stream) == {"id": 1, "payload": {"x": 1}}
    assert snowflake_connection.cursors[-1].closed


def test_session_close_releases_active_arrow_row_stream_once(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, False)],
        "tables": [pa.table({"ID": [1, 2]})],
    }
    driver = SnowflakeDriver(snowflake_connection)
    stream = driver.select_stream("SELECT ID FROM data", chunk_size=1)
    assert next(stream) == {"id": 1}
    cursor = snowflake_connection.cursors[-1]
    driver.close()
    assert cursor.closed
    stream.close()
    assert cursor.close_count == 1


def test_arrow_materialization_promotes_null_batches_without_losing_rows(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, True)],
        "tables": [pa.table({"ID": pa.nulls(1)}), pa.table({"ID": [2]})],
    }
    table = SnowflakeDriver(snowflake_connection).select_to_arrow("SELECT ID FROM data").get_data()
    assert table.column("id").type == pa.int64()
    assert table.to_pylist() == [{"id": None}, {"id": 2}]


def test_arrow_reader_casts_compatible_later_batch_to_first_schema(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, True)],
        "tables": [pa.table({"ID": pa.array([1], type=pa.int64())}), pa.table({"ID": pa.array([2], type=pa.int32())})],
    }
    reader = (
        SnowflakeDriver(snowflake_connection).select_to_arrow("SELECT ID FROM data", return_format="reader").get_data()
    )
    table = reader.read_all()
    assert table.column("id").type == pa.int64()
    assert table.to_pylist() == [{"id": 1}, {"id": 2}]
    assert snowflake_connection.cursors[-1].closed


def test_arrow_reader_refuses_incompatible_later_batch_and_closes_cursor(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, True)],
        "tables": [pa.table({"ID": [1]}), pa.table({"ID": ["bad"]})],
    }
    reader = (
        SnowflakeDriver(snowflake_connection).select_to_arrow("SELECT ID FROM data", return_format="reader").get_data()
    )
    with pytest.raises(pa.ArrowInvalid, match="bad"):
        reader.read_all()
    assert snowflake_connection.cursors[-1].closed
