"""Unit tests for Db2SyncDriver Apache Arrow result conversion."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.exceptions import ImproperConfigurationError
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeDb2Cursor

pa = pytest.importorskip("pyarrow")


def test_select_to_arrow_db2_data_types() -> None:
    """Db2 value shapes (DECIMAL/DECFLOAT, timestamps, binary, boolean) convert to Arrow columns."""
    rows = [
        (
            1,
            Decimal("12345.6789"),
            3.14159,
            "sample text",
            b"\x00\x01\x02\xff",
            date(2026, 9, 22),
            datetime(2026, 9, 22, 12, 0, 0),
            True,
        )
    ]
    description = [
        ("id",),
        ("decfloat_val",),
        ("double_val",),
        ("varchar_val",),
        ("blob_val",),
        ("date_val",),
        ("timestamp_val",),
        ("bool_val",),
    ]
    cursor = FakeDb2Cursor(rows=rows, description=description)
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT * FROM complex_table")
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table["id"].to_pylist() == [1]
    assert table["decfloat_val"].to_pylist() == [Decimal("12345.6789")]
    assert table["double_val"].to_pylist() == [3.14159]
    assert table["varchar_val"].to_pylist() == ["sample text"]
    assert table["blob_val"].to_pylist() == [b"\x00\x01\x02\xff"]
    assert table["date_val"].to_pylist() == [date(2026, 9, 22)]
    assert table["timestamp_val"].to_pylist() == [datetime(2026, 9, 22, 12, 0, 0)]
    assert table["bool_val"].to_pylist() == [True]


def test_select_to_arrow_native_only_raises() -> None:
    """Verify select_to_arrow with native_only=True raises ImproperConfigurationError."""
    cursor = FakeDb2Cursor(rows=[(1,)], description=[("id",)])
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)

    with pytest.raises(ImproperConfigurationError, match="does not support native Arrow"):
        driver.select_to_arrow("SELECT id FROM users", native_only=True)
