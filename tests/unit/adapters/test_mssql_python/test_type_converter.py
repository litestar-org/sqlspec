"""Unit tests for mssql_python type conversion helpers."""

from uuid import UUID

from sqlspec.adapters.mssql_python.type_converter import MssqlPythonTypeConverter, mssql_type_to_arrow


def test_type_converter_serializes_json_containers() -> None:
    """dict and list parameters should bind as JSON text."""
    converter = MssqlPythonTypeConverter(json_serializer=lambda value: f"json:{value!r}")

    assert converter.coerce_bind_value({"a": 1}) == "json:{'a': 1}"
    assert converter.coerce_bind_value([1, 2]) == "json:[1, 2]"


def test_type_converter_preserves_uuid_values() -> None:
    """mssql-python 1.5+ binds UUID values natively."""
    value = UUID("00000000-0000-0000-0000-000000000001")
    converter = MssqlPythonTypeConverter()

    assert converter.coerce_bind_value(value) is value


def test_mssql_type_to_arrow_maps_core_sql_server_types() -> None:
    """SQL Server type names should resolve to stable Arrow data types."""
    import pyarrow as pa

    assert mssql_type_to_arrow("bit") == pa.bool_()
    assert mssql_type_to_arrow("int") == pa.int32()
    assert mssql_type_to_arrow("datetimeoffset") == pa.timestamp("us", tz="UTC")
    assert mssql_type_to_arrow("uniqueidentifier") == pa.string()


def test_mssql_type_to_arrow_uses_decimal_precision_and_scale() -> None:
    """DECIMAL and NUMERIC should preserve declared precision and scale."""
    import pyarrow as pa

    assert mssql_type_to_arrow("decimal", precision=18, scale=4) == pa.decimal128(18, 4)
    assert mssql_type_to_arrow("numeric", precision=10, scale=2) == pa.decimal128(10, 2)


def test_mssql_type_converter_public_package_imports() -> None:
    from sqlspec.adapters.mssql_python import MssqlPythonTypeConverter as ExportedConverter
    from sqlspec.adapters.mssql_python import mssql_type_to_arrow as exported_mssql_type_to_arrow

    assert ExportedConverter is MssqlPythonTypeConverter
    assert exported_mssql_type_to_arrow is mssql_type_to_arrow


def test_mssql_type_converter_public_all() -> None:
    import sqlspec.adapters.mssql_python as mssql_python

    assert "MssqlPythonTypeConverter" in mssql_python.__all__
    assert "mssql_type_to_arrow" in mssql_python.__all__
