"""Arrow type helpers for generic ODBC result sets."""

from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = ("ArrowOdbcTypeConverter", "odbc_type_to_arrow")

_ODBC_ARROW_TYPE_SPECS: Final[dict[str, tuple[str, tuple[Any, ...], dict[str, Any]]]] = {
    "bit": ("bool_", (), {}),
    "boolean": ("bool_", (), {}),
    "bool": ("bool_", (), {}),
    "tinyint": ("uint8", (), {}),
    "smallint": ("int16", (), {}),
    "integer": ("int32", (), {}),
    "int": ("int32", (), {}),
    "bigint": ("int64", (), {}),
    "float": ("float64", (), {}),
    "double": ("float64", (), {}),
    "real": ("float32", (), {}),
    "smallmoney": ("decimal128", (10, 4), {}),
    "money": ("decimal128", (19, 4), {}),
    "decimal": ("decimal128", (38, 10), {}),
    "numeric": ("decimal128", (38, 10), {}),
    "date": ("date32", (), {}),
    "time": ("time64", ("us",), {}),
    "timestamp": ("timestamp", ("us",), {}),
    "datetime": ("timestamp", ("us",), {}),
    "datetime2": ("timestamp", ("us",), {}),
    "smalldatetime": ("timestamp", ("s",), {}),
    "datetimeoffset": ("timestamp", ("us",), {"tz": "UTC"}),
    "char": ("string", (), {}),
    "varchar": ("string", (), {}),
    "nchar": ("string", (), {}),
    "nvarchar": ("string", (), {}),
    "text": ("string", (), {}),
    "ntext": ("string", (), {}),
    "clob": ("string", (), {}),
    "xml": ("string", (), {}),
    "blob": ("binary", (), {}),
    "image": ("binary", (), {}),
    "binary": ("binary", (), {}),
    "varbinary": ("binary", (), {}),
    "rowversion": ("binary", (), {}),
    "uuid": ("string", (), {}),
    "uniqueidentifier": ("string", (), {}),
}


class ArrowOdbcTypeConverter:
    """Small bind/read converter surface for arrow-odbc."""

    __slots__ = ()

    def coerce_bind_value(self, value: "Any") -> "Any":
        """Coerce values before arrow-odbc parameter binding."""
        return None if value is None else str(value)

    def coerce_read_value(self, value: "Any") -> "Any":
        """Return arrow-odbc result values unchanged."""
        return value


def odbc_type_to_arrow(sql_type: str, *, precision: int | None = None, scale: int | None = None) -> "pa.DataType":
    """Resolve an ODBC SQL type name to an Arrow data type."""
    normalized_type = sql_type.lower().split("(", 1)[0].strip()
    if normalized_type in {"decimal", "numeric"} and precision is not None and scale is not None:
        return _arrow_type("decimal128", (precision, scale))
    spec = _ODBC_ARROW_TYPE_SPECS.get(normalized_type)
    if spec is None:
        return _arrow_type("string")
    name, args, kwargs = spec
    return _arrow_type(name, args, kwargs)


def _arrow_type(name: str, args: tuple[Any, ...] = (), kwargs: dict[str, Any] | None = None) -> "pa.DataType":
    ensure_pyarrow()
    import pyarrow as pa

    return cast("pa.DataType", getattr(pa, name)(*args, **(kwargs or {})))
