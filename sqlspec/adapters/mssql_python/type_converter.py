"""Type converters for mssql-python parameter binding."""

from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyarrow as pa

__all__ = ("MssqlPythonTypeConverter", "mssql_type_to_arrow")

_MSSQL_ARROW_TYPE_SPECS: Final[dict[str, tuple[str, tuple[Any, ...], dict[str, Any]]]] = {
    "bit": ("bool_", (), {}),
    "tinyint": ("uint8", (), {}),
    "smallint": ("int16", (), {}),
    "int": ("int32", (), {}),
    "bigint": ("int64", (), {}),
    "float": ("float64", (), {}),
    "real": ("float32", (), {}),
    "smallmoney": ("decimal128", (10, 4), {}),
    "money": ("decimal128", (19, 4), {}),
    "date": ("date32", (), {}),
    "datetime": ("timestamp", ("ms",), {}),
    "datetime2": ("timestamp", ("us",), {}),
    "smalldatetime": ("timestamp", ("s",), {}),
    "datetimeoffset": ("timestamp", ("us",), {"tz": "UTC"}),
    "uniqueidentifier": ("string", (), {}),
    "xml": ("string", (), {}),
    "image": ("binary", (), {}),
    "binary": ("binary", (), {}),
    "varbinary": ("binary", (), {}),
    "timestamp": ("binary", (), {}),
    "rowversion": ("binary", (), {}),
    "char": ("string", (), {}),
    "varchar": ("string", (), {}),
    "nchar": ("string", (), {}),
    "nvarchar": ("string", (), {}),
    "text": ("string", (), {}),
    "ntext": ("string", (), {}),
}


class MssqlPythonTypeConverter:
    """Utility converter for explicit mssql-python value coercion.

    The driver pipeline builds its internal coercions directly from feature
    flags. This class is a public utility for callers that need per-value bind
    or result coercion outside the driver execution path.
    """

    __slots__ = ("_json_deserializer", "_json_serializer")

    def __init__(
        self, json_serializer: "Callable[[Any], str]" = to_json, json_deserializer: "Callable[[str], Any]" = from_json
    ) -> None:
        self._json_serializer = json_serializer
        self._json_deserializer = json_deserializer

    def coerce_bind_value(self, value: "Any") -> "Any":
        """Coerce Python values before mssql-python parameter binding."""
        if isinstance(value, (dict, list)):
            return self._json_serializer(value)
        if isinstance(value, UUID):
            return value
        return value

    def coerce_read_value(self, value: "Any") -> "Any":
        """Coerce mssql-python result values after fetching."""
        return value


def mssql_type_to_arrow(sql_type: str, *, precision: int | None = None, scale: int | None = None) -> "pa.DataType":
    """Resolve a T-SQL type name to an Arrow data type."""
    normalized_type = sql_type.lower().split("(", 1)[0].strip()
    if normalized_type in {"decimal", "numeric"} and precision is not None and scale is not None:
        return _arrow_type("decimal128", (precision, scale))
    spec = _MSSQL_ARROW_TYPE_SPECS.get(normalized_type)
    if spec is None:
        return _arrow_type("string")
    name, args, kwargs = spec
    return _arrow_type(name, args, kwargs)


def _arrow_type(name: str, args: tuple[Any, ...] = (), kwargs: dict[str, Any] | None = None) -> "pa.DataType":
    ensure_pyarrow()
    import pyarrow as pa

    return cast("pa.DataType", getattr(pa, name)(*args, **(kwargs or {})))
