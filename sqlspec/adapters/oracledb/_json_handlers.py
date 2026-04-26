"""Oracle native JSON type handlers.

Provides automatic conversion between Python ``dict`` / ``list`` / ``tuple`` values
and Oracle's JSON storage types via connection type handlers.

Routing matrix (input):

* Oracle 21c+ native ``JSON``: bind via ``DB_TYPE_JSON`` (binary OSON).
* Oracle 19c-20c with ``BLOB CHECK (... IS JSON)``: bind via ``DB_TYPE_BLOB`` with
  UTF-8 JSON bytes.
* Oracle 12c-18c with ``CLOB CHECK (... IS JSON)``: bind via ``DB_TYPE_CLOB`` with
  serialized JSON string.
* Server major version is read from ``connection._sqlspec_oracle_major`` (set in
  ``OracleSyncConfig._init_connection`` / ``OracleAsyncConfig._init_connection``).
  When unknown, default to 21c+ behavior.

Routing matrix (output):

* ``DB_TYPE_JSON``: passthrough (python-oracledb already returns ``dict``).
* ``DB_TYPE_BLOB`` with ``JSON`` in column ``type_name``: parse via
  ``json_converter_out_blob``.
* ``DB_TYPE_CLOB`` with ``JSON`` in column ``type_name``: parse via
  ``json_converter_out_clob``.

Handlers chain to any pre-existing ``inputtypehandler`` / ``outputtypehandler``
registered on the connection (e.g. NumPy vector, UUID), so registration order
matters: register JSON after numpy, before UUID is also safe because each
handler returns ``None`` for values it does not own.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

__all__ = (
    "json_converter_in_blob",
    "json_converter_in_clob",
    "json_converter_out_blob",
    "json_converter_out_clob",
    "json_input_type_handler",
    "json_output_type_handler",
    "register_json_handlers",
)


_JSON_TYPE_NAME_MARKER = "JSON"

# Server-version thresholds for JSON binding strategy selection.
# 21c+ supports DB_TYPE_JSON (binary OSON); 19c-20c uses BLOB CHECK (... IS JSON);
# pre-19c uses CLOB CHECK (... IS JSON).
_NATIVE_JSON_MIN_MAJOR = 21
_BLOB_IS_JSON_MIN_MAJOR = 19


def json_converter_in_clob(value: Any) -> str:
    """Serialize a Python value to a JSON string for CLOB binding."""
    return to_json(value)


def json_converter_in_blob(value: Any) -> bytes:
    """Serialize a Python value to UTF-8 JSON bytes for BLOB binding."""
    return to_json(value, as_bytes=True)


def json_converter_out_clob(value: "str | None") -> Any:
    """Parse a JSON string from a CLOB read back into a Python value."""
    if value is None:
        return None
    return from_json(value)


def json_converter_out_blob(value: "bytes | None") -> Any:
    """Parse JSON bytes from a BLOB read back into a Python value."""
    if value is None:
        return None
    return from_json(value)


def _is_json_payload(value: Any) -> bool:
    """Return True if the value should be claimed by the JSON input handler.

    ``dict`` and ``tuple``/``list`` of dicts are claimed. Sequences whose first
    element is a number are NOT claimed — those are vector embeddings and
    belong to the vector handler.
    """
    if isinstance(value, dict):
        return True
    if isinstance(value, (list, tuple)):
        if not value:
            # Empty sequence: ambiguous (could be empty vector or empty list).
            # Defer to the next handler in the chain.
            return False
        first = value[0]
        # Reject sequences of numbers (vector embeddings).
        return not (isinstance(first, (int, float)) and not isinstance(first, bool))
    return False


def _input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Oracle input type handler for JSON-shaped Python values."""
    import oracledb

    if not _is_json_payload(value):
        return None

    server_major = getattr(cursor.connection, "_sqlspec_oracle_major", None)

    if server_major is None or server_major >= _NATIVE_JSON_MIN_MAJOR:
        return cursor.var(oracledb.DB_TYPE_JSON, arraysize=arraysize)
    if server_major >= _BLOB_IS_JSON_MIN_MAJOR:
        return cursor.var(oracledb.DB_TYPE_BLOB, arraysize=arraysize, inconverter=json_converter_in_blob)
    return cursor.var(oracledb.DB_TYPE_CLOB, arraysize=arraysize, inconverter=json_converter_in_clob)


def _output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Oracle output type handler for JSON-bearing column reads."""
    import oracledb

    type_code = getattr(metadata, "type_code", None)

    if type_code is oracledb.DB_TYPE_JSON:
        # Native JSON: python-oracledb returns dict/list directly. No conversion.
        return None

    type_name = (getattr(metadata, "type_name", "") or "").upper()
    if _JSON_TYPE_NAME_MARKER not in type_name:
        return None

    if type_code is oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_BLOB, arraysize=cursor.arraysize, outconverter=json_converter_out_blob)
    if type_code is oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_CLOB, arraysize=cursor.arraysize, outconverter=json_converter_out_clob)
    return None


def json_input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Public input type handler entry point."""
    return _input_type_handler(cursor, value, arraysize)


def json_output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Public output type handler entry point."""
    return _output_type_handler(cursor, metadata)


def register_json_handlers(connection: "Connection | AsyncConnection") -> None:
    """Register JSON type handlers on an Oracle connection.

    Chains to any existing handlers via ``_JsonInputHandler`` / ``_JsonOutputHandler``
    wrapper classes so vector / UUID handlers continue to fire for non-JSON values.
    """
    try:
        existing_input = connection.inputtypehandler
    except AttributeError:
        existing_input = None
    try:
        existing_output = connection.outputtypehandler
    except AttributeError:
        existing_output = None

    connection.inputtypehandler = _JsonInputHandler(existing_input)
    connection.outputtypehandler = _JsonOutputHandler(existing_output)


class _JsonInputHandler:
    """Chaining wrapper that claims dict/list/tuple values, falling back otherwise."""

    __slots__ = ("_fallback",)

    def __init__(self, fallback: "Any | None") -> None:
        self._fallback = fallback

    def __call__(self, cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
        result = _input_type_handler(cursor, value, arraysize)
        if result is not None:
            return result
        if self._fallback is not None:
            return self._fallback(cursor, value, arraysize)
        return None


class _JsonOutputHandler:
    """Chaining wrapper that claims JSON-bearing columns, falling back otherwise."""

    __slots__ = ("_fallback",)

    def __init__(self, fallback: "Any | None") -> None:
        self._fallback = fallback

    def __call__(self, cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
        result = _output_type_handler(cursor, metadata)
        if result is not None:
            return result
        if self._fallback is not None:
            return self._fallback(cursor, metadata)
        return None
