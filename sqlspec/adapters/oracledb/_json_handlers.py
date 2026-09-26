"""Oracle native JSON type handlers.

Provides automatic conversion between Python ``dict`` / ``list`` / ``tuple`` values
and Oracle's JSON storage types via connection type handlers.

Routing matrix (input):

* Oracle 21c+ native ``JSON``: bind via ``DB_TYPE_JSON`` (binary OSON).
* Oracle 12c-20c with ``BLOB CHECK (... IS JSON)``: the driver pre-serializes
  JSON-shaped values and creates a ``DB_TYPE_BLOB`` locator containing UTF-8
  JSON before input-handler dispatch.
* Oracle 11g and earlier: bind via ``DB_TYPE_CLOB`` with serialized JSON string.
* The rung is chosen by ``resolve_oracle_json_storage``, the single JSON-threshold
 source shared with ``OracleVersionInfo`` and the extension stores.
* Server major version is read from ``connection._sqlspec_oracle_major`` (set in
 ``OracleSyncConfig._init_connection`` / ``OracleAsyncConfig._init_connection``).
 When unknown, default to 21c+ behavior.

Routing matrix (output):

* ``DB_TYPE_JSON``: passthrough (python-oracledb already returns ``dict``).
* ``FetchInfo.is_oson`` + ``DB_TYPE_BLOB``: decode through
 ``connection.decode_oson``. This branch precedes ``is_json`` because OSON
 metadata can report both flags.
* ``FetchInfo.is_json`` + ``DB_TYPE_BLOB``: fetch as bytes and parse via
 ``json_converter_out_blob``.
* ``FetchInfo.is_json`` + ``DB_TYPE_CLOB`` or string types: parse via
 ``json_converter_out_clob``.

Handlers chain to any pre-existing ``inputtypehandler`` / ``outputtypehandler``
registered on the connection, so registration order
matters: register JSON after numpy, before UUID is also safe because each
handler returns ``None`` for values it does not own.
"""

from functools import partial
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.oracledb._typing import (
    DB_TYPE_BLOB,
    DB_TYPE_CHAR,
    DB_TYPE_CLOB,
    DB_TYPE_JSON,
    DB_TYPE_LONG,
    DB_TYPE_LONG_RAW,
    DB_TYPE_NCHAR,
    DB_TYPE_NVARCHAR,
    DB_TYPE_VARCHAR,
)
from sqlspec.adapters.oracledb.data_dictionary import resolve_oracle_connection_major
from sqlspec.data_dictionary.dialects.oracle import (
    ORACLE_JSON_STORAGE_BLOB_JSON,
    ORACLE_JSON_STORAGE_NATIVE,
    resolve_oracle_json_storage,
)
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection as AsyncConnection
    from sqlspec.adapters.oracledb._typing import OracleAsyncRawCursor as AsyncCursor
    from sqlspec.adapters.oracledb._typing import OracleSyncConnection as Connection
    from sqlspec.adapters.oracledb._typing import OracleSyncRawCursor as Cursor

__all__ = (
    "chain_input_handler",
    "chain_output_handler",
    "is_json_payload",
    "json_converter_in_blob",
    "json_converter_in_clob",
    "json_converter_out_blob",
    "json_converter_out_clob",
    "json_converter_out_oson",
    "json_input_type_handler",
    "json_output_type_handler",
    "register_json_handlers",
)

_JSON_STRING_TYPE_CODES = (DB_TYPE_VARCHAR, DB_TYPE_CHAR, DB_TYPE_NVARCHAR, DB_TYPE_NCHAR)


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


def json_converter_out_oson(connection: "Connection | AsyncConnection", value: "bytes | None") -> Any:
    """Decode OSON bytes from a BLOB read back into a Python value."""
    if value is None:
        return None
    return connection.decode_oson(value)


def json_input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Public input type handler entry point."""
    return _input_type_handler(cursor, value, arraysize)


def json_output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Public output type handler entry point."""
    return _output_type_handler(cursor, metadata)


def _has_input_handler(handler: Any, target_inner: Any) -> bool:
    current = handler
    while current is not None:
        if current is target_inner:
            return True
        if isinstance(current, partial):
            args = current.args
            if args and args[0] is target_inner:
                return True
            if len(args) > 1:
                current = args[1]
                continue
        break
    return False


def _has_output_handler(handler: Any, target_inner: Any) -> bool:
    current = handler
    while current is not None:
        if current is target_inner:
            return True
        if isinstance(current, partial):
            args = current.args
            if args and args[0] is target_inner:
                return True
            if len(args) > 1:
                current = args[1]
                continue
        break
    return False


def register_json_handlers(connection: "Connection | AsyncConnection") -> None:
    """Register JSON type handlers on an Oracle connection idempotently.

    Chains to any existing handlers via ``chain_input_handler`` / ``chain_output_handler``
    so vector / UUID handlers continue to fire for non-JSON values.
    """
    try:
        existing_input = connection.inputtypehandler
    except AttributeError:
        existing_input = None
    try:
        existing_output = connection.outputtypehandler
    except AttributeError:
        existing_output = None

    connection.inputtypehandler = chain_input_handler(_input_type_handler, existing_input)
    connection.outputtypehandler = chain_output_handler(_output_type_handler, existing_output)


def chain_input_handler(inner: Any, fallback: "Any | None") -> Any:
    """Build an input type handler that chains ``inner`` to ``fallback``."""
    if fallback is not None and _has_input_handler(fallback, inner):
        return fallback
    return partial(_chained_input_handler, inner, fallback)


def chain_output_handler(inner: Any, fallback: "Any | None") -> Any:
    """Build an output type handler that chains ``inner`` to ``fallback``."""
    if fallback is not None and _has_output_handler(fallback, inner):
        return fallback
    return partial(_chained_output_handler, inner, fallback)


def is_json_payload(value: Any) -> bool:
    """Return True if the value should be claimed by the JSON input handler.

    ``dict`` and ``tuple``/``list`` of dicts are claimed. Sequences whose first
    element is a number are NOT claimed — those are vector embeddings and
    belong to the vector handler.

    An empty sequence is ambiguous (could be empty vector or empty list) and
    defers to the next handler in the chain. Sequences of numbers (vector
    embeddings) are rejected.
    """
    if isinstance(value, dict):
        return True
    if isinstance(value, (list, tuple)):
        if not value:
            return False
        first = value[0]
        return not (isinstance(first, (int, float)) and not isinstance(first, bool))
    return False


def _input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Oracle input type handler for JSON-shaped Python values."""
    if not is_json_payload(value):
        return None

    server_major = resolve_oracle_connection_major(cursor.connection)

    if server_major is None:
        return cursor.var(DB_TYPE_JSON, arraysize=arraysize)

    storage = resolve_oracle_json_storage(server_major)
    if storage == ORACLE_JSON_STORAGE_NATIVE:
        return cursor.var(DB_TYPE_JSON, arraysize=arraysize)
    if storage == ORACLE_JSON_STORAGE_BLOB_JSON:
        return cursor.var(DB_TYPE_BLOB, arraysize=arraysize, inconverter=json_converter_in_blob)
    return cursor.var(DB_TYPE_CLOB, arraysize=arraysize, inconverter=json_converter_in_clob)


def _output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Oracle output type handler for JSON-bearing column reads.

    For native JSON columns (DB_TYPE_JSON), python-oracledb returns dict/list
    directly, so no conversion is needed.
    """
    type_code = getattr(metadata, "type_code", None)

    if type_code is DB_TYPE_JSON:
        return None

    if getattr(metadata, "is_oson", False) and type_code is DB_TYPE_BLOB:
        return cursor.var(
            DB_TYPE_LONG_RAW,
            arraysize=cursor.arraysize,
            outconverter=partial(json_converter_out_oson, cursor.connection),
        )

    if getattr(metadata, "is_json", False):
        if type_code is DB_TYPE_BLOB:
            return cursor.var(DB_TYPE_LONG_RAW, arraysize=cursor.arraysize, outconverter=json_converter_out_blob)
        if type_code is DB_TYPE_CLOB:
            return cursor.var(DB_TYPE_LONG, arraysize=cursor.arraysize, outconverter=json_converter_out_clob)
        if type_code in _JSON_STRING_TYPE_CODES:
            return cursor.var(type_code, arraysize=cursor.arraysize, outconverter=json_converter_out_clob)

    return None


def _chained_input_handler(
    inner: Any, fallback: "Any | None", cursor: "Cursor | AsyncCursor", value: Any, arraysize: int
) -> Any:
    """Run ``inner`` input handler, falling back to ``fallback`` when it abstains."""
    result = inner(cursor, value, arraysize)
    if result is not None:
        return result
    if fallback is not None:
        return fallback(cursor, value, arraysize)
    return None


def _chained_output_handler(inner: Any, fallback: "Any | None", cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Run ``inner`` output handler, falling back to ``fallback`` when it abstains."""
    result = inner(cursor, metadata)
    if result is not None:
        return result
    if fallback is not None:
        return fallback(cursor, metadata)
    return None
