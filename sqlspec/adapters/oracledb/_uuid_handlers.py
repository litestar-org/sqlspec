"""Oracle UUID type handlers for RAW(16) binary storage.

Provides automatic conversion between Python UUID objects and Oracle RAW(16)
via connection type handlers. Uses stdlib uuid (no external dependencies).
"""

import logging
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

__all__ = ("register_uuid_handlers", "uuid_converter_in", "uuid_converter_out")


logger = logging.getLogger(__name__)


UUID_BINARY_SIZE = 16


def uuid_converter_in(value: uuid.UUID) -> bytes:
    """Convert Python UUID to 16-byte binary for Oracle RAW(16).

    Args:
        value: Python UUID object to convert.

    Returns:
        16-byte binary representation in big-endian format (RFC 4122).
    """
    return value.bytes


def uuid_converter_out(value: bytes | None) -> "uuid.UUID | bytes | None":
    """Convert 16-byte binary from Oracle RAW(16) to Python UUID.

    Falls back to bytes if value is not valid UUID format.

    Args:
        value: 16-byte binary from Oracle RAW(16) column, or None.

    Returns:
        Python UUID object if valid, original bytes if invalid, None if NULL.
    """
    if value is None:
        return None

    if len(value) != UUID_BINARY_SIZE:
        return value

    try:
        return uuid.UUID(bytes=value)
    except (ValueError, TypeError):
        logger.debug("RAW(16) value is not valid UUID format, returning as bytes", extra={"value_length": len(value)})
        return value


def _input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Oracle input type handler for UUID objects.

    Detects Python UUID objects and converts them to RAW(16) binary format.

    Args:
        cursor: Oracle cursor (sync or async).
        value: Value being inserted.
        arraysize: Array size for the cursor variable.

    Returns:
        Cursor variable with UUID converter if value is UUID, None otherwise.
    """
    import oracledb

    if isinstance(value, uuid.UUID):
        return cursor.var(oracledb.DB_TYPE_RAW, arraysize=arraysize, inconverter=uuid_converter_in)
    return None


def _output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Oracle output type handler for RAW(16) columns.

    Detects RAW(16) columns and converts them to Python UUID objects.

    Args:
        cursor: Oracle cursor (sync or async).
        metadata: Column metadata from Oracle.

    Returns:
        Cursor variable with UUID converter if column is RAW(16), None otherwise.
    """
    import oracledb

    if metadata.type_code is oracledb.DB_TYPE_RAW and metadata.size == UUID_BINARY_SIZE:
        return cursor.var(metadata.type_code, arraysize=cursor.arraysize, outconverter=uuid_converter_out)
    return None


def register_uuid_handlers(connection: "Connection | AsyncConnection") -> None:
    """Register UUID type handlers with chaining support.

    Chains to existing type handlers (e.g., NumPy vectors) to avoid conflicts.
    Works for both sync and async connections.

    Args:
        connection: Oracle connection (sync or async).
    """
    existing_input = getattr(connection, "inputtypehandler", None)
    existing_output = getattr(connection, "outputtypehandler", None)

    def combined_input_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
        result = _input_type_handler(cursor, value, arraysize)
        if result is not None:
            return result
        if existing_input is not None:
            return existing_input(cursor, value, arraysize)
        return None

    def combined_output_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
        result = _output_type_handler(cursor, metadata)
        if result is not None:
            return result
        if existing_output is not None:
            return existing_output(cursor, metadata)
        return None

    connection.inputtypehandler = combined_input_handler
    connection.outputtypehandler = combined_output_handler
    logger.debug("Registered UUID type handlers on Oracle connection with chaining support")
