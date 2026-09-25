"""Psycopg pgvector type handlers for vector data type support.

Provides automatic conversion between NumPy arrays and PostgreSQL vector types
via pgvector-python library. Supports both sync and async connections.

The optional pgvector.psycopg module is cached at import time so registration
does not pay importlib dispatch cost per connection.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_optional

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgConnection as Connection
    from sqlspec.adapters.psycopg._typing import PsycopgNativeAsyncConnection as AsyncConnection

__all__ = ("register_pgvector_async", "register_pgvector_sync")


logger = get_logger(__name__)

_pgvector_psycopg: Any | None = import_optional("pgvector.psycopg")
_cached_sync_type_infos: dict[str, Any] = {}
_cached_async_type_infos: dict[str, Any] = {}


def register_pgvector_sync(connection: "Connection[Any]") -> None:
    """Register pgvector type handlers on psycopg sync connection.

    Enables automatic conversion between NumPy arrays and PostgreSQL vector types
    using the pgvector-python library with cached TypeInfo lookups.

    Args:
        connection: Psycopg sync connection.
    """
    from sqlspec.adapters.psycopg._typing import PsycopgProgrammingError as ProgrammingError

    pgvector_psycopg = _pgvector_psycopg
    if pgvector_psycopg is None:
        return

    if _cached_sync_type_infos:
        try:
            from pgvector.psycopg.bit import register_bit_info
            from pgvector.psycopg.halfvec import register_halfvec_info
            from pgvector.psycopg.sparsevec import register_sparsevec_info
            from pgvector.psycopg.vector import register_vector_info

            if _cached_sync_type_infos.get("vector") is not None:
                register_vector_info(connection, _cached_sync_type_infos["vector"])
            if _cached_sync_type_infos.get("bit") is not None:
                register_bit_info(connection, _cached_sync_type_infos["bit"])
            if _cached_sync_type_infos.get("halfvec") is not None:
                register_halfvec_info(connection, _cached_sync_type_infos["halfvec"])
            if _cached_sync_type_infos.get("sparsevec") is not None:
                register_sparsevec_info(connection, _cached_sync_type_infos["sparsevec"])
        except Exception:
            _cached_sync_type_infos.clear()
        else:
            return

    try:
        pgvector_psycopg.register_vector(connection)
        for type_name in ("vector", "bit", "halfvec", "sparsevec"):
            _cached_sync_type_infos[type_name] = _fetch_type_info_sync(connection, type_name)
    except (ValueError, TypeError, ProgrammingError) as error:
        if _is_missing_vector_error(error):
            return
        logger.warning("Unexpected error during pgvector registration: %s", error)
    except Exception:
        logger.exception("Failed to register pgvector for psycopg sync")


async def register_pgvector_async(connection: "AsyncConnection[Any]") -> None:
    """Register pgvector type handlers on psycopg async connection.

    Enables automatic conversion between NumPy arrays and PostgreSQL vector types
    using the pgvector-python library with cached TypeInfo lookups.

    Args:
        connection: Psycopg async connection.
    """
    from sqlspec.adapters.psycopg._typing import PsycopgProgrammingError as ProgrammingError

    pgvector_psycopg = _pgvector_psycopg
    if pgvector_psycopg is None:
        return

    if _cached_async_type_infos:
        try:
            from pgvector.psycopg.bit import register_bit_info
            from pgvector.psycopg.halfvec import register_halfvec_info
            from pgvector.psycopg.sparsevec import register_sparsevec_info
            from pgvector.psycopg.vector import register_vector_info

            if _cached_async_type_infos.get("vector") is not None:
                register_vector_info(connection, _cached_async_type_infos["vector"])
            if _cached_async_type_infos.get("bit") is not None:
                register_bit_info(connection, _cached_async_type_infos["bit"])
            if _cached_async_type_infos.get("halfvec") is not None:
                register_halfvec_info(connection, _cached_async_type_infos["halfvec"])
            if _cached_async_type_infos.get("sparsevec") is not None:
                register_sparsevec_info(connection, _cached_async_type_infos["sparsevec"])
        except Exception:
            _cached_async_type_infos.clear()
        else:
            return

    try:
        register_vector_async = pgvector_psycopg.register_vector_async
        await register_vector_async(connection)
        for type_name in ("vector", "bit", "halfvec", "sparsevec"):
            _cached_async_type_infos[type_name] = await _fetch_type_info_async(connection, type_name)
    except (ValueError, TypeError, ProgrammingError) as error:
        if _is_missing_vector_error(error):
            return
        logger.warning("Unexpected error during pgvector registration: %s", error)
    except Exception:
        logger.exception("Failed to register pgvector for psycopg async")


def _fetch_type_info_sync(connection: "Connection[Any]", type_name: str) -> Any:
    """Fetch TypeInfo synchronously, returning None on failure."""
    from psycopg.types import TypeInfo

    try:
        return TypeInfo.fetch(connection, type_name)
    except Exception:
        return None


async def _fetch_type_info_async(connection: "AsyncConnection[Any]", type_name: str) -> Any:
    """Fetch TypeInfo asynchronously, returning None on failure."""
    from psycopg.types import TypeInfo

    try:
        return await TypeInfo.fetch(connection, type_name)
    except Exception:
        return None


def _is_missing_vector_error(error: Exception) -> bool:
    """Check if error indicates missing vector type in database.

    Args:
        error: Exception to check.

    Returns:
        True if error indicates vector type not found.
    """
    from sqlspec.adapters.psycopg._typing import psycopg_errors as errors

    message = str(error).lower()
    return (
        "vector type not found" in message
        or 'type "vector" does not exist' in message
        or "vector type does not exist" in message
        or isinstance(error, errors.UndefinedObject)
    )
