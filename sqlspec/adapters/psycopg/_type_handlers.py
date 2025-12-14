"""Psycopg pgvector type handlers for vector data type support.

Provides automatic conversion between NumPy arrays and PostgreSQL vector types
via pgvector-python library. Supports both sync and async connections.
"""

from typing import TYPE_CHECKING, Any

from psycopg import ProgrammingError, errors

from sqlspec.typing import NUMPY_INSTALLED, PGVECTOR_INSTALLED
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from psycopg import AsyncConnection, Connection

__all__ = ("register_pgvector_async", "register_pgvector_sync")


logger = get_logger(__name__)


def _is_missing_vector_error(error: Exception) -> bool:
    message = str(error).lower()
    return (
        "vector type not found" in message
        or 'type "vector" does not exist' in message
        or "vector type does not exist" in message
        or isinstance(error, errors.UndefinedObject)
    )


def register_pgvector_sync(connection: "Connection[Any]") -> None:
    """Register pgvector type handlers on psycopg sync connection.

    Enables automatic conversion between NumPy arrays and PostgreSQL vector types
    using the pgvector-python library.

    Args:
        connection: Psycopg sync connection.
    """
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type handlers")
        return

    if not NUMPY_INSTALLED:
        logger.debug("NumPy not installed - registering pgvector without NumPy support")

    try:
        import pgvector.psycopg

        pgvector.psycopg.register_vector(connection)
        logger.debug("Registered pgvector type handlers on psycopg sync connection")
    except (ValueError, TypeError, ProgrammingError) as error:
        if _is_missing_vector_error(error):
            logger.debug("Skipping pgvector registration - extension not enabled in database")
            return
        logger.warning("Unexpected error during pgvector registration: %s", error)
    except Exception:
        logger.exception("Failed to register pgvector for psycopg sync")


async def register_pgvector_async(connection: "AsyncConnection[Any]") -> None:
    """Register pgvector type handlers on psycopg async connection.

    Enables automatic conversion between NumPy arrays and PostgreSQL vector types
    using the pgvector-python library.

    Args:
        connection: Psycopg async connection.
    """
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type handlers")
        return

    if not NUMPY_INSTALLED:
        logger.debug("NumPy not installed - registering pgvector without NumPy support")

    try:
        from pgvector.psycopg import register_vector_async

        await register_vector_async(connection)
        logger.debug("Registered pgvector type handlers on psycopg async connection")
    except (ValueError, TypeError, ProgrammingError) as error:
        if _is_missing_vector_error(error):
            logger.debug("Skipping pgvector registration - extension not enabled in database")
            return
        logger.warning("Unexpected error during pgvector registration: %s", error)
    except Exception:
        logger.exception("Failed to register pgvector for psycopg async")
