"""Psycopg pgvector type handlers for vector data type support.

Provides automatic conversion between NumPy arrays and PostgreSQL vector types
via pgvector-python library. Supports both sync and async connections.

All functions are designed for mypyc compilation with imports inside
functions to avoid module-level optional dependency issues.
"""

import importlib
from typing import TYPE_CHECKING, Any

from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from psycopg import AsyncConnection, Connection

__all__ = ("register_pgvector_async", "register_pgvector_sync")


logger = get_logger(__name__)


def _is_missing_vector_error(error: Exception) -> bool:
    """Check if error indicates missing vector type in database.

    Args:
        error: Exception to check.

    Returns:
        True if error indicates vector type not found.
    """
    from psycopg import errors

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
    from psycopg import ProgrammingError

    if not PGVECTOR_INSTALLED:
        return

    try:
        pgvector_psycopg = importlib.import_module("pgvector.psycopg")
        pgvector_psycopg.register_vector(connection)
    except (ValueError, TypeError, ProgrammingError) as error:
        if _is_missing_vector_error(error):
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
    from psycopg import ProgrammingError

    if not PGVECTOR_INSTALLED:
        return

    try:
        pgvector_psycopg = importlib.import_module("pgvector.psycopg")
        register_vector_async = pgvector_psycopg.register_vector_async
        await register_vector_async(connection)
    except (ValueError, TypeError, ProgrammingError) as error:
        if _is_missing_vector_error(error):
            return
        logger.warning("Unexpected error during pgvector registration: %s", error)
    except Exception:
        logger.exception("Failed to register pgvector for psycopg async")
