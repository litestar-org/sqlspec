"""AsyncPG type handlers for JSON and pgvector support.

Provides automatic registration of JSON codecs and pgvector extension
for asyncpg connections. Supports custom JSON serializers/deserializers
and optional vector type support.
"""

import logging
from typing import TYPE_CHECKING, Any

from sqlspec.typing import PGVECTOR_INSTALLED

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.asyncpg._types import AsyncpgConnection

__all__ = ("register_json_codecs", "register_pgvector_support")

logger = logging.getLogger(__name__)


def _is_missing_vector_error(error: Exception) -> bool:
    message = str(error).lower()
    return 'type "vector" does not exist' in message or "unknown type" in message


async def register_json_codecs(
    connection: "AsyncpgConnection", encoder: "Callable[[Any], str]", decoder: "Callable[[str], Any]"
) -> None:
    """Register JSON type codecs on asyncpg connection.

    Configures both JSON and JSONB types with custom serializer/deserializer
    functions. This allows using custom JSON libraries like orjson or msgspec
    for better performance.

    Args:
        connection: AsyncPG connection instance.
        encoder: Function to serialize Python objects to JSON strings.
        decoder: Function to deserialize JSON strings to Python objects.
    """
    try:
        await connection.set_type_codec("json", encoder=encoder, decoder=decoder, schema="pg_catalog")
        await connection.set_type_codec("jsonb", encoder=encoder, decoder=decoder, schema="pg_catalog")
        logger.debug("Registered JSON type codecs on asyncpg connection")
    except Exception:
        logger.exception("Failed to register JSON type codecs")


async def register_pgvector_support(connection: "AsyncpgConnection") -> None:
    """Register pgvector extension support on asyncpg connection.

    Enables automatic conversion between Python vector types and PostgreSQL
    VECTOR columns when the pgvector library is installed. Gracefully skips
    if pgvector is not available.

    Args:
        connection: AsyncPG connection instance.
    """
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type support")
        return

    try:
        import pgvector.asyncpg

        await pgvector.asyncpg.register_vector(connection)
        logger.debug("Registered pgvector support on asyncpg connection")
    except (ValueError, TypeError) as exc:
        message = str(exc).lower()
        if _is_missing_vector_error(exc) or ("vector" in message and "unknown type" in message):
            logger.debug("Skipping pgvector registration because extension is unavailable")
            return
        logger.exception("Failed to register pgvector support")
    except Exception:
        logger.exception("Failed to register pgvector support")
