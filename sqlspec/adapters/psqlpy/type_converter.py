"""PostgreSQL-specific helpers for the psqlpy adapter.

This module preserves the ``register_pgvector`` placeholder used by the
driver configuration layer.
"""

from typing import TYPE_CHECKING

from sqlspec.typing import PGVECTOR_INSTALLED

if TYPE_CHECKING:
    from psqlpy import Connection

__all__ = ("register_pgvector",)


def register_pgvector(connection: "Connection") -> None:
    """Register pgvector type handlers on psqlpy connection.

    Currently a placeholder for future implementation. The psqlpy library
    does not yet expose a type handler registration API compatible with
    pgvector's automatic conversion system.

    Args:
        connection: Psqlpy connection instance.
    """
    if not PGVECTOR_INSTALLED:
        return
