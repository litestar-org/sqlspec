"""PostgreSQL-specific helpers for the psqlpy adapter.

This module preserves the ``register_pgvector`` placeholder used by the
driver configuration layer.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.typing import PGVECTOR_INSTALLED, import_optional_attr

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection as Connection

__all__ = ("coerce_pgvector", "register_pgvector")

_PGVECTOR_TYPE = import_optional_attr("psqlpy.extra_types", "PgVector")


def coerce_pgvector(value: Any) -> Any:
    """Wrap supported dense vector values using psqlpy's native vector encoder."""
    if value is None or _PGVECTOR_TYPE is None or isinstance(value, _PGVECTOR_TYPE):
        return value
    if isinstance(value, (list, tuple)):
        return _PGVECTOR_TYPE(list(value))
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _PGVECTOR_TYPE(tolist())
    return value


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
