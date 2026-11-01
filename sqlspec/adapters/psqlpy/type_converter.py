"""PostgreSQL-specific helpers for the psqlpy adapter."""

from typing import Any

from sqlspec.typing import import_optional_attr

__all__ = ("coerce_pgvector",)

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
