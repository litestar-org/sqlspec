"""Sphinx logging filters for optional dependency guarded imports."""

from __future__ import annotations

from logging import Filter, LogRecord, getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sphinx.application import Sphinx

_AUTODOC_TYPEHINTS_LOGGER = "sphinx.sphinx_autodoc_typehints"


class AutodocTypehintsGuardedImportFilter(Filter):
    """Suppress known optional-driver type aliases that exist only in stubs."""

    def filter(self, record: LogRecord) -> bool:
        message = record.getMessage()
        return not (
            "Failed guarded type import" in message
            and "QueryParams" in message
            and "pymssql._pymssql" in message
        )


def setup(app: Sphinx) -> dict[str, bool]:
    getLogger(_AUTODOC_TYPEHINTS_LOGGER).addFilter(AutodocTypehintsGuardedImportFilter())
    return {"parallel_read_safe": True, "parallel_write_safe": True}
