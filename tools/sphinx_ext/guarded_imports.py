"""Sphinx logging filters for optional dependency guarded imports."""

from __future__ import annotations

from logging import Filter, LogRecord, getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sphinx.application import Sphinx

_AUTODOC_TYPEHINTS_LOGGER = "sphinx.sphinx_autodoc_typehints"


class AutodocTypehintsGuardedImportFilter(Filter):
    """Suppress known optional-driver annotations unavailable at runtime."""

    def filter(self, record: LogRecord) -> bool:
        message = record.getMessage()
        if "Failed guarded type import" in message:
            return not (
                ("QueryParams" in message and "pymssql._pymssql" in message)
                or ("oracledb.__version__" in message and "str object expected" in message)
            )
        if "Cannot resolve forward reference" in message:
            return not (
                "sqlspec.adapters.oracledb.data_dictionary.Oracledb" in message
                and any(
                    type_name in message for type_name in ("DialectConfig", "OracleAsyncDriver", "OracleSyncDriver")
                )
            )
        return True


def setup(app: Sphinx) -> dict[str, bool]:
    getLogger(_AUTODOC_TYPEHINTS_LOGGER).addFilter(AutodocTypehintsGuardedImportFilter())
    return {"parallel_read_safe": True, "parallel_write_safe": True}
