"""Migration context for passing runtime information to migrations."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

logger = get_logger("migrations.context")

__all__ = ("MigrationContext",)


@dataclass
class MigrationContext:
    """Context object passed to migration functions.

    Provides runtime information about the database environment
    to migration functions, allowing them to generate dialect-specific SQL.
    """

    config: "Optional[Any]" = None
    """Database configuration object."""
    dialect: "Optional[str]" = None
    """Database dialect (e.g., 'postgres', 'mysql', 'sqlite')."""
    metadata: "Optional[dict[str, Any]]" = None
    """Additional metadata for the migration."""
    extension_config: "Optional[dict[str, Any]]" = None
    """Extension-specific configuration options."""

    driver: "Optional[Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]]" = None
    """Database driver instance (available during execution)."""

    def __post_init__(self) -> None:
        """Initialize metadata and extension config if not provided."""
        if not self.metadata:
            self.metadata = {}
        if not self.extension_config:
            self.extension_config = {}

    @classmethod
    def from_config(cls, config: Any) -> "MigrationContext":
        """Create context from database configuration.

        Args:
            config: Database configuration object.

        Returns:
            Migration context with dialect information.
        """
        dialect = None
        try:
            if hasattr(config, "statement_config") and config.statement_config:
                dialect = getattr(config.statement_config, "dialect", None)
            elif hasattr(config, "_create_statement_config") and callable(config._create_statement_config):
                stmt_config = config._create_statement_config()
                dialect = getattr(stmt_config, "dialect", None)
        except Exception:
            logger.debug("Unable to extract dialect from config")

        return cls(dialect=dialect, config=config)
