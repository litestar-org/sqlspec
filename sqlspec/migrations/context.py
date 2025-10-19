"""Migration context for passing runtime information to migrations."""

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

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

    config: "Any | None" = None
    """Database configuration object."""
    dialect: "str | None" = None
    """Database dialect (e.g., 'postgres', 'mysql', 'sqlite')."""
    metadata: "dict[str, Any] | None" = None
    """Additional metadata for the migration."""
    extension_config: "dict[str, Any] | None" = None
    """Extension-specific configuration options."""

    driver: "SyncDriverAdapterBase | AsyncDriverAdapterBase | None" = None
    """Database driver instance (available during execution)."""

    _execution_metadata: "dict[str, Any]" = field(default_factory=dict)
    """Internal execution metadata for tracking async operations."""

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

    @property
    def is_async_execution(self) -> bool:
        """Check if migrations are running in an async execution context.

        Returns:
            True if executing in an async context.
        """
        try:
            asyncio.current_task()
        except RuntimeError:
            return False
        else:
            return True

    @property
    def is_async_driver(self) -> bool:
        """Check if the current driver is async.

        Returns:
            True if driver supports async operations.
        """
        if self.driver is None:
            return False

        execute_method = getattr(self.driver, "execute_script", None)
        return execute_method is not None and inspect.iscoroutinefunction(execute_method)

    @property
    def execution_mode(self) -> str:
        """Get the current execution mode.

        Returns:
            'async' if in async context, 'sync' otherwise.
        """
        return "async" if self.is_async_execution else "sync"

    def set_execution_metadata(self, key: str, value: Any) -> None:
        """Set execution metadata for tracking migration state.

        Args:
            key: Metadata key.
            value: Metadata value.
        """
        self._execution_metadata[key] = value

    def get_execution_metadata(self, key: str, default: Any = None) -> Any:
        """Get execution metadata.

        Args:
            key: Metadata key.
            default: Default value if key not found.

        Returns:
            Metadata value or default.
        """
        return self._execution_metadata.get(key, default)

    def validate_async_usage(self, migration_func: Any) -> None:
        """Validate proper usage of async functions in migration context.

        Args:
            migration_func: The migration function to validate.
        """
        if inspect.iscoroutinefunction(migration_func) and not self.is_async_execution and not self.is_async_driver:
            msg = (
                "Async migration function detected but execution context is sync. "
                "Consider using async database configuration or sync migration functions."
            )
            logger.warning(msg)

        if not inspect.iscoroutinefunction(migration_func) and self.is_async_driver:
            self.set_execution_metadata("mixed_execution", value=True)
            logger.debug("Sync migration function in async driver context - using compatibility mode")
