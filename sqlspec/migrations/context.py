"""Migration context for passing runtime information to migrations."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from typing_extensions import TypeGuard

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

logger = get_logger("migrations.context")

__all__ = ("MigrationContext", "_has_create_statement_config", "_has_statement_config")


def _has_statement_config(config: Any) -> "TypeGuard[Any]":
    """Check if config has statement_config attribute.

    Args:
        config: Configuration object to check.

    Returns:
        True if config has statement_config attribute, False otherwise.
    """
    try:
        _ = config.statement_config
    except AttributeError:
        return False
    else:
        return True


def _has_create_statement_config(config: Any) -> "TypeGuard[Any]":
    """Check if config has _create_statement_config method.

    Args:
        config: Configuration object to check.

    Returns:
        True if config has _create_statement_config method, False otherwise.
    """
    try:
        _ = config._create_statement_config
    except AttributeError:
        return False
    else:
        return callable(config._create_statement_config)


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
        if _has_statement_config(config) and config.statement_config:
            try:
                dialect = config.statement_config.dialect
            except AttributeError:
                logger.debug("Statement config has no dialect attribute")
        elif _has_create_statement_config(config):
            try:
                stmt_config = config._create_statement_config()
                try:
                    dialect = stmt_config.dialect
                except AttributeError:
                    logger.debug("Created statement config has no dialect attribute")
            except Exception:
                logger.debug("Unable to get dialect from statement config")

        return cls(dialect=dialect, config=config)
