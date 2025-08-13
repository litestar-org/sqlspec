from typing import TYPE_CHECKING, Any, Union

from litestar.di import Provide
from litestar.plugins import CLIPlugin, InitPluginProtocol

from sqlspec.base import SQLSpec as SQLSpecBase
from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar.config import DatabaseConfig
from sqlspec.typing import ConnectionT, PoolT
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from click import Group
    from litestar.config.app import AppConfig

logger = get_logger("extensions.litestar")


class SQLSpec(InitPluginProtocol, CLIPlugin, SQLSpecBase):
    """Litestar plugin for SQLSpec database integration."""

    __slots__ = ("_config", "_plugin_configs")

    def __init__(self, config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfig", list["DatabaseConfig"]]) -> None:
        """Initialize SQLSpec plugin.

        Args:
            config: Database configuration for SQLSpec plugin.
        """
        self._configs: dict[Any, DatabaseConfigProtocol[Any, Any, Any]] = {}
        if isinstance(config, DatabaseConfigProtocol):
            self._plugin_configs: list[DatabaseConfig] = [DatabaseConfig(config=config)]
        elif isinstance(config, DatabaseConfig):
            self._plugin_configs = [config]
        else:
            self._plugin_configs = config

    @property
    def config(self) -> "list[DatabaseConfig]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Return the plugin configuration.

        Returns:
            List of database configurations.
        """
        return self._plugin_configs

    def on_cli_init(self, cli: "Group") -> None:
        """Configure CLI commands for SQLSpec database operations.

        Args:
            cli: The Click command group to add commands to.
        """
        from sqlspec.extensions.litestar.cli import database_group

        cli.add_command(database_group)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Configure Litestar application with SQLSpec database integration.

        Args:
            app_config: The Litestar application configuration instance.

        Returns:
            The updated application configuration instance.
        """

        self._validate_dependency_keys()

        def store_sqlspec_in_state() -> None:
            app_config.state.sqlspec = self

        app_config.on_startup.append(store_sqlspec_in_state)
        app_config.signature_types.extend(
            [SQLSpec, ConnectionT, PoolT, DriverT, DatabaseConfig, DatabaseConfigProtocol, SyncConfigT, AsyncConfigT]
        )

        signature_namespace = {}

        for c in self._plugin_configs:
            c.annotation = self.add_config(c.config)
            app_config.signature_types.append(c.annotation)
            app_config.signature_types.append(c.config.connection_type)  # type: ignore[union-attr]
            app_config.signature_types.append(c.config.driver_type)  # type: ignore[union-attr]

            if hasattr(c.config, "get_signature_namespace"):
                signature_namespace.update(c.config.get_signature_namespace())  # type: ignore[attr-defined]

            app_config.before_send.append(c.before_send_handler)
            app_config.lifespan.append(c.lifespan_handler)  # pyright: ignore[reportUnknownMemberType]
            app_config.dependencies.update(
                {
                    c.connection_key: Provide(c.connection_provider),
                    c.pool_key: Provide(c.pool_provider),
                    c.session_key: Provide(c.session_provider),
                }
            )

        if signature_namespace:
            app_config.signature_namespace.update(signature_namespace)

        return app_config

    def get_annotations(self) -> "list[type[Union[SyncConfigT, AsyncConfigT]]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Return the list of annotations.

        Returns:
            List of annotations.
        """
        return [c.annotation for c in self.config]

    def get_annotation(
        self, key: "Union[str, SyncConfigT, AsyncConfigT, type[Union[SyncConfigT, AsyncConfigT]]]"
    ) -> "type[Union[SyncConfigT, AsyncConfigT]]":
        """Return the annotation for the given configuration.

        Args:
            key: The configuration instance or key to lookup

        Raises:
            KeyError: If no configuration is found for the given key.

        Returns:
            The annotation for the configuration.
        """
        for c in self.config:
            if key == c.config or key in {c.annotation, c.connection_key, c.pool_key}:
                return c.annotation
        msg = f"No configuration found for {key}"
        raise KeyError(msg)

    def _validate_dependency_keys(self) -> None:
        """Validate that connection and pool keys are unique across configurations.

        Raises:
            ImproperConfigurationError: If connection keys or pool keys are not unique.
        """
        connection_keys = [c.connection_key for c in self.config]
        pool_keys = [c.pool_key for c in self.config]
        if len(set(connection_keys)) != len(connection_keys):
            msg = "When using multiple database configuration, each configuration must have a unique `connection_key`."
            raise ImproperConfigurationError(detail=msg)
        if len(set(pool_keys)) != len(pool_keys):
            msg = "When using multiple database configuration, each configuration must have a unique `pool_key`."
            raise ImproperConfigurationError(detail=msg)
