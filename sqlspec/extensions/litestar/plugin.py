from typing import TYPE_CHECKING, Any, Union

from litestar.di import Provide
from litestar.plugins import InitPluginProtocol

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


class SQLSpec(InitPluginProtocol, SQLSpecBase):
    """SQLSpec plugin."""

    __slots__ = ("_config", "_plugin_configs")

    def __init__(self, config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfig", list["DatabaseConfig"]]) -> None:
        """Initialize ``SQLSpecPlugin``.

        Args:
            config: configure SQLSpec plugin for use with Litestar.
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
        """Return the plugin config.

        Returns:
            ConfigManager.
        """
        return self._plugin_configs

    def on_cli_init(self, cli: "Group") -> None:
        """Configure the CLI for use with SQLSpec."""

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Configure application for use with SQLSpec.

        Args:
            app_config: The :class:`AppConfig <.config.app.AppConfig>` instance.

        Returns:
            The updated :class:`AppConfig <.config.app.AppConfig>` instance.
        """

        self._validate_dependency_keys()

        def store_sqlspec_in_state() -> None:
            app_config.state.sqlspec = self

        app_config.on_startup.append(store_sqlspec_in_state)
        # Register types for injection
        app_config.signature_types.extend(
            [SQLSpec, ConnectionT, PoolT, DriverT, DatabaseConfig, DatabaseConfigProtocol, SyncConfigT, AsyncConfigT]
        )

        # Create signature namespace for connection types
        signature_namespace = {}

        for c in self._plugin_configs:
            c.annotation = self.add_config(c.config)
            app_config.signature_types.append(c.annotation)
            app_config.signature_types.append(c.config.connection_type)  # type: ignore[union-attr]
            app_config.signature_types.append(c.config.driver_type)  # type: ignore[union-attr]

            # Get signature namespace from the config
            if hasattr(c.config, "get_signature_namespace"):
                config_namespace = c.config.get_signature_namespace()  # type: ignore[attr-defined]
                signature_namespace.update(config_namespace)

            app_config.before_send.append(c.before_send_handler)
            app_config.lifespan.append(c.lifespan_handler)  # pyright: ignore[reportUnknownMemberType]
            app_config.dependencies.update(
                {
                    c.connection_key: Provide(c.connection_provider),
                    c.pool_key: Provide(c.pool_provider),
                    c.session_key: Provide(c.session_provider),
                }
            )

        # Update app config with signature namespace
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
        """Verify uniqueness of ``connection_key`` and ``pool_key``.

        Raises:
            ImproperConfigurationError: If session keys or pool keys are not unique.
        """
        connection_keys = [c.connection_key for c in self.config]
        pool_keys = [c.pool_key for c in self.config]
        if len(set(connection_keys)) != len(connection_keys):
            msg = "When using multiple database configuration, each configuration must have a unique `connection_key`."
            raise ImproperConfigurationError(detail=msg)
        if len(set(pool_keys)) != len(pool_keys):
            msg = "When using multiple database configuration, each configuration must have a unique `pool_key`."
            raise ImproperConfigurationError(detail=msg)
