from __future__ import annotations

from typing import TYPE_CHECKING

from litestar.plugins import InitPluginProtocol

from sqlspec.base import ConfigManager

if TYPE_CHECKING:
    from litestar.config.app import AppConfig


class SQLSpecPlugin(InitPluginProtocol):
    """SQLSpec plugin."""

    __slots__ = ("_config",)

    def __init__(self, config: ConfigManager) -> None:
        """Initialize ``SQLSpecPlugin``.

        Args:
            config: configure SQLSpec plugin for use with Litestar.
        """
        self._config = config

    @property
    def config(self) -> ConfigManager:
        """Return the plugin config.

        Returns:
            ConfigManager.
        """
        return self._config

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Configure application for use with SQLSpec.

        Args:
            app_config: The :class:`AppConfig <.config.app.AppConfig>` instance.
        """
        app_config.signature_types.append(ConfigManager)
        return app_config
