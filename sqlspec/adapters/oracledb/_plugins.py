"""Lazy Oracle cloud plugins and idempotent SDK hook registration."""

import contextlib
import importlib
import threading
from collections.abc import Callable, Sequence
from types import ModuleType
from typing import Any, Final, Literal, TypeAlias, cast

from sqlspec.adapters.oracledb._typing import oracledb_module as oracledb
from sqlspec.adapters.oracledb.core import normalize_oracle_config_dsn
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError

__all__ = (
    "OraclePluginName",
    "ensure_oracle_config_plugins",
    "ensure_oracle_plugin",
    "refresh_oracle_config_cache",
    "register_oracle_params_hook",
    "register_oracle_protocol",
    "resolve_oracle_dsn_plugin",
    "unregister_oracle_params_hook",
)


OraclePluginName: TypeAlias = Literal[
    "oci_config_provider",
    "aws_config_provider",
    "azure_config_provider",
    "gcp_config_provider",
    "oci_tokens",
    "azure_tokens",
]
ORACLE_CONFIG_URI_PLUGIN_MAP: Final[dict[str, OraclePluginName | None]] = {
    "config-file": None,
    "config-ociobject": "oci_config_provider",
    "config-azure": "azure_config_provider",
    "config-awss3": "aws_config_provider",
    "config-awssecretsmanager": "aws_config_provider",
    "config-gcpstorage": "gcp_config_provider",
    "config-gcpsecretmanager": "gcp_config_provider",
}
ORACLE_PLUGIN_INSTALL_HINTS: Final[dict[str, tuple[str, str]]] = {
    "oci_config_provider": ("oci", "oci"),
    "aws_config_provider": ("boto3", "boto3"),
    "azure_config_provider": (
        "azure-appconfiguration",
        "azure-appconfiguration azure-keyvault-secrets azure-identity azure-core",
    ),
    "gcp_config_provider": (
        "google-cloud-storage",
        "google-cloud-storage google-cloud-secret-manager google-cloud-pubsub google-crc32c",
    ),
    "oci_tokens": ("oci", "oci cryptography requests"),
    "azure_tokens": ("msal", "msal"),
}
VALID_ORACLE_PLUGIN_NAMES: Final[frozenset[str]] = frozenset(ORACLE_PLUGIN_INSTALL_HINTS)
_ORACLE_HOOKS_LOCK: Final[threading.RLock] = threading.RLock()


def resolve_oracle_dsn_plugin(dsn: str | None) -> OraclePluginName | None:
    """Resolve a built-in provider without importing its optional cloud SDK."""
    if dsn is None:
        return None
    return ORACLE_CONFIG_URI_PLUGIN_MAP.get(normalize_oracle_config_dsn(dsn).partition("://")[0])


def ensure_oracle_plugin(plugin_name: str) -> ModuleType:
    """Load a named native Oracle plugin with actionable dependency errors."""
    if plugin_name not in VALID_ORACLE_PLUGIN_NAMES:
        msg = f"Unknown Oracle plugin: {plugin_name!r}"
        raise ImproperConfigurationError(msg)
    try:
        return importlib.import_module(f"oracledb.plugins.{plugin_name}")
    except ImportError as exc:
        package, install = ORACLE_PLUGIN_INSTALL_HINTS[plugin_name]
        raise MissingDependencyError(package, install_package=install) from exc


def ensure_oracle_config_plugins(
    *,
    dsn: str | None = None,
    plugins: Sequence[str] | None = None,
    params_hooks: Sequence[Callable[[Any], None]] | None = None,
) -> None:
    """Initialize required provider plugins and validated parameter hooks."""
    if isinstance(plugins, (str, bytes)) or isinstance(params_hooks, (str, bytes)):
        msg = "Oracle plugins and params_hooks must be sequences, not strings"
        raise ImproperConfigurationError(msg)
    provider = resolve_oracle_dsn_plugin(dsn)
    names = list(plugins or ())
    if provider is not None:
        names.insert(0, provider)
    for name in dict.fromkeys(names):
        ensure_oracle_plugin(name)
    for hook in params_hooks or ():
        register_oracle_params_hook(hook)


def register_oracle_protocol(protocol: str, hook_function: Callable[..., None] | None) -> None:
    """Register a protocol or idempotently remove its native SDK handler."""
    if (
        not isinstance(protocol, str)
        or not protocol.strip()
        or (hook_function is not None and not callable(hook_function))
    ):
        msg = "Oracle protocol requires a nonempty name and callable hook"
        raise ImproperConfigurationError(msg)
    with _ORACLE_HOOKS_LOCK, contextlib.suppress(KeyError):
        oracledb.register_protocol(protocol, cast("Callable[..., Any]", hook_function))


def register_oracle_params_hook(hook_function: Callable[[Any], None]) -> None:
    """Register a callable once, including handlers registered through the SDK."""
    if not callable(hook_function):
        msg = "Oracle params hook must be callable"
        raise ImproperConfigurationError(msg)
    with _ORACLE_HOOKS_LOCK:
        registry = importlib.import_module("oracledb.base_impl").REGISTERED_PARAMS_HOOKS
        if hook_function not in registry:
            oracledb.register_params_hook(hook_function)


def unregister_oracle_params_hook(hook_function: Callable[[Any], None]) -> None:
    """Remove all registrations of a callable, tolerating repeated teardown."""
    if not callable(hook_function):
        msg = "Oracle params hook must be callable"
        raise ImproperConfigurationError(msg)
    with _ORACLE_HOOKS_LOCK:
        registry = importlib.import_module("oracledb.base_impl").REGISTERED_PARAMS_HOOKS
        while hook_function in registry:
            oracledb.unregister_params_hook(hook_function)


def refresh_oracle_config_cache(dsn: str, *, remove_permanently: bool = False) -> None:
    """Expire a normalized configuration DSN through the public native SDK."""
    oracledb.update_config_cache(normalize_oracle_config_dsn(dsn), remove_permanently=remove_permanently)
