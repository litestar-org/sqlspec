"""Adapter name and extension store resolution utilities."""

import importlib
from typing import Any

__all__ = ("resolve_adapter_name", "resolve_extension_store_class")

_ADAPTERS_MODULE_PREFIX = "sqlspec.adapters."
_MIN_ADAPTER_MODULE_PARTS = 3


def resolve_adapter_name(config_or_store: object) -> "str | None":
    """Resolve the adapter identifier from a configuration or store class/instance.

    Resolution order:
        1. Explicit ``adapter_name`` attribute declared on a class in ``__mro__``.
        2. First class in ``__mro__`` whose module path starts with ``sqlspec.adapters.<name>``.
        3. Bound ``_config`` attribute when ``config_or_store`` is a store instance.

    Args:
        config_or_store: Database configuration or extension store class or instance.

    Returns:
        Adapter identifier string (for example ``"sqlite"`` or ``"spanner"``),
        or ``None`` when no adapter can be determined.
    """
    if config_or_store is None:
        return None
    target_cls = config_or_store if isinstance(config_or_store, type) else type(config_or_store)
    resolved = _resolve_adapter_name_from_mro(target_cls)
    if resolved is not None:
        return resolved
    if not isinstance(config_or_store, type):
        inner_config = _extract_bound_config(config_or_store, target_cls)
        if inner_config is not None and inner_config is not config_or_store:
            return resolve_adapter_name(inner_config)
    return None


def resolve_extension_store_class(
    config_or_cls: object, *, extension: str, store_attr: str, store_suffix: str
) -> "type[Any] | None":
    """Resolve the extension store class for a database configuration.

    Resolution order:
        1. Explicit store class attribute (such as ``events_store_class``,
           ``adk_store_class``, ``adk_memory_store_class``, or
           ``litestar_store_class``) on the configuration instance or class.
        2. Built-in store class exported by
           ``sqlspec.adapters.<adapter_name>.<extension>.store`` matching the
           configuration class name, MRO adapter base config name, or
           sync/async execution mode.

    Args:
        config_or_cls: Database configuration instance or class.
        extension: Extension package name (``"events"``, ``"adk"``, or ``"litestar"``).
        store_attr: Configuration attribute holding an explicit store class override.
        store_suffix: Store class suffix (``"EventQueueStore"``, ``"ADKStore"``,
            ``"ADKMemoryStore"``, or ``"Store"``).

    Returns:
        Resolved store class, or ``None`` when no store class is available.
    """
    if config_or_cls is None:
        return None
    target_cls = config_or_cls if isinstance(config_or_cls, type) else type(config_or_cls)
    explicit_store = None if isinstance(config_or_cls, type) else getattr(config_or_cls, store_attr, None)
    if not isinstance(explicit_store, type):
        explicit_store = getattr(target_cls, store_attr, None)
    if isinstance(explicit_store, type):
        return explicit_store

    adapter_name = resolve_adapter_name(config_or_cls)
    if adapter_name is None:
        return None

    store_module = _import_extension_store_module(adapter_name, extension)
    if store_module is None:
        return None

    return _match_store_class_in_module(store_module, target_cls, config_or_cls, adapter_name, store_suffix)


def _resolve_adapter_name_from_mro(target_cls: "type[Any]") -> "str | None":
    for cls in target_cls.__mro__:
        explicit = cls.__dict__.get("adapter_name")
        if isinstance(explicit, str):
            stripped = explicit.strip()
            if stripped:
                return stripped
        module_name = cls.__module__
        if module_name.startswith(_ADAPTERS_MODULE_PREFIX):
            parts = module_name.split(".")
            if len(parts) >= _MIN_ADAPTER_MODULE_PARTS and parts[2]:
                return parts[2]
    return None


def _import_extension_store_module(adapter_name: str, extension: str) -> "Any | None":
    module_name = f"sqlspec.adapters.{adapter_name}.{extension}.store"
    adapter_prefix = f"sqlspec.adapters.{adapter_name}"
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing_name = exc.name or ""
        if missing_name == "sqlspec.adapters" or missing_name.startswith(adapter_prefix):
            return None
        raise


def _candidate_config_names(target_cls: "type[Any]", adapter_name: str) -> "list[str]":
    names: list[str] = [target_cls.__name__]
    adapter_module_prefix = f"sqlspec.adapters.{adapter_name}."
    for cls in target_cls.__mro__:
        cls_name = cls.__name__
        if (
            cls.__module__.startswith(adapter_module_prefix)
            and cls_name.endswith("Config")
            and cls_name not in names
        ):
            names.append(cls_name)
    return names


def _exported_store_names(store_module: Any, store_suffix: str) -> "tuple[str, ...]":
    exported = getattr(store_module, "__all__", ())
    matched: list[str] = []
    for name in exported:
        if not isinstance(name, str) or not name.endswith(store_suffix):
            continue
        if store_suffix == "ADKStore" and name.endswith("ADKMemoryStore"):
            continue
        candidate = getattr(store_module, name, None)
        if isinstance(candidate, type) and not name.endswith("Config"):
            matched.append(name)
    return tuple(matched)


def _match_store_class_in_module(
    store_module: Any,
    target_cls: "type[Any]",
    config_or_cls: object,
    adapter_name: str,
    store_suffix: str,
) -> "type[Any] | None":
    candidate_names = _candidate_config_names(target_cls, adapter_name)
    exported_names = _exported_store_names(store_module, store_suffix)

    for config_name in candidate_names:
        if config_name.endswith("Config"):
            direct_name = f"{config_name[:-6]}{store_suffix}"
            direct_attr = getattr(store_module, direct_name, None)
            if isinstance(direct_attr, type):
                return direct_attr
        expected_prefix = config_name.removesuffix("Config").lower()
        for exported_name in exported_names:
            prefix = exported_name[: -len(store_suffix)].lower()
            if prefix == expected_prefix:
                resolved = getattr(store_module, exported_name, None)
                if isinstance(resolved, type):
                    return resolved

    if len(exported_names) == 1:
        single = getattr(store_module, exported_names[0], None)
        if isinstance(single, type):
            return single

    if len(exported_names) > 1:
        is_async = bool(getattr(config_or_cls, "is_async", False))
        mode_suffix = f"Async{store_suffix}" if is_async else f"Sync{store_suffix}"
        for exported_name in exported_names:
            if exported_name.endswith(mode_suffix):
                resolved = getattr(store_module, exported_name, None)
                if isinstance(resolved, type):
                    return resolved

    return None


def _extract_bound_config(config_or_store: object, target_cls: "type[Any]") -> "object | None":
    """Extract bound inner configuration from a store instance while avoiding mock recursion."""
    if hasattr(config_or_store, "_mock_return_value"):
        if "_config" in getattr(config_or_store, "__dict__", {}):
            return getattr(config_or_store, "_config", None)
        return None
    has_slot = any("_config" in getattr(cls, "__slots__", ()) for cls in target_cls.__mro__)
    has_attr = (
        "_config" in getattr(config_or_store, "__dict__", {})
        or has_slot
        or isinstance(getattr(target_cls, "_config", None), property)
    )
    if not has_attr:
        return None
    return getattr(config_or_store, "_config", None)
