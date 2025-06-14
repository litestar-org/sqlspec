"""General utility functions."""

import importlib
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Optional

__all__ = ("import_string", "module_to_os_path")


def module_to_os_path(dotted_path: str = "app") -> "Path":
    """Find Module to OS Path.

    Return a path to the base directory of the project or the module
    specified by `dotted_path`.

    Args:
        dotted_path: The path to the module. Defaults to "app".

    Raises:
        TypeError: The module could not be found.

    Returns:
        Path: The path to the module.
    """
    try:
        if (src := find_spec(dotted_path)) is None:  # pragma: no cover
            msg = f"Couldn't find the path for {dotted_path}"
            raise TypeError(msg)
    except ModuleNotFoundError as e:
        msg = f"Couldn't find the path for {dotted_path}"
        raise TypeError(msg) from e

    path = Path(str(src.origin))
    return path.parent if path.is_file() else path


def import_string(dotted_path: str) -> "Any":
    """Dotted Path Import.

    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.

    Args:
        dotted_path: The path of the module to import.

    Returns:
        object: The imported object.
    """

    def _raise_import_error(msg: str, exc: "Optional[Exception]" = None) -> None:
        if exc is not None:
            raise ImportError(msg) from exc
        raise ImportError(msg)

    obj: Any = None
    try:
        parts = dotted_path.split(".")
        module = None
        i = len(parts)  # Initialize to full length

        for i in range(len(parts), 0, -1):
            module_path = ".".join(parts[:i])
            try:
                module = importlib.import_module(module_path)
                break
            except ModuleNotFoundError:
                continue
        else:
            _raise_import_error(f"{dotted_path} doesn't look like a module path")

        if module is None:
            _raise_import_error(f"Failed to import any module from {dotted_path}")

        obj = module
        attrs = parts[i:]
        if not attrs and i == len(parts) and len(parts) > 1:
            parent_module_path = ".".join(parts[:-1])
            attr = parts[-1]
            try:
                parent_module = importlib.import_module(parent_module_path)
            except Exception:
                return obj
            if not hasattr(parent_module, attr):
                _raise_import_error(f"Module '{parent_module_path}' has no attribute '{attr}' in '{dotted_path}'")
        for attr in attrs:
            if not hasattr(obj, attr):
                _raise_import_error(
                    f"Module '{module.__name__ if module is not None else 'unknown'}' has no attribute '{attr}' in '{dotted_path}'"
                )
            obj = getattr(obj, attr)
    except Exception as e:  # pylint: disable=broad-exception-caught
        _raise_import_error(f"Could not import '{dotted_path}': {e}", e)
    return obj
