"""General utility functions."""

import importlib
from importlib.util import find_spec
from pathlib import Path
from typing import Any

__all__ = (
    "import_string",
    "module_to_os_path",
)


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

    Raises:
        ImportError: Could not import the module.

    Returns:
        object: The imported object.
    """
    try:
        parts = dotted_path.split(".")
        for i in range(len(parts), 0, -1):
            module_path = ".".join(parts[:i])
            try:
                module = importlib.import_module(module_path)
                break
            except ModuleNotFoundError:
                continue
        else:
            msg = f"{dotted_path} doesn't look like a module path"
            raise ImportError(msg)
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
                msg = f"Module '{parent_module_path}' has no attribute '{attr}' in '{dotted_path}'"
                raise ImportError(msg)
        for attr in attrs:
            try:
                obj = getattr(obj, attr)
            except AttributeError as e:
                msg = f"Module '{module.__name__}' has no attribute '{attr}' in '{dotted_path}'"
                raise ImportError(msg) from e
        return obj
    except Exception as e:
        msg = f"Could not import '{dotted_path}': {e}"
        raise ImportError(msg) from e
