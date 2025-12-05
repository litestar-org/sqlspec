"""Config discovery for SQLSpec CLI.

Provides pyproject.toml config discovery for CLI convenience.
Environment variable support is handled natively by Click via envvar parameter.
"""

import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


__all__ = ("discover_config_from_pyproject", "find_pyproject_toml", "parse_pyproject_config")


def discover_config_from_pyproject() -> str | None:
    """Find and parse pyproject.toml for SQLSpec config.

    Walks filesystem upward from current directory to find pyproject.toml.
    Parses [tool.sqlspec] section for 'config' key.

    Returns:
        Config path(s) as string (comma-separated if list), or None if not found.
    """
    pyproject_path = find_pyproject_toml()
    if pyproject_path is None:
        return None

    return parse_pyproject_config(pyproject_path)


def find_pyproject_toml() -> "Path | None":
    """Walk filesystem upward to find pyproject.toml.

    Starts from current working directory and walks up to filesystem root.
    Stops at .git directory boundary (repository root) if found.

    Returns:
        Path to pyproject.toml, or None if not found.
    """
    current = Path.cwd()

    while True:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject

        # Stop at .git boundary (repository root)
        if (current / ".git").exists():
            return None

        # Stop at filesystem root
        if current == current.parent:
            return None

        current = current.parent


def parse_pyproject_config(pyproject_path: "Path") -> str | None:
    """Parse pyproject.toml for [tool.sqlspec] config.

    Args:
        pyproject_path: Path to pyproject.toml file.

    Returns:
        Config path(s) as string (converts list to comma-separated), or None if not found.

    Raises:
        ValueError: If [tool.sqlspec].config has invalid type (not str or list[str]).
    """
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        msg = f"Failed to parse {pyproject_path}: {e}"
        raise ValueError(msg) from e

    # Navigate to [tool.sqlspec] section
    tool_section = data.get("tool", {})
    if not isinstance(tool_section, dict):
        return None

    sqlspec_section = tool_section.get("sqlspec", {})
    if not isinstance(sqlspec_section, dict):
        return None

    # Extract config value
    config = sqlspec_section.get("config")
    if config is None:
        return None

    # Handle string config
    if isinstance(config, str):
        return config

    # Handle list config (convert to comma-separated)
    if isinstance(config, list):
        if not all(isinstance(item, str) for item in config):
            msg = f"Invalid [tool.sqlspec].config in {pyproject_path}: list items must be strings"
            raise ValueError(msg)
        return ",".join(config)

    # Invalid type
    msg = f"Invalid [tool.sqlspec].config in {pyproject_path}: must be string or list of strings, got {type(config).__name__}"
    raise ValueError(msg)
