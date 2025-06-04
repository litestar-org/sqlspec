from typing import Any, Optional

from sqlspec._serialization import decode_json
from sqlspec.exceptions import MissingDependencyError

__all__ = ("open_fixture", "open_fixture_async")

# TODO: determine if this is needed
# Define Path and AsyncPath as None for test patching
Path: "Optional[type]" = None  # pyright: ignore  #TODO: for test patching
AsyncPath: "Optional[type]" = None  # pyright: ignore  #TODO: for test patching


def open_fixture(fixtures_path: Any, fixture_name: str) -> Any:
    """Loads JSON file with the specified fixture name

    Args:
        fixtures_path: The path to look for fixtures (pathlib.Path or anyio.Path)
        fixture_name (str): The fixture name to load.

    Raises:
        FileNotFoundError: Fixtures not found.

    Returns:
        Any: The parsed JSON data
    """
    from pathlib import Path

    fixture = Path(fixtures_path / f"{fixture_name}.json")
    if fixture.exists():
        with fixture.open(mode="r", encoding="utf-8") as f:
            f_data = f.read()
        return decode_json(f_data)
    msg = f"Could not find the {fixture_name} fixture"
    raise FileNotFoundError(msg)


async def open_fixture_async(fixtures_path: Any, fixture_name: str) -> Any:
    """Loads JSON file with the specified fixture name

    Args:
        fixtures_path: The path to look for fixtures (pathlib.Path or anyio.Path)
        fixture_name (str): The fixture name to load.

    Raises:
        FileNotFoundError: Fixtures not found.
        MissingDependencyError: The `anyio` library is required to use this function.

    Returns:
        Any: The parsed JSON data
    """
    try:
        from anyio import Path as AsyncPath
    except ImportError as exc:
        raise MissingDependencyError(package="anyio") from exc

    fixture = AsyncPath(fixtures_path / f"{fixture_name}.json")
    if await fixture.exists():
        async with await fixture.open(mode="r", encoding="utf-8") as f:
            f_data = await f.read()
        return decode_json(f_data)
    msg = f"Could not find the {fixture_name} fixture"
    raise FileNotFoundError(msg)
