"""Fixture loading utilities for SQLSpec.

Provides functions for writing, loading and parsing JSON fixture files
used in testing and development. Supports both sync and async operations.
"""

import gzip
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlspec.storage import storage_registry
from sqlspec.utils.serializers import from_json as decode_json
from sqlspec.utils.serializers import schema_dump
from sqlspec.utils.serializers import to_json as encode_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.typing import SupportedSchemaModel

__all__ = ("open_fixture", "open_fixture_async", "write_fixture", "write_fixture_async")


def _read_text_sync(path: "Path") -> str:
    return path.read_text(encoding="utf-8")


def _compress_text(content: str) -> bytes:
    return gzip.compress(content.encode("utf-8"))


def _read_compressed_file(file_path: Path) -> str:
    """Read and decompress a file based on its extension.

    Args:
        file_path: Path to the file to read

    Returns:
        The decompressed file content as a string

    Raises:
        ValueError: If the file format is not supported
    """
    if file_path.suffix == ".gz":
        with gzip.open(file_path, mode="rt", encoding="utf-8") as f:
            return f.read()
    elif file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path, "r") as zf:
            json_name = file_path.stem + ".json"
            if json_name in zf.namelist():
                with zf.open(json_name) as f:
                    return f.read().decode("utf-8")
            json_files = [name for name in zf.namelist() if name.endswith(".json")]
            if json_files:
                with zf.open(json_files[0]) as f:
                    return f.read().decode("utf-8")
            msg = f"No JSON file found in ZIP archive: {file_path}"
            raise ValueError(msg)
    else:
        msg = f"Unsupported compression format: {file_path.suffix}"
        raise ValueError(msg)


def _find_fixture_file(fixtures_path: Any, fixture_name: str) -> Path:
    """Find a fixture file with various extensions.

    Args:
        fixtures_path: The path to look for fixtures
        fixture_name: The fixture name to load

    Returns:
        Path to the found fixture file

    Raises:
        FileNotFoundError: If no fixture file is found
    """
    base_path = Path(fixtures_path)

    for extension in [".json", ".json.gz", ".json.zip"]:
        fixture_path = base_path / f"{fixture_name}{extension}"
        if fixture_path.exists():
            return fixture_path

    msg = f"Could not find the {fixture_name} fixture"
    raise FileNotFoundError(msg)


def open_fixture(fixtures_path: Any, fixture_name: str) -> Any:
    """Load and parse a JSON fixture file with compression support.

    Supports reading from:
    - Regular JSON files (.json)
    - Gzipped JSON files (.json.gz)
    - Zipped JSON files (.json.zip)

    Args:
        fixtures_path: The path to look for fixtures (pathlib.Path)
        fixture_name: The fixture name to load.


    Returns:
        The parsed JSON data
    """
    fixture_path = _find_fixture_file(fixtures_path, fixture_name)

    if fixture_path.suffix in {".gz", ".zip"}:
        f_data = _read_compressed_file(fixture_path)
    else:
        with fixture_path.open(mode="r", encoding="utf-8") as f:
            f_data = f.read()

    return decode_json(f_data)


async def open_fixture_async(fixtures_path: Any, fixture_name: str) -> Any:
    """Load and parse a JSON fixture file asynchronously with compression support.

    Supports reading from:
    - Regular JSON files (.json)
    - Gzipped JSON files (.json.gz)
    - Zipped JSON files (.json.zip)

    For compressed files, uses sync reading in a thread pool since gzip and zipfile
    don't have native async equivalents.

    Args:
        fixtures_path: The path to look for fixtures (pathlib.Path)
        fixture_name: The fixture name to load.


    Returns:
        The parsed JSON data
    """
    fixture_path = _find_fixture_file(fixtures_path, fixture_name)

    if fixture_path.suffix in {".gz", ".zip"}:
        read_func = async_(_read_compressed_file)
        f_data = await read_func(fixture_path)
    else:
        async_read = async_(_read_text_sync)
        f_data = await async_read(fixture_path)

    return decode_json(f_data)


def _serialize_data(data: Any) -> str:
    """Serialize data to JSON string, handling different input types.

    Args:
        data: Data to serialize. Can be dict, list, or SQLSpec model types

    Returns:
        JSON string representation of the data
    """
    if isinstance(data, (list, tuple)):
        serialized_items: list[Any] = []

        for item in data:
            if isinstance(item, (str, int, float, bool, type(None))):
                serialized_items.append(item)
            else:
                serialized_items.append(schema_dump(item))

        return encode_json(serialized_items)
    if isinstance(data, (str, int, float, bool, type(None))):
        return encode_json(data)

    return encode_json(schema_dump(data))


def write_fixture(
    fixtures_path: str,
    table_name: str,
    data: "list[SupportedSchemaModel] | list[dict[str, Any]] | SupportedSchemaModel",
    storage_backend: str = "local",
    compress: bool = False,
    **storage_kwargs: Any,
) -> None:
    """Write fixture data to storage using SQLSpec storage backend.

    Args:
        fixtures_path: Base path where fixtures should be stored
        table_name: Name of the table/fixture (used as filename)
        data: Data to write - can be list of dicts, models, or single model
        storage_backend: Storage backend to use (default: "local")
        compress: Whether to gzip compress the output
        **storage_kwargs: Additional arguments for the storage backend

    Raises:
        ValueError: If storage backend is not found
    """
    if storage_backend == "local":
        uri = "file://"
        storage_kwargs["base_path"] = str(Path(fixtures_path).resolve())
    else:
        uri = storage_backend

    try:
        storage = storage_registry.get(uri, **storage_kwargs)
    except Exception as exc:
        msg = f"Failed to get storage backend for '{storage_backend}': {exc}"
        raise ValueError(msg) from exc

    json_content = _serialize_data(data)

    if compress:
        file_path = f"{table_name}.json.gz"
        content = gzip.compress(json_content.encode("utf-8"))
        storage.write_bytes(file_path, content)
    else:
        file_path = f"{table_name}.json"
        storage.write_text(file_path, json_content)


async def write_fixture_async(
    fixtures_path: str,
    table_name: str,
    data: "list[SupportedSchemaModel] | list[dict[str, Any]] | SupportedSchemaModel",
    storage_backend: str = "local",
    compress: bool = False,
    **storage_kwargs: Any,
) -> None:
    """Write fixture data to storage using SQLSpec storage backend asynchronously.

    Args:
        fixtures_path: Base path where fixtures should be stored
        table_name: Name of the table/fixture (used as filename)
        data: Data to write - can be list of dicts, models, or single model
        storage_backend: Storage backend to use (default: "local")
        compress: Whether to gzip compress the output
        **storage_kwargs: Additional arguments for the storage backend

    Raises:
        ValueError: If storage backend is not found
    """
    if storage_backend == "local":
        uri = "file://"
        storage_kwargs["base_path"] = str(Path(fixtures_path).resolve())
    else:
        uri = storage_backend

    try:
        storage = storage_registry.get(uri, **storage_kwargs)
    except Exception as exc:
        msg = f"Failed to get storage backend for '{storage_backend}': {exc}"
        raise ValueError(msg) from exc

    serialize_func = async_(_serialize_data)
    json_content = await serialize_func(data)

    if compress:
        file_path = f"{table_name}.json.gz"
        compress_func = async_(_compress_text)
        content = await compress_func(json_content)
        await storage.write_bytes_async(file_path, content)
    else:
        file_path = f"{table_name}.json"
        await storage.write_text_async(file_path, json_content)
