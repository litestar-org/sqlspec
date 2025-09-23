"""JSON serialization utilities for SQLSpec.

Re-exports common JSON encoding and decoding functions from the core
serialization module for convenient access.
"""

from typing import Any, Literal, Union, overload

from sqlspec._serialization import decode_json, encode_json


@overload
def to_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...


@overload
def to_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...


def to_json(data: Any, *, as_bytes: bool = False) -> Union[str, bytes]:
    """Encode data to JSON string or bytes.

    Args:
        data: Data to encode.
        as_bytes: Whether to return bytes instead of string for optimal performance.

    Returns:
        JSON string or bytes representation based on as_bytes parameter.
    """
    if as_bytes:
        return encode_json(data, as_bytes=True)
    return encode_json(data, as_bytes=False)


@overload
def from_json(data: str) -> Any: ...


@overload
def from_json(data: bytes, *, decode_bytes: bool = ...) -> Any: ...


def from_json(data: Union[str, bytes], *, decode_bytes: bool = True) -> Any:
    """Decode JSON string or bytes to Python object.

    Args:
        data: JSON string or bytes to decode.
        decode_bytes: Whether to decode bytes input (vs passing through).

    Returns:
        Decoded Python object.
    """
    if isinstance(data, bytes):
        return decode_json(data, decode_bytes=decode_bytes)
    return decode_json(data)


__all__ = ("from_json", "to_json")
