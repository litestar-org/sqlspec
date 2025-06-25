import datetime
import enum
from typing import Any

from sqlspec.typing import PYDANTIC_INSTALLED, BaseModel


def _type_to_string(value: Any) -> str:  # pragma: no cover
    if isinstance(value, datetime.datetime):
        return convert_datetime_to_gmt_iso(value)
    if isinstance(value, datetime.date):
        return convert_date_to_iso(value)
    if isinstance(value, enum.Enum):
        return str(value.value)
    if PYDANTIC_INSTALLED and isinstance(value, BaseModel):
        return value.model_dump_json()
    try:
        val = str(value)
    except Exception as exc:
        raise TypeError from exc
    return val


try:
    from msgspec.json import Decoder, Encoder

    encoder, decoder = Encoder(enc_hook=_type_to_string), Decoder()
    decode_json = decoder.decode

    def encode_json(data: Any) -> str:  # pragma: no cover
        return encoder.encode(data).decode("utf-8")

except ImportError:
    try:
        from orjson import (  # pyright: ignore[reportMissingImports]
            OPT_NAIVE_UTC,  # pyright: ignore[reportUnknownVariableType]
            OPT_SERIALIZE_NUMPY,  # pyright: ignore[reportUnknownVariableType]
            OPT_SERIALIZE_UUID,  # pyright: ignore[reportUnknownVariableType]
        )
        from orjson import dumps as _encode_json  # pyright: ignore[reportUnknownVariableType,reportMissingImports]
        from orjson import loads as decode_json  # type: ignore[no-redef,assignment,unused-ignore]

        def encode_json(data: Any) -> str:  # pragma: no cover
            return _encode_json(
                data, default=_type_to_string, option=OPT_SERIALIZE_NUMPY | OPT_NAIVE_UTC | OPT_SERIALIZE_UUID
            ).decode("utf-8")

    except ImportError:
        from json import dumps as encode_json  # type: ignore[assignment]
        from json import loads as decode_json  # type: ignore[assignment]

__all__ = ("convert_date_to_iso", "convert_datetime_to_gmt_iso", "decode_json", "encode_json")


def convert_datetime_to_gmt_iso(dt: datetime.datetime) -> str:  # pragma: no cover
    """Handle datetime serialization for nested timestamps.

    Args:
        dt: The datetime to convert.

    Returns:
        The ISO formatted datetime string.
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def convert_date_to_iso(dt: datetime.date) -> str:  # pragma: no cover
    """Handle datetime serialization for nested timestamps.

    Args:
        dt: The date to convert.

    Returns:
        The ISO formatted date string.
    """
    return dt.isoformat()
