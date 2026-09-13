"""Type coercion and serialization capabilities."""

from typing import Any, Literal

from mypy_extensions import mypyc_attr

__all__ = ("TypeCoercionCapabilities",)


@mypyc_attr(allow_interpreted_subclasses=False)
class TypeCoercionCapabilities:
    """Type coercion capabilities supported by a database adapter."""

    __slots__ = ("datetime_binding", "json_columns_decoded", "timestamp_precision", "uuid_binding")

    datetime_binding: Literal["native", "iso_text", "naive_utc"]
    timestamp_precision: Literal["microsecond", "millisecond", "second"]
    json_columns_decoded: bool
    uuid_binding: Literal["native", "text"]

    def __init__(
        self,
        datetime_binding: Literal["native", "iso_text", "naive_utc"],
        timestamp_precision: Literal["microsecond", "millisecond", "second"],
        json_columns_decoded: bool,
        uuid_binding: Literal["native", "text"],
    ) -> None:
        super().__setattr__("datetime_binding", datetime_binding)
        super().__setattr__("timestamp_precision", timestamp_precision)
        super().__setattr__("json_columns_decoded", json_columns_decoded)
        super().__setattr__("uuid_binding", uuid_binding)

    def __setattr__(self, name: str, value: Any) -> None:
        msg = f"{type(self).__name__} is immutable"
        raise AttributeError(msg)

    def __delattr__(self, name: str) -> None:
        msg = f"{type(self).__name__} is immutable"
        raise AttributeError(msg)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TypeCoercionCapabilities):
            return False
        return (
            self.datetime_binding == other.datetime_binding
            and self.timestamp_precision == other.timestamp_precision
            and self.json_columns_decoded == other.json_columns_decoded
            and self.uuid_binding == other.uuid_binding
        )

    def __hash__(self) -> int:
        return hash((self.datetime_binding, self.timestamp_precision, self.json_columns_decoded, self.uuid_binding))

    def __repr__(self) -> str:
        return (
            f"TypeCoercionCapabilities("
            f"datetime_binding={self.datetime_binding!r}, "
            f"timestamp_precision={self.timestamp_precision!r}, "
            f"json_columns_decoded={self.json_columns_decoded!r}, "
            f"uuid_binding={self.uuid_binding!r})"
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            TypeCoercionCapabilities,
            (self.datetime_binding, self.timestamp_precision, self.json_columns_decoded, self.uuid_binding),
        )
