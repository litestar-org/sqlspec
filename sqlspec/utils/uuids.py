"""UUID and ID generation utilities with optional acceleration.

Provides wrapper functions for uuid3, uuid4, uuid5, uuid6, uuid7, and nanoid generation.
Uses uuid-utils and fastnanoid packages for performance when available,
falling back to standard library.

When uuid-utils is installed:
    - uuid3, uuid4, uuid5, uuid6, uuid7 use the faster Rust implementation
    - uuid6 and uuid7 provide proper time-ordered UUIDs per RFC 9562

When uuid-utils is NOT installed:
    - uuid3, uuid4, uuid5 fall back silently to stdlib (equivalent output)
    - uuid6, uuid7 fall back to uuid4 with a one-time warning (different UUID version)

When fastnanoid is installed:
    - nanoid() uses the Rust implementation for 21-char URL-safe IDs

When fastnanoid is NOT installed:
    - nanoid() falls back to uuid4().hex with a one-time warning (different format)
"""

import warnings
from typing import TYPE_CHECKING, Any
from uuid import UUID
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid4 as _stdlib_uuid4
from uuid import uuid5 as _stdlib_uuid5

from sqlspec.typing import NANOID_INSTALLED, UUID_UTILS_INSTALLED

__all__ = (
    "NAMESPACE_DNS",
    "NAMESPACE_OID",
    "NAMESPACE_URL",
    "NAMESPACE_X500",
    "NANOID_INSTALLED",
    "UUID_UTILS_INSTALLED",
    "nanoid",
    "uuid3",
    "uuid4",
    "uuid5",
    "uuid6",
    "uuid7",
)

_uuid6_warned: bool = False
_uuid7_warned: bool = False
_nanoid_warned: bool = False

if UUID_UTILS_INSTALLED and not TYPE_CHECKING:
    from uuid_utils import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500
    from uuid_utils import UUID as _UUID_UTILS_UUID
    from uuid_utils import uuid3 as _uuid3
    from uuid_utils import uuid4 as _uuid4
    from uuid_utils import uuid5 as _uuid5
    from uuid_utils import uuid6 as _uuid6
    from uuid_utils import uuid7 as _uuid7

    def _convert_namespace(namespace: "Any") -> "_UUID_UTILS_UUID":
        """Convert a namespace to uuid_utils.UUID if needed."""
        if isinstance(namespace, _UUID_UTILS_UUID):
            return namespace
        return _UUID_UTILS_UUID(str(namespace))

else:
    from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500

    _uuid3 = _stdlib_uuid3
    _uuid4 = _stdlib_uuid4
    _uuid5 = _stdlib_uuid5
    _uuid6 = _stdlib_uuid4
    _uuid7 = _stdlib_uuid4
    _UUID_UTILS_UUID = UUID

    def _convert_namespace(namespace: "Any") -> "UUID":
        """Pass through namespace when uuid-utils is not installed."""
        return namespace  # type: ignore[no-any-return]


if NANOID_INSTALLED and not TYPE_CHECKING:
    from fastnanoid import generate as _nanoid_impl
else:

    def _nanoid_impl() -> str:
        return _stdlib_uuid4().hex


def uuid3(name: str, namespace: "UUID | None" = None) -> "UUID":
    """Generate a deterministic UUID (version 3) using MD5 hash.

    Uses uuid-utils for performance when available, falls back to
    standard library uuid.uuid3() silently (equivalent output).

    Args:
        name: The name to hash within the namespace.
        namespace: The namespace UUID. Defaults to NAMESPACE_DNS if not provided.

    Returns:
        A deterministic UUID based on namespace and name.
    """
    namespace = NAMESPACE_DNS if namespace is None else _convert_namespace(namespace)
    return _uuid3(namespace, name)


def uuid4() -> "UUID":
    """Generate a random UUID (version 4).

    Uses uuid-utils for performance when available, falls back to
    standard library uuid.uuid4() silently (equivalent output).

    Returns:
        A randomly generated UUID.
    """
    return _uuid4()


def uuid5(name: str, namespace: "UUID | None" = None) -> "UUID":
    """Generate a deterministic UUID (version 5) using SHA-1 hash.

    Uses uuid-utils for performance when available, falls back to
    standard library uuid.uuid5() silently (equivalent output).

    Args:
        name: The name to hash within the namespace.
        namespace: The namespace UUID. Defaults to NAMESPACE_DNS if not provided.

    Returns:
        A deterministic UUID based on namespace and name.
    """
    namespace = NAMESPACE_DNS if namespace is None else _convert_namespace(namespace)
    return _uuid5(namespace, name)


def uuid6() -> "UUID":
    """Generate a time-ordered UUID (version 6).

    Uses uuid-utils when available. When uuid-utils is not installed,
    falls back to uuid4() with a warning (emitted once per session).

    UUIDv6 is lexicographically sortable by timestamp, making it
    suitable for database primary keys. It is a reordering of UUIDv1
    fields to improve database performance.

    Returns:
        A time-ordered UUID, or a random UUID if uuid-utils unavailable.
    """
    global _uuid6_warned
    if not UUID_UTILS_INSTALLED and not _uuid6_warned:
        warnings.warn(
            "uuid-utils not installed, falling back to uuid4 for UUID v6 generation. "
            "Install with: pip install sqlspec[uuid]",
            UserWarning,
            stacklevel=2,
        )
        _uuid6_warned = True
    return _uuid6()


def uuid7() -> "UUID":
    """Generate a time-ordered UUID (version 7).

    Uses uuid-utils when available. When uuid-utils is not installed,
    falls back to uuid4() with a warning (emitted once per session).

    UUIDv7 is the recommended time-ordered UUID format per RFC 9562,
    providing millisecond precision timestamps. It is designed for
    modern distributed systems and database primary keys.

    Returns:
        A time-ordered UUID, or a random UUID if uuid-utils unavailable.
    """
    global _uuid7_warned
    if not UUID_UTILS_INSTALLED and not _uuid7_warned:
        warnings.warn(
            "uuid-utils not installed, falling back to uuid4 for UUID v7 generation. "
            "Install with: pip install sqlspec[uuid]",
            UserWarning,
            stacklevel=2,
        )
        _uuid7_warned = True
    return _uuid7()


def nanoid() -> str:
    """Generate a Nano ID.

    Uses fastnanoid for performance when available. When fastnanoid is
    not installed, falls back to uuid4().hex with a warning (emitted
    once per session).

    Nano IDs are URL-safe, compact 21-character identifiers suitable
    for use as primary keys or short identifiers. The default alphabet
    uses A-Za-z0-9_- characters.

    Returns:
        A 21-character Nano ID string, or 32-character UUID hex if
        fastnanoid unavailable.
    """
    global _nanoid_warned
    if not NANOID_INSTALLED and not _nanoid_warned:
        warnings.warn(
            "fastnanoid not installed, falling back to uuid4.hex for Nano ID generation. "
            "Install with: pip install sqlspec[nanoid]",
            UserWarning,
            stacklevel=2,
        )
        _nanoid_warned = True
    return _nanoid_impl()
