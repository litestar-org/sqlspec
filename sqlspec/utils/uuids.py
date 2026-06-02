"""UUID and ID generation utilities with optional acceleration.

Provides wrapper functions for uuid3, uuid4, uuid5, uuid6, uuid7, and nanoid generation.
Uses uuid-utils and fastnanoid packages for performance when available,
falling back to standard library.

When uuid-utils is installed:
    - uuid3, uuid4, uuid5, uuid6, uuid7 use the faster Rust implementation
    - uuid6 and uuid7 provide proper time-ordered UUIDs per RFC 9562

When uuid-utils is NOT installed:
    - uuid3, uuid4, uuid5 fall back silently to stdlib (equivalent output)
    - uuid6, uuid7 fall back to uuid4 with a warning (different UUID version)

When fastnanoid is installed:
    - nanoid() uses the Rust implementation for 21-char URL-safe IDs

When fastnanoid is NOT installed:
    - nanoid() falls back to uuid4().hex with a warning (different format)
"""

import uuid as _uuid_mod
import warnings
from typing import Any, cast
from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500, UUID
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid4 as _stdlib_uuid4
from uuid import uuid5 as _stdlib_uuid5

from sqlspec.typing import NANOID_INSTALLED, UUID_UTILS_INSTALLED
from sqlspec.utils.module_loader import import_optional

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


_uuid_utils_mod: Any | None = import_optional("uuid_utils")
_fastnanoid_mod: Any | None = import_optional("fastnanoid")


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
    module = _uuid_utils_mod
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    if module is None:
        return _stdlib_uuid3(namespace_value, name)
    # The uuid-utils module is loaded dynamically, so Mypy treats it as Any.
    # We cast the return value to UUID to satisfy the return type annotation.
    return cast("UUID", module.uuid3(_convert_namespace(namespace_value, module), name))


def uuid4() -> "UUID":
    """Generate a random UUID (version 4).

    Uses uuid-utils for performance when available, falls back to
    standard library uuid.uuid4() silently (equivalent output).

    Returns:
        A randomly generated UUID.
    """
    module = _uuid_utils_mod
    if module is None:
        return _stdlib_uuid4()
    return cast("UUID", module.uuid4())


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
    module = _uuid_utils_mod
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    if module is None:
        return _stdlib_uuid5(namespace_value, name)
    return cast("UUID", module.uuid5(_convert_namespace(namespace_value, module), name))


def uuid6() -> "UUID":
    """Generate a time-ordered UUID (version 6).

    Uses uuid-utils when available, falls back to Python 3.14+ native
    uuid.uuid6() or uuid4() with a warning.

    UUIDv6 is lexicographically sortable by timestamp, making it
    suitable for database primary keys. It is a reordering of UUIDv1
    fields to improve database performance.

    Returns:
        A time-ordered UUID, or a random UUID if time-ordered generation unavailable.
    """
    module = _uuid_utils_mod
    if module is not None:
        return cast("UUID", module.uuid6())

    native_uuid6 = getattr(_uuid_mod, "uuid6", None)
    if native_uuid6 is not None:
        return cast("UUID", native_uuid6())

    warnings.warn(
        "uuid-utils not installed and Python < 3.14, falling back to uuid4 for UUID v6 generation. "
        "Install with: pip install sqlspec[uuid]",
        UserWarning,
        stacklevel=2,
    )
    return _stdlib_uuid4()


def uuid7() -> "UUID":
    """Generate a time-ordered UUID (version 7).

    Uses uuid-utils when available, falls back to Python 3.14+ native
    uuid.uuid7() or uuid4() with a warning.

    UUIDv7 is the recommended time-ordered UUID format per RFC 9562,
    providing millisecond precision timestamps. It is designed for
    modern distributed systems and database primary keys.

    Returns:
        A time-ordered UUID, or a random UUID if time-ordered generation unavailable.
    """
    module = _uuid_utils_mod
    if module is not None:
        return cast("UUID", module.uuid7())

    native_uuid7 = getattr(_uuid_mod, "uuid7", None)
    if native_uuid7 is not None:
        return cast("UUID", native_uuid7())

    warnings.warn(
        "uuid-utils not installed and Python < 3.14, falling back to uuid4 for UUID v7 generation. "
        "Install with: pip install sqlspec[uuid]",
        UserWarning,
        stacklevel=2,
    )
    return _stdlib_uuid4()


def nanoid() -> str:
    """Generate a Nano ID.

    Uses fastnanoid for performance when available. When fastnanoid is
    not installed, falls back to uuid4().hex with a warning.

    Nano IDs are URL-safe, compact 21-character identifiers suitable
    for use as primary keys or short identifiers. The default alphabet
    uses A-Za-z0-9_- characters.

    Returns:
        A 21-character Nano ID string, or 32-character UUID hex if
        fastnanoid unavailable.
    """
    module = _fastnanoid_mod
    if module is None:
        warnings.warn(
            "fastnanoid not installed, falling back to uuid4.hex for Nano ID generation. "
            "Install with: pip install sqlspec[nanoid]",
            UserWarning,
            stacklevel=2,
        )
        return _nanoid_impl()
    return cast("str", module.generate())


def _convert_namespace(namespace: "Any", module: "Any | None") -> "Any":
    """Convert namespace to uuid-utils UUID when available."""
    if module is None:
        return namespace
    uuid_cls = module.UUID
    if isinstance(namespace, uuid_cls):
        return namespace
    return uuid_cls(str(namespace))


def _nanoid_impl() -> str:
    return _stdlib_uuid4().hex
