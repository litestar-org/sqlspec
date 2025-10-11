"""Migration version parsing and comparison utilities.

Provides structured parsing of migration versions supporting both legacy sequential
(0001) and timestamp-based (20251011120000) formats with type-safe comparison.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

__all__ = (
    "MigrationVersion",
    "VersionType",
    "generate_timestamp_version",
    "is_sequential_version",
    "is_timestamp_version",
    "parse_version",
)

logger = logging.getLogger(__name__)

# Regex patterns for version detection
SEQUENTIAL_PATTERN = re.compile(r"^(\d{1,4})$")
TIMESTAMP_PATTERN = re.compile(r"^(\d{14})$")
EXTENSION_PATTERN = re.compile(r"^ext_(\w+)_(.+)$")


class VersionType(Enum):
    """Migration version format type."""

    SEQUENTIAL = "sequential"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True)
class MigrationVersion:
    """Parsed migration version with structured comparison support.

    Attributes:
        raw: Original version string (e.g., "0001", "20251011120000", "ext_litestar_0001").
        type: Version format type (sequential or timestamp).
        sequence: Numeric value for sequential versions (e.g., 1, 2, 42).
        timestamp: Parsed datetime for timestamp versions (UTC).
        extension: Extension name for extension-prefixed versions (e.g., "litestar").
    """

    raw: str
    type: VersionType
    sequence: "int | None"
    timestamp: "datetime | None"
    extension: "str | None"

    def __lt__(self, other: "MigrationVersion") -> bool:
        """Compare versions supporting mixed formats.

        Comparison Rules:
            1. Extension migrations sort by extension name first, then version
            2. Sequential < Timestamp (legacy migrations first)
            3. Sequential vs Sequential: numeric comparison
            4. Timestamp vs Timestamp: chronological comparison

        Args:
            other: Version to compare against.

        Returns:
            True if this version sorts before other.

        Raises:
            TypeError: If comparing against non-MigrationVersion.
        """
        if not isinstance(other, MigrationVersion):
            return NotImplemented

        if self.extension != other.extension:
            if self.extension is None:
                return True
            if other.extension is None:
                return False
            return self.extension < other.extension

        if self.type == other.type:
            if self.type == VersionType.SEQUENTIAL:
                return (self.sequence or 0) < (other.sequence or 0)
            return (self.timestamp or datetime.min.replace(tzinfo=timezone.utc)) < (
                other.timestamp or datetime.min.replace(tzinfo=timezone.utc)
            )

        return self.type == VersionType.SEQUENTIAL

    def __eq__(self, other: object) -> bool:
        """Check version equality.

        Args:
            other: Version to compare against.

        Returns:
            True if versions are equal.
        """
        if not isinstance(other, MigrationVersion):
            return NotImplemented
        return self.raw == other.raw

    def __hash__(self) -> int:
        """Hash version for use in sets and dicts.

        Returns:
            Hash value based on raw version string.
        """
        return hash(self.raw)

    def __repr__(self) -> str:
        """Get string representation for debugging.

        Returns:
            String representation with type and value.
        """
        if self.extension:
            return f"MigrationVersion(ext={self.extension}, {self.type.value}={self.raw})"
        return f"MigrationVersion({self.type.value}={self.raw})"


def is_sequential_version(version_str: str) -> bool:
    """Check if version string is sequential format.

    Sequential format: 1-4 digit number (0001, 42, 9999).

    Args:
        version_str: Version string to check.

    Returns:
        True if sequential format.

    Examples:
        >>> is_sequential_version("0001")
        True
        >>> is_sequential_version("42")
        True
        >>> is_sequential_version("20251011120000")
        False
    """
    return bool(SEQUENTIAL_PATTERN.match(version_str))


def is_timestamp_version(version_str: str) -> bool:
    """Check if version string is timestamp format.

    Timestamp format: 14-digit YYYYMMDDHHmmss (20251011120000).

    Args:
        version_str: Version string to check.

    Returns:
        True if timestamp format.

    Examples:
        >>> is_timestamp_version("20251011120000")
        True
        >>> is_timestamp_version("0001")
        False
    """
    if not TIMESTAMP_PATTERN.match(version_str):
        return False

    try:
        datetime.strptime(version_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    else:
        return True


def parse_version(version_str: str) -> MigrationVersion:
    """Parse version string into structured format.

    Supports:
        - Sequential: "0001", "42", "9999"
        - Timestamp: "20251011120000"
        - Extension: "ext_litestar_0001", "ext_litestar_20251011120000"

    Args:
        version_str: Version string to parse.

    Returns:
        Parsed migration version.

    Raises:
        ValueError: If version format is invalid.

    Examples:
        >>> v = parse_version("0001")
        >>> v.type == VersionType.SEQUENTIAL
        True
        >>> v.sequence
        1

        >>> v = parse_version("20251011120000")
        >>> v.type == VersionType.TIMESTAMP
        True

        >>> v = parse_version("ext_litestar_0001")
        >>> v.extension
        'litestar'
    """
    extension_match = EXTENSION_PATTERN.match(version_str)
    if extension_match:
        extension_name = extension_match.group(1)
        base_version = extension_match.group(2)
        parsed = parse_version(base_version)

        return MigrationVersion(
            raw=version_str,
            type=parsed.type,
            sequence=parsed.sequence,
            timestamp=parsed.timestamp,
            extension=extension_name,
        )

    if is_sequential_version(version_str):
        return MigrationVersion(
            raw=version_str, type=VersionType.SEQUENTIAL, sequence=int(version_str), timestamp=None, extension=None
        )

    if is_timestamp_version(version_str):
        dt = datetime.strptime(version_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return MigrationVersion(
            raw=version_str, type=VersionType.TIMESTAMP, sequence=None, timestamp=dt, extension=None
        )

    msg = f"Invalid migration version format: {version_str}. Expected sequential (0001) or timestamp (YYYYMMDDHHmmss)."
    raise ValueError(msg)


def generate_timestamp_version() -> str:
    """Generate new timestamp version in UTC.

    Format: YYYYMMDDHHmmss (14 digits).

    Returns:
        Timestamp version string.

    Examples:
        >>> version = generate_timestamp_version()
        >>> len(version)
        14
        >>> is_timestamp_version(version)
        True
    """
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
