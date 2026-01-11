"""Migration version parsing and comparison utilities.

Provides structured parsing of migration versions supporting both legacy sequential
(0001) and timestamp-based (20251011120000) formats with type-safe comparison.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlspec.utils.logging import get_logger

__all__ = (
    "MigrationVersion",
    "VersionType",
    "convert_to_sequential_version",
    "generate_conversion_map",
    "generate_timestamp_version",
    "get_next_sequential_number",
    "is_sequential_version",
    "is_timestamp_version",
    "parse_version",
)

logger = get_logger(__name__)

SEQUENTIAL_PATTERN = re.compile(r"^(?!\d{14}$)\d+$")
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

    def __le__(self, other: "MigrationVersion") -> bool:
        """Check if version is less than or equal to another.

        Args:
            other: Version to compare against.

        Returns:
            True if this version is less than or equal to other.
        """
        return self == other or self < other

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


def is_sequential_version(version_str: "str | None") -> bool:
    """Check if version string is sequential format.

    Sequential format: Any sequence of digits (0001, 42, 9999, 10000+).

    Args:
        version_str: Version string to check.

    Returns:
        True if sequential format, False if None or whitespace.

    Examples:
        >>> is_sequential_version("0001")
        True
        >>> is_sequential_version("42")
        True
        >>> is_sequential_version("10000")
        True
        >>> is_sequential_version("20251011120000")
        False
        >>> is_sequential_version(None)
        False
    """
    if version_str is None or not version_str.strip():
        return False
    return bool(SEQUENTIAL_PATTERN.match(version_str))


def is_timestamp_version(version_str: "str | None") -> bool:
    """Check if version string is timestamp format.

    Timestamp format: 14-digit YYYYMMDDHHmmss (20251011120000).

    Args:
        version_str: Version string to check.

    Returns:
        True if timestamp format, False if None or whitespace.

    Examples:
        >>> is_timestamp_version("20251011120000")
        True
        >>> is_timestamp_version("0001")
        False
        >>> is_timestamp_version(None)
        False
    """
    if version_str is None or not version_str.strip():
        return False
    if not TIMESTAMP_PATTERN.match(version_str):
        return False

    try:
        datetime.strptime(version_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    else:
        return True


def parse_version(version_str: "str | None") -> MigrationVersion:
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
        ValueError: If version format is invalid, None, or whitespace-only.

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
    if version_str is None or not version_str.strip():
        msg = "Invalid migration version: version string is None or empty"
        raise ValueError(msg)

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

    if is_timestamp_version(version_str):
        dt = datetime.strptime(version_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return MigrationVersion(
            raw=version_str, type=VersionType.TIMESTAMP, sequence=None, timestamp=dt, extension=None
        )

    if is_sequential_version(version_str):
        return MigrationVersion(
            raw=version_str, type=VersionType.SEQUENTIAL, sequence=int(version_str), timestamp=None, extension=None
        )

    msg = f"Invalid migration version format: {version_str}. Expected sequential (0001) or timestamp (YYYYMMDDHHmmss)."
    raise ValueError(msg)


def _try_parse_version(version_str: str) -> "MigrationVersion | None":
    """Parse version string, returning None for invalid versions."""
    try:
        return parse_version(version_str)
    except ValueError:
        logger.warning("Skipping invalid migration version: %s", version_str)
        return None


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


def get_next_sequential_number(migrations: "list[MigrationVersion]", extension: "str | None" = None) -> int:
    """Find highest sequential number and return next available.

    Scans migrations for sequential versions and returns the next number in sequence.
    When extension is specified, only that extension's migrations are considered.
    When extension is None, only core (non-extension) migrations are considered.

    Args:
        migrations: List of parsed migration versions.
        extension: Optional extension name to filter by (e.g., "litestar", "adk").
                  None means core migrations only.

    Returns:
        Next available sequential number (1 if no sequential migrations exist).

    Examples:
        >>> v1 = parse_version("0001")
        >>> v2 = parse_version("0002")
        >>> get_next_sequential_number([v1, v2])
        3

        >>> get_next_sequential_number([])
        1

        >>> ext = parse_version("ext_litestar_0001")
        >>> core = parse_version("0001")
        >>> get_next_sequential_number([ext, core])
        2

        >>> ext1 = parse_version("ext_litestar_0001")
        >>> get_next_sequential_number([ext1], extension="litestar")
        2
    """
    sequential = [
        m.sequence for m in migrations if m.type == VersionType.SEQUENTIAL and m.extension == extension and m.sequence
    ]

    if not sequential:
        return 1

    return max(sequential) + 1


def convert_to_sequential_version(timestamp_version: MigrationVersion, sequence_number: int) -> str:
    """Convert timestamp MigrationVersion to sequential string format.

    Preserves extension prefixes during conversion. Format uses zero-padded
    4-digit numbers (0001, 0002, etc.).

    Args:
        timestamp_version: Parsed timestamp version to convert.
        sequence_number: Sequential number to assign.

    Returns:
        Sequential version string with extension prefix if applicable.

    Raises:
        ValueError: If input is not a timestamp version.

    Examples:
        >>> v = parse_version("20251011120000")
        >>> convert_to_sequential_version(v, 3)
        '0003'

        >>> v = parse_version("ext_litestar_20251011120000")
        >>> convert_to_sequential_version(v, 1)
        'ext_litestar_0001'

        >>> v = parse_version("0001")
        >>> convert_to_sequential_version(v, 2)
        Traceback (most recent call last):
            ...
        ValueError: Can only convert timestamp versions to sequential
    """
    if timestamp_version.type != VersionType.TIMESTAMP:
        msg = "Can only convert timestamp versions to sequential"
        raise ValueError(msg)

    seq_str = str(sequence_number).zfill(4)

    if timestamp_version.extension:
        return f"ext_{timestamp_version.extension}_{seq_str}"

    return seq_str


def generate_conversion_map(migrations: "list[tuple[str, Any]]") -> "dict[str, str]":
    """Generate mapping from timestamp versions to sequential versions.

    Separates timestamp migrations from sequential, sorts timestamps chronologically,
    and assigns sequential numbers starting after the highest existing sequential
    number. Extension migrations maintain separate numbering within their namespace.

    Args:
        migrations: List of tuples (version_string, migration_path).

    Returns:
        Dictionary mapping old timestamp versions to new sequential versions.

    Examples:
        >>> migrations = [
        ...     ("0001", Path("0001_init.sql")),
        ...     ("0002", Path("0002_users.sql")),
        ...     ("20251011120000", Path("20251011120000_products.sql")),
        ...     ("20251012130000", Path("20251012130000_orders.sql")),
        ... ]
        >>> result = generate_conversion_map(migrations)
        >>> result
        {'20251011120000': '0003', '20251012130000': '0004'}

        >>> migrations = [
        ...     ("20251011120000", Path("20251011120000_first.sql")),
        ...     ("20251010090000", Path("20251010090000_earlier.sql")),
        ... ]
        >>> result = generate_conversion_map(migrations)
        >>> result
        {'20251010090000': '0001', '20251011120000': '0002'}

        >>> migrations = []
        >>> generate_conversion_map(migrations)
        {}
    """
    if not migrations:
        return {}

    parsed_versions = [v for version_str, _path in migrations if (v := _try_parse_version(version_str)) is not None]

    timestamp_migrations = sorted([v for v in parsed_versions if v.type == VersionType.TIMESTAMP])

    if not timestamp_migrations:
        return {}

    core_timestamps = [m for m in timestamp_migrations if m.extension is None]
    ext_timestamps_by_name: dict[str, list[MigrationVersion]] = {}
    for m in timestamp_migrations:
        if m.extension:
            ext_timestamps_by_name.setdefault(m.extension, []).append(m)

    conversion_map: dict[str, str] = {}

    if core_timestamps:
        next_seq = get_next_sequential_number(parsed_versions)
        for timestamp_version in core_timestamps:
            sequential_version = convert_to_sequential_version(timestamp_version, next_seq)
            conversion_map[timestamp_version.raw] = sequential_version
            next_seq += 1

    for ext_name, ext_migrations in ext_timestamps_by_name.items():
        ext_parsed = [v for v in parsed_versions if v.extension == ext_name]
        next_seq = get_next_sequential_number(ext_parsed, extension=ext_name)
        for timestamp_version in ext_migrations:
            sequential_version = convert_to_sequential_version(timestamp_version, next_seq)
            conversion_map[timestamp_version.raw] = sequential_version
            next_seq += 1

    return conversion_map
