"""Migration validation and out-of-order detection for SQLSpec.

This module provides functionality to detect and handle out-of-order migrations,
which can occur when branches with migrations merge in different orders across
staging and production environments.
"""

import logging
from typing import TYPE_CHECKING

from rich.console import Console

from sqlspec.exceptions import OutOfOrderMigrationError, SquashValidationError
from sqlspec.migrations.version import parse_version
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from sqlspec.migrations.version import MigrationVersion

__all__ = (
    "MigrationGap",
    "detect_out_of_order_migrations",
    "format_out_of_order_warning",
    "validate_extension_consistency",
    "validate_squash_idempotency",
    "validate_squash_range",
)

console = Console()
logger = get_logger("sqlspec.migrations.validation")


class MigrationGap:
    """Represents a migration that is out of order.

    An out-of-order migration occurs when a pending migration has a timestamp
    earlier than already-applied migrations, indicating it was created in a branch
    that merged after other migrations were already applied.

    Attributes:
        missing_version: The out-of-order migration version.
        applied_after: List of already-applied migrations with later timestamps.

    """

    __slots__ = ("_initialized", "applied_after", "missing_version")
    applied_after: "list[MigrationVersion]"
    missing_version: "MigrationVersion"
    _initialized: bool

    def __init__(self, missing_version: "MigrationVersion", applied_after: "list[MigrationVersion]") -> None:
        object.__setattr__(self, "missing_version", missing_version)
        object.__setattr__(self, "applied_after", list(applied_after))
        object.__setattr__(self, "_initialized", True)

    def __repr__(self) -> str:
        return f"MigrationGap(missing_version={self.missing_version!r}, applied_after={self.applied_after!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MigrationGap):
            return NotImplemented
        return self.missing_version == other.missing_version and self.applied_after == other.applied_after

    def __hash__(self) -> int:
        return hash((self.missing_version, tuple(self.applied_after)))

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_initialized":
            object.__setattr__(self, name, value)
            return
        try:
            initialized = self._initialized
        except AttributeError:
            initialized = False
        if initialized:
            msg = "MigrationGap is immutable"
            raise AttributeError(msg)
        object.__setattr__(self, name, value)


def detect_out_of_order_migrations(
    pending_versions: "Sequence[str | None]", applied_versions: "Sequence[str | None]"
) -> "list[MigrationGap]":
    """Detect migrations created before already-applied migrations.

    Identifies pending migrations with timestamps earlier than the latest applied
    migration, which indicates they were created in branches that merged late or
    were cherry-picked across environments.

    Extension migrations are excluded from out-of-order detection as they maintain
    independent sequences within their own namespaces.

    Args:
        pending_versions: List of migration versions not yet applied (may contain None).
        applied_versions: List of migration versions already applied (may contain None).

    Returns:
        List of migration gaps where pending versions are older than applied.

    """
    if not applied_versions or not pending_versions:
        return []

    gaps: list[MigrationGap] = []

    # Filter out None values, empty strings, and whitespace-only strings
    valid_applied = [v for v in applied_versions if v is not None and v.strip()]
    valid_pending = [v for v in pending_versions if v is not None and v.strip()]

    if not valid_applied or not valid_pending:
        return []

    parsed_applied = [parse_version(v) for v in valid_applied]
    parsed_pending = [parse_version(v) for v in valid_pending]

    core_applied = [v for v in parsed_applied if v.extension is None]
    core_pending = [v for v in parsed_pending if v.extension is None]

    if not core_applied or not core_pending:
        return []

    latest_applied = max(core_applied)

    for pending in core_pending:
        if pending < latest_applied:
            applied_after = [a for a in core_applied if a > pending]
            if applied_after:
                gaps.append(MigrationGap(missing_version=pending, applied_after=applied_after))

    return gaps


def format_out_of_order_warning(gaps: "list[MigrationGap]") -> str:
    """Create user-friendly warning message for out-of-order migrations.

    Formats migration gaps into a clear warning message explaining which migrations
    are out of order and what migrations were already applied after them.

    Args:
        gaps: List of migration gaps to format.

    Returns:
        Formatted warning message string.

    Example:
        >>> gaps = [MigrationGap(version1, [version2, version3])]
        >>> print(format_out_of_order_warning(gaps))
        Out-of-order migrations detected:

        - 20251011130000 created before:
          - 20251012140000
          - 20251013090000

    """
    if not gaps:
        return ""

    lines = ["Out-of-order migrations detected:", ""]

    for gap in gaps:
        lines.append(f"- {gap.missing_version.raw} created before:")
        lines.extend(f"  - {applied.raw}" for applied in gap.applied_after)
        lines.append("")

    lines.extend((
        "These migrations will be applied but may cause issues if they",
        "depend on schema changes from later migrations.",
        "",
        "To prevent this in the future, ensure migrations are merged in",
        "chronological order or use strict_ordering mode in migration_config.",
    ))

    return "\n".join(lines)


def validate_migration_order(
    pending_versions: "list[str]",
    applied_versions: "list[str]",
    strict_ordering: bool = False,
    *,
    use_logger: bool = False,
    echo: bool = True,
    summary_only: bool = False,
) -> None:
    """Validate migration order and raise error if out-of-order in strict mode.

    Checks for out-of-order migrations and either warns or raises an error
    depending on the strict_ordering configuration.

    Args:
        pending_versions: List of migration versions not yet applied.
        applied_versions: List of migration versions already applied.
        strict_ordering: If True, raise error for out-of-order migrations.
            If False (default), log warning but allow.
        use_logger: If True, emit warning via logger instead of console.
        echo: Whether to echo output to the console.
        summary_only: Whether summary-only logging is enabled.

    Raises:
        OutOfOrderMigrationError: If out-of-order migrations detected and
            strict_ordering is True.

    Example:
        >>> validate_migration_order(
        ...     ["20251011130000"],
        ...     ["20251012140000"],
        ...     strict_ordering=True,
        ... )
        OutOfOrderMigrationError: Out-of-order migrations detected...

    """
    gaps = detect_out_of_order_migrations(pending_versions, applied_versions)

    if not gaps:
        return

    warning_message = format_out_of_order_warning(gaps)

    if strict_ordering:
        msg = f"{warning_message}\n\nStrict ordering is enabled. Use --allow-missing to override."
        raise OutOfOrderMigrationError(msg)

    if use_logger:
        if not summary_only:
            log_with_context(logger, logging.WARNING, "migration.order.warning", warning_message=warning_message)
        return
    if not echo:
        return
    console.print("[yellow]Out-of-order migrations detected[/]")
    console.print(f"[yellow]{warning_message}[/]")


def validate_squash_range(
    migrations: "list[tuple[str, Path]]", start_version: str, end_version: str, *, allow_gaps: bool = False
) -> "list[tuple[str, Path]]":
    """Validate and filter migrations within a squash range.

    Filters migrations to those within [start, end] range (inclusive),
    validates the range exists, and optionally checks for gaps.

    Args:
        migrations: List of (version, path) tuples for all available migrations.
        start_version: First version in the range to squash (inclusive).
        end_version: Last version in the range to squash (inclusive).
        allow_gaps: If True, skip gap detection. If False (default), raise error on gaps.

    Returns:
        Sorted list of (version, path) tuples within the range.

    Raises:
        SquashValidationError: If validation fails (invalid range, missing versions, gaps).

    """
    if int(start_version) > int(end_version):
        msg = f"Invalid range: start version {start_version} is greater than end version {end_version}"
        raise SquashValidationError(msg)

    version_map: dict[str, Path] = dict(migrations)

    if start_version not in version_map:
        msg = f"Start version {start_version} not found in migrations"
        raise SquashValidationError(msg)
    if end_version not in version_map:
        msg = f"End version {end_version} not found in migrations"
        raise SquashValidationError(msg)

    start_int = int(start_version)
    end_int = int(end_version)
    result: list[tuple[str, Path]] = []

    for version, path in migrations:
        try:
            version_int = int(version)
        except ValueError:
            continue
        if start_int <= version_int <= end_int:
            result.append((version, path))

    result.sort(key=lambda x: int(x[0]))

    if not result:
        msg = f"No migrations found in range {start_version} to {end_version}"
        raise SquashValidationError(msg)

    if not allow_gaps and len(result) > 1:
        sorted_versions = [int(v) for v, _ in result]
        for i in range(1, len(sorted_versions)):
            if sorted_versions[i] - sorted_versions[i - 1] != 1:
                msg = f"Gap detected in version sequence between {sorted_versions[i - 1]:04d} and {sorted_versions[i]:04d}"
                raise SquashValidationError(msg)

    return result


def validate_extension_consistency(migrations: "list[tuple[str, Path]]") -> None:
    """Validate all migrations belong to same namespace (core or single extension).

    Ensures migrations can be safely squashed together by verifying they all
    belong to either core migrations or the same extension namespace.

    Args:
        migrations: List of (version, path) tuples to validate.

    Raises:
        SquashValidationError: If migrations mix core and extension, or different extensions.

    """
    if not migrations:
        return

    extensions: set[str | None] = set()

    for version, _ in migrations:
        parsed = parse_version(version)
        extensions.add(parsed.extension)

    if len(extensions) > 1:
        has_core = None in extensions
        ext_names = [e for e in extensions if e is not None]

        if has_core:
            msg = f"Cannot squash migrations mixing core and extension ({', '.join(ext_names)})"
            raise SquashValidationError(msg)

        msg = f"Cannot squash migrations from different extensions: {', '.join(sorted(ext_names))}"
        raise SquashValidationError(msg)


def validate_squash_idempotency(source_files: "list[Path]", target_file: "Path") -> str:
    """Check if a squash operation has already been performed.

    Determines the current state of a squash operation based on file existence.

    Args:
        source_files: List of original migration file paths that would be squashed.
        target_file: Path where the squashed migration would be written.

    Returns:
        Status string: "ready" (can squash), "already_squashed" (already done),
        or "partial" (inconsistent state - target exists but some sources remain).

    """
    target_exists = target_file.exists()
    sources_exist = [f.exists() for f in source_files]
    any_source_exists = any(sources_exist)

    if not target_exists and any_source_exists:
        return "ready"

    if target_exists and not any_source_exists:
        return "already_squashed"

    if target_exists and any_source_exists:
        return "partial"

    return "ready"
