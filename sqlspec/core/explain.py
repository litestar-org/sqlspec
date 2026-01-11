"""EXPLAIN plan options and format definitions.

Provides types for configuring EXPLAIN statement generation across different
database dialects.
"""

from enum import Enum
from typing import Any, Final

from mypy_extensions import mypyc_attr

__all__ = ("ExplainFormat", "ExplainOptions")


EXPLAIN_OPTIONS_SLOTS: Final = (
    "analyze",
    "verbose",
    "format",
    "costs",
    "buffers",
    "timing",
    "summary",
    "memory",
    "settings",
    "wal",
    "generic_plan",
)


class ExplainFormat(str, Enum):
    """Output formats for EXPLAIN statements.

    Different databases support different output formats:
    - TEXT: All databases (default)
    - JSON: PostgreSQL, MySQL (8.0+), DuckDB
    - XML: PostgreSQL
    - YAML: PostgreSQL
    - TREE: MySQL (8.0+), DuckDB
    - TRADITIONAL: MySQL (tabular output)
    """

    TEXT = "text"
    JSON = "json"
    XML = "xml"
    YAML = "yaml"
    TREE = "tree"
    TRADITIONAL = "traditional"


@mypyc_attr(allow_interpreted_subclasses=False)
class ExplainOptions:
    """Configuration options for EXPLAIN statements.

    Encapsulates all possible EXPLAIN options across different database dialects.
    Not all options are supported by all databases - the builder will select
    appropriate options based on the target dialect.

    PostgreSQL Options:
        analyze: Execute the statement and show actual runtime statistics
        verbose: Show additional information (schema-qualified names, etc.)
        costs: Include estimated costs (default True in PostgreSQL)
        buffers: Include buffer usage information (requires ANALYZE)
        timing: Include actual timing information (requires ANALYZE)
        summary: Include summary information after the query plan
        memory: Include memory usage information (PostgreSQL 17+)
        settings: Include configuration parameter information (PostgreSQL 12+)
        wal: Include WAL usage information (requires ANALYZE, PostgreSQL 13+)
        generic_plan: Generate a generic plan ignoring parameter values (PostgreSQL 16+)
        format: Output format (TEXT, JSON, XML, YAML)

    MySQL Options:
        analyze: Execute and show actual statistics (always uses TREE format)
        format: Output format (TRADITIONAL, JSON, TREE)

    SQLite:
        Only supports EXPLAIN QUERY PLAN (no additional options)

    DuckDB:
        analyze: Execute and show actual statistics
        format: Output format (TEXT, JSON)

    Oracle:
        Uses EXPLAIN PLAN FOR + DBMS_XPLAN.DISPLAY (special handling)

    BigQuery:
        analyze: Execute and show actual statistics (incurs costs!)
    """

    __slots__ = EXPLAIN_OPTIONS_SLOTS

    def __init__(
        self,
        analyze: bool = False,
        verbose: bool = False,
        format: "ExplainFormat | str | None" = None,
        costs: bool | None = None,
        buffers: bool | None = None,
        timing: bool | None = None,
        summary: bool | None = None,
        memory: bool | None = None,
        settings: bool | None = None,
        wal: bool | None = None,
        generic_plan: bool | None = None,
    ) -> None:
        """Initialize ExplainOptions.

        Args:
            analyze: Execute the statement and show actual runtime statistics
            verbose: Show additional information
            format: Output format (TEXT, JSON, XML, YAML, TREE, TRADITIONAL)
            costs: Include estimated costs
            buffers: Include buffer usage information
            timing: Include actual timing information
            summary: Include summary information
            memory: Include memory usage information
            settings: Include configuration parameter information
            wal: Include WAL usage information
            generic_plan: Generate a generic plan ignoring parameter values
        """
        self.analyze = analyze
        self.verbose = verbose
        self.costs = costs
        self.buffers = buffers
        self.timing = timing
        self.summary = summary
        self.memory = memory
        self.settings = settings
        self.wal = wal
        self.generic_plan = generic_plan

        if format is not None:
            if isinstance(format, ExplainFormat):
                self.format: ExplainFormat | None = format
            else:
                self.format = ExplainFormat(format.lower())
        else:
            self.format = None

    def __repr__(self) -> str:
        """String representation of ExplainOptions."""
        parts = []
        if self.analyze:
            parts.append("analyze=True")
        if self.verbose:
            parts.append("verbose=True")
        if self.format is not None:
            parts.append(f"format={self.format.value!r}")
        if self.costs is not None:
            parts.append(f"costs={self.costs}")
        if self.buffers is not None:
            parts.append(f"buffers={self.buffers}")
        if self.timing is not None:
            parts.append(f"timing={self.timing}")
        if self.summary is not None:
            parts.append(f"summary={self.summary}")
        if self.memory is not None:
            parts.append(f"memory={self.memory}")
        if self.settings is not None:
            parts.append(f"settings={self.settings}")
        if self.wal is not None:
            parts.append(f"wal={self.wal}")
        if self.generic_plan is not None:
            parts.append(f"generic_plan={self.generic_plan}")
        return f"ExplainOptions({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, ExplainOptions):
            return False
        return (
            self.analyze == other.analyze
            and self.verbose == other.verbose
            and self.format == other.format
            and self.costs == other.costs
            and self.buffers == other.buffers
            and self.timing == other.timing
            and self.summary == other.summary
            and self.memory == other.memory
            and self.settings == other.settings
            and self.wal == other.wal
            and self.generic_plan == other.generic_plan
        )

    def __hash__(self) -> int:
        """Hash computation."""
        return hash((
            self.analyze,
            self.verbose,
            self.format,
            self.costs,
            self.buffers,
            self.timing,
            self.summary,
            self.memory,
            self.settings,
            self.wal,
            self.generic_plan,
        ))

    def copy(
        self,
        analyze: "bool | None" = None,
        verbose: "bool | None" = None,
        format: "ExplainFormat | str | None" = None,
        costs: "bool | None" = None,
        buffers: "bool | None" = None,
        timing: "bool | None" = None,
        summary: "bool | None" = None,
        memory: "bool | None" = None,
        settings: "bool | None" = None,
        wal: "bool | None" = None,
        generic_plan: "bool | None" = None,
    ) -> "ExplainOptions":
        """Create a copy with modifications.

        Args:
            analyze: Override analyze setting
            verbose: Override verbose setting
            format: Override format setting
            costs: Override costs setting
            buffers: Override buffers setting
            timing: Override timing setting
            summary: Override summary setting
            memory: Override memory setting
            settings: Override settings setting
            wal: Override wal setting
            generic_plan: Override generic_plan setting

        Returns:
            New ExplainOptions instance with modifications applied
        """
        return ExplainOptions(
            analyze=analyze if analyze is not None else self.analyze,
            verbose=verbose if verbose is not None else self.verbose,
            format=format if format is not None else self.format,
            costs=costs if costs is not None else self.costs,
            buffers=buffers if buffers is not None else self.buffers,
            timing=timing if timing is not None else self.timing,
            summary=summary if summary is not None else self.summary,
            memory=memory if memory is not None else self.memory,
            settings=settings if settings is not None else self.settings,
            wal=wal if wal is not None else self.wal,
            generic_plan=generic_plan if generic_plan is not None else self.generic_plan,
        )

    def to_dict(self) -> "dict[str, Any]":
        """Convert options to dictionary (only non-None values).

        Returns:
            Dictionary of option names to values
        """
        result: dict[str, Any] = {}
        if self.analyze:
            result["analyze"] = True
        if self.verbose:
            result["verbose"] = True
        if self.format is not None:
            result["format"] = self.format.value.upper()
        if self.costs is not None:
            result["costs"] = self.costs
        if self.buffers is not None:
            result["buffers"] = self.buffers
        if self.timing is not None:
            result["timing"] = self.timing
        if self.summary is not None:
            result["summary"] = self.summary
        if self.memory is not None:
            result["memory"] = self.memory
        if self.settings is not None:
            result["settings"] = self.settings
        if self.wal is not None:
            result["wal"] = self.wal
        if self.generic_plan is not None:
            result["generic_plan"] = self.generic_plan
        return result
