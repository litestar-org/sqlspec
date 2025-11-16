"""Migration template rendering and configuration utilities."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = (
    "MigrationTemplateProfile",
    "MigrationTemplateSettings",
    "PythonTemplateDefinition",
    "SQLTemplateDefinition",
    "TemplateDescriptionHints",
    "TemplateValidationError",
    "build_template_settings",
)


class TemplateValidationError(SQLSpecError):
    """Raised when a migration template definition is invalid."""


@dataclass(slots=True)
class TemplateDescriptionHints:
    """Hints for extracting descriptions from rendered templates."""

    sql_keys: "tuple[str, ...]" = ("Description",)
    python_keys: "tuple[str, ...]" = ("Description",)


@dataclass(slots=True)
class SQLTemplateDefinition:
    """SQL migration template fragments."""

    header: str
    metadata: "list[str]" = field(default_factory=list)
    body: str = ""
    description_keys: "tuple[str, ...]" = ("Description",)

    def render(self, context: "Mapping[str, str]") -> str:
        """Render the SQL template using the supplied context."""

        rendered_lines: list[str] = [self._format(self.header, context)]
        rendered_lines.extend(self._format(line, context) for line in self.metadata if line)
        rendered_lines.append("")
        rendered_lines.append(self._format(self.body, context))
        return "\n".join(_normalize_newlines(rendered_lines)).rstrip() + "\n"

    def _format(self, template: str, context: "Mapping[str, str]") -> str:
        try:
            return template.format_map(context)
        except KeyError as exc:  # pragma: no cover - defensive
            missing = str(exc).strip("'")
            msg = f"Missing template variable '{missing}' in SQL template"
            raise TemplateValidationError(msg) from exc
        except ValueError as exc:  # pragma: no cover - defensive
            msg = f"Invalid SQL template fragment: {exc}"
            raise TemplateValidationError(msg) from exc


@dataclass(slots=True)
class PythonTemplateDefinition:
    """Python migration template fragments."""

    docstring: str
    body: str
    imports: "list[str]" = field(default_factory=list)
    description_keys: "tuple[str, ...]" = ("Description",)

    def render(self, context: "Mapping[str, str]") -> str:
        """Render the Python template using the supplied context."""

        docstring_block = f'"""{self._format(self.docstring, context)}"""'
        rendered_lines: list[str] = [docstring_block, ""]
        rendered_lines.extend(self.imports)
        if self.imports:
            rendered_lines.append("")
        rendered_lines.append(self._format(self.body, context))
        return "\n".join(_normalize_newlines(rendered_lines)).rstrip() + "\n"

    def _format(self, template: str, context: "Mapping[str, str]") -> str:
        try:
            return template.format_map(context)
        except KeyError as exc:  # pragma: no cover - defensive
            missing = str(exc).strip("'")
            msg = f"Missing template variable '{missing}' in Python template"
            raise TemplateValidationError(msg) from exc
        except ValueError as exc:  # pragma: no cover - defensive
            msg = f"Invalid Python template fragment: {exc}"
            raise TemplateValidationError(msg) from exc


@dataclass(slots=True)
class MigrationTemplateProfile:
    """Concrete template profile selected via configuration."""

    name: str
    title: str
    sql: "SQLTemplateDefinition"
    python: "PythonTemplateDefinition"


@dataclass(slots=True)
class MigrationTemplateSettings:
    """Resolved template configuration for a migration command context."""

    default_format: str
    profile: "MigrationTemplateProfile"

    def resolve_format(self, requested: str | None) -> str:
        """Resolve the effective file format to render."""

        format_choice = (requested or self.default_format or "sql").lower()
        if format_choice not in {"sql", "py"}:
            msg = f"Unsupported migration format '{format_choice}'"
            raise TemplateValidationError(msg)
        return format_choice

    @property
    def description_hints(self) -> "TemplateDescriptionHints":
        """Expose description extraction hints derived from the active profile."""

        return TemplateDescriptionHints(
            sql_keys=self.profile.sql.description_keys, python_keys=self.profile.python.description_keys
        )


def build_template_settings(migration_config: dict[str, Any] | None) -> "MigrationTemplateSettings":
    """Build template settings from migration configuration."""

    config = migration_config or {}
    templates_config = config.get("templates") or {}
    default_format = str(config.get("default_format") or "sql").lower()
    if default_format not in {"sql", "py"}:
        default_format = "sql"
    title = str(config.get("title") or templates_config.get("title") or _DEFAULT_TITLE)
    sql_definition = _build_sql_definition(templates_config.get("sql"))
    python_definition = _build_python_definition(templates_config.get("py"))
    profile = MigrationTemplateProfile(name="default", title=title, sql=sql_definition, python=python_definition)
    return MigrationTemplateSettings(default_format=default_format, profile=profile)


def _build_sql_definition(overrides: Any) -> "SQLTemplateDefinition":
    if overrides is None:
        return _DEFAULT_SQL_TEMPLATE
    if not isinstance(overrides, dict):
        msg = "SQL template override must be a mapping"
        raise TemplateValidationError(msg)
    header = str(overrides.get("header") or _DEFAULT_SQL_TEMPLATE.header)
    metadata = _coerce_string_list(overrides.get("metadata"), _DEFAULT_SQL_TEMPLATE.metadata)
    body = str(overrides.get("body") or _DEFAULT_SQL_TEMPLATE.body)
    description = _coerce_string_list(overrides.get("description_key"), list(_DEFAULT_SQL_TEMPLATE.description_keys))
    description_keys = tuple(description) if description else _DEFAULT_SQL_TEMPLATE.description_keys
    return SQLTemplateDefinition(header=header, metadata=metadata, body=body, description_keys=description_keys)


def _build_python_definition(overrides: Any) -> "PythonTemplateDefinition":
    if overrides is None:
        return _DEFAULT_PY_TEMPLATE
    if not isinstance(overrides, dict):
        msg = "Python template override must be a mapping"
        raise TemplateValidationError(msg)
    docstring = str(overrides.get("docstring") or _DEFAULT_PY_TEMPLATE.docstring)
    body = str(overrides.get("body") or _DEFAULT_PY_TEMPLATE.body)
    imports = _coerce_string_list(overrides.get("imports"), _DEFAULT_PY_TEMPLATE.imports)
    description = _coerce_string_list(overrides.get("description_key"), list(_DEFAULT_PY_TEMPLATE.description_keys))
    description_keys = tuple(description) if description else _DEFAULT_PY_TEMPLATE.description_keys
    return PythonTemplateDefinition(docstring=docstring, body=body, imports=imports, description_keys=description_keys)


def _coerce_string_list(value: Any, default: "list[str]") -> "list[str]":
    if value is None:
        return list(default)
    if isinstance(value, str):
        return [line for line in value.splitlines() if line]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    msg = "Template list override must be a string or list"
    raise TemplateValidationError(msg)


def _normalize_newlines(lines: "list[str]") -> "list[str]":
    normalized: list[str] = [line.rstrip("\r") for line in lines]
    return normalized


_DEFAULT_TITLE = "SQLSpec Migration"

_DEFAULT_SQL_TEMPLATE = SQLTemplateDefinition(
    header="-- {title}",
    metadata=[
        "-- Version: {version}",
        "-- Description: {description}",
        "-- Created: {created_at}",
        "-- Author: {author}",
    ],
    body=(
        "-- name: migrate-{version}-up\n"
        "CREATE TABLE placeholder (\n"
        "    id INTEGER PRIMARY KEY\n"
        ");\n\n"
        "-- name: migrate-{version}-down\n"
        "DROP TABLE placeholder;"
    ),
)

_DEFAULT_PY_TEMPLATE = PythonTemplateDefinition(
    docstring=(
        "{title} - {message}\n"
        "Description: {description}\n"
        "Version: {version}\n"
        "Created: {created_at}\n"
        "Author: {author}\n\n"
        "Replace 'def' with 'async def' if you need awaitables. The optional"
        " context argument receives the SQLSpec migration context when provided."
    ),
    imports=["from typing import Iterable"],
    body=(
        "def up(context: object | None = None) -> str | Iterable[str]:\n"
        '    """Apply the migration (upgrade)."""\n'
        '    return "\n'
        "    CREATE TABLE example (\n"
        "        id INTEGER PRIMARY KEY,\n"
        "        name TEXT NOT NULL\n"
        "    );\n"
        '    "\n\n'
        "def down(context: object | None = None) -> str | Iterable[str]:\n"
        '    """Reverse the migration."""\n'
        '    return "DROP TABLE example;"'
    ),
)
