"""Pure SQL and ownership rules for PostgreSQL tenant session settings."""

import re
from collections.abc import Callable, Mapping, Sequence
from typing import ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import TenantLeaseState, validate_session_setting_name
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContextPayload
from sqlspec.utils.text import quote_identifier

__all__ = ("PostgresTenantSettings", "build_setting_query", "psycopg_setting_query")


DEFAULT_TENANT_KEY = "app.current_tenant_id"
DEFAULT_SEARCH_PATH_RESTORE = '"$user", public'


def build_setting_query(count: int, *, restore: bool = False) -> str:
    """Build one management statement with an explicit evaluation dependency."""
    if restore:
        applied = ", ".join(f"set_config(${2 * i + 1}, ${2 * i + 2}, false) AS v{i}" for i in range(count))
        output = ", ".join(
            f"applied.v{i}, current_setting(${2 * i + 1} || substring(applied.v{i}, 1, 0), true)" for i in range(count)
        )
        return f"WITH applied AS MATERIALIZED (SELECT {applied}) SELECT {output} FROM applied"
    captured = ", ".join(f"current_setting(${2 * i + 1}, true) AS b{i}" for i in range(count))
    output = ", ".join(f"captured.b{i}, set_config(${2 * i + 1}, ${2 * i + 2}, false)" for i in range(count))
    return f"WITH captured AS MATERIALIZED (SELECT {captured}) SELECT {output} FROM captured"


def psycopg_setting_query(sql: str, params: Sequence[str]) -> tuple[str, dict[str, str]]:
    """Translate numeric management parameters to psycopg named placeholders."""
    return re.sub(r"\$(\d+)", r"%(p\1)s", sql), {f"p{i}": value for i, value in enumerate(params, 1)}


@mypyc_attr(allow_interpreted_subclasses=True)
class PostgresTenantSettings:
    """Validate owned settings and prepare bound management statements."""

    __slots__ = ("_default_checkout_sql", "_schema_resolver", "_sorted_variable_keys", "_tenant_key", "_variable_keys")

    _schema_resolver: Callable[[TenantContextPayload], object] | None
    _unset_restore_value: ClassVar[str] = ""

    def __init__(
        self,
        *,
        tenant_key: object = DEFAULT_TENANT_KEY,
        variable_keys: Mapping[str, str] | None = None,
        schema_resolver: Callable[[TenantContextPayload], str] | None = None,
    ) -> None:
        self._tenant_key = validate_session_setting_name(tenant_key)
        self._variable_keys = dict(variable_keys or {})
        unvalidated = cast("Mapping[object, object]", self._variable_keys)
        if any(not isinstance(key, str) or not key for key in unvalidated):
            msg = "Tenant variable names must be non-empty strings"
            raise ImproperConfigurationError(msg)
        targets = {self._tenant_key.casefold()}
        if schema_resolver is not None:
            targets.add("search_path")
            if self._tenant_key.casefold() == "search_path":
                msg = "Tenant key conflicts with strategy-owned search_path"
                raise ImproperConfigurationError(msg)
        for target in unvalidated.values():
            normalized = validate_session_setting_name(target).casefold()
            if normalized in targets:
                msg = "Tenant session setting targets must be distinct"
                raise ImproperConfigurationError(msg)
            targets.add(normalized)
        self._sorted_variable_keys = tuple(sorted(self._variable_keys))
        self._schema_resolver = schema_resolver
        self._default_checkout_sql = build_setting_query(
            1 + len(self._variable_keys) + int(schema_resolver is not None)
        )

    def prepare_checkout(self, payload: TenantContextPayload) -> tuple[str, tuple[str, ...]]:
        """Validate a payload and return the management SQL with bound values."""
        if payload.variables.keys() - self._variable_keys.keys():
            msg = "Tenant payload contains an unmapped session variable"
            raise TenantContextError(msg)
        pairs = [(self._tenant_key, payload.tenant_id)]
        pairs.extend(
            (self._variable_keys[key], payload.variables[key])
            for key in self._sorted_variable_keys
            if key in payload.variables
        )
        if self._schema_resolver is not None:
            schema = self._schema_resolver(payload)
            if not isinstance(schema, str) or re.fullmatch(r"[a-z_][a-z0-9_]*", schema) is None:
                msg = "Tenant schema must match [a-z_][a-z0-9_]*"
                raise TenantContextError(msg)
            pairs.append(("search_path", quote_identifier(schema)))
        sql = (
            self._default_checkout_sql
            if len(payload.variables) == len(self._variable_keys)
            else build_setting_query(len(pairs))
        )
        return sql, tuple(part for pair in pairs for part in pair)

    def capture(
        self, payload: TenantContextPayload, params: Sequence[str], row: Sequence[str | None] | None
    ) -> TenantLeaseState:
        """Verify all applied values and capture the immutable previous settings."""
        if row is None or len(row) != len(params) or any(row[i + 1] != params[i + 1] for i in range(0, len(params), 2)):
            msg = "Tenant session checkout could not be verified"
            raise TenantContextError(msg)
        return TenantLeaseState(payload, {params[i]: row[i] for i in range(0, len(params), 2)})

    def prepare_checkin(self, state: TenantLeaseState) -> tuple[str, tuple[str, ...]]:
        """Prepare restoration of only the settings captured by this lease."""
        pairs = [
            (
                key,
                value
                if value is not None
                else (DEFAULT_SEARCH_PATH_RESTORE if key.casefold() == "search_path" else self._unset_restore_value),
            )
            for key, value in state.baseline.items()
        ]
        return build_setting_query(len(pairs), restore=True), tuple(part for pair in pairs for part in pair)

    def verify_checkin(self, params: Sequence[str], row: Sequence[str | None] | None) -> None:
        """Require actual session read-back after the materialized restoration."""
        if (
            row is None
            or len(row) != len(params)
            or any((row[i + 1] or "") != params[i + 1] for i in range(0, len(params), 2))
        ):
            msg = "Tenant session restoration could not be verified"
            raise TenantCleanupError(msg)
