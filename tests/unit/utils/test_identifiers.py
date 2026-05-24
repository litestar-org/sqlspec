"""Tests for SQL identifier validation helpers."""

import pytest

from sqlspec.utils.identifiers import DEFAULT_MAX_IDENTIFIER_LENGTH, validate_identifier


def test_validate_identifier_returns_valid_name_unchanged() -> None:
    """Valid identifiers are returned unchanged."""
    assert validate_identifier("adk_session") == "adk_session"


@pytest.mark.parametrize("name", ["", "1_table", "table-name", "table name", "foo; DROP TABLE x"])
def test_validate_identifier_rejects_invalid_names(name: str) -> None:
    """Invalid identifiers are rejected."""
    with pytest.raises(ValueError):
        validate_identifier(name)


def test_validate_identifier_rejects_names_longer_than_default_limit() -> None:
    """Identifiers longer than the default max length are rejected."""
    name = "a" * (DEFAULT_MAX_IDENTIFIER_LENGTH + 1)

    with pytest.raises(ValueError, match="Identifier too long"):
        validate_identifier(name)


def test_validate_identifier_rejects_schema_qualifier_by_default() -> None:
    """Schema-qualified names are rejected unless explicitly enabled."""
    with pytest.raises(ValueError, match="Schema qualifier not allowed"):
        validate_identifier("public.adk_session")


def test_validate_identifier_accepts_schema_qualified_name_when_enabled() -> None:
    """Schema-qualified names are validated segment by segment when enabled."""
    assert validate_identifier("public.adk_session", allow_schema_qualifier=True) == "public.adk_session"


def test_validate_identifier_accepts_multi_segment_qualified_name_when_enabled() -> None:
    """Existing event queue behavior accepts multi-segment qualified names."""
    name = "catalog.public.adk_event"

    assert validate_identifier(name, allow_schema_qualifier=True) == name


@pytest.mark.parametrize("name", [".adk_session", "public.", "public..adk_session", "public.1_sessions"])
def test_validate_identifier_rejects_invalid_schema_qualified_segments(name: str) -> None:
    """Every schema-qualified segment must be a valid identifier."""
    with pytest.raises(ValueError, match="Invalid identifier"):
        validate_identifier(name, allow_schema_qualifier=True)


def test_validate_identifier_uses_custom_error_class() -> None:
    """Callers can preserve their existing exception type."""

    class IdentifierError(Exception):
        pass

    with pytest.raises(IdentifierError):
        validate_identifier("invalid-name", error_cls=IdentifierError)


def test_validate_identifier_uses_custom_label_in_error_messages() -> None:
    """Callers can preserve existing domain-specific error messages."""
    with pytest.raises(ValueError, match="Invalid table name"):
        validate_identifier("invalid-name", label="table name")
