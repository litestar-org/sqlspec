from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)


def test_new_exception_hierarchy() -> None:
    """Test new exception classes inherit correctly."""
    assert issubclass(UniqueViolationError, IntegrityError)
    assert issubclass(ForeignKeyViolationError, IntegrityError)
    assert issubclass(CheckViolationError, IntegrityError)
    assert issubclass(NotNullViolationError, IntegrityError)

    assert issubclass(DatabaseConnectionError, SQLSpecError)
    assert issubclass(TransactionError, SQLSpecError)
    assert issubclass(DataError, SQLSpecError)
    assert issubclass(OperationalError, SQLSpecError)


def test_exception_instantiation() -> None:
    """Test exceptions can be instantiated with messages."""
    exc = UniqueViolationError("Duplicate key")
    assert str(exc) == "Duplicate key"
    assert isinstance(exc, Exception)


def test_exception_chaining() -> None:
    """Test exceptions support chaining with 'from'."""
    try:
        try:
            raise ValueError("Original error")
        except ValueError as e:
            raise UniqueViolationError("Mapped error") from e
    except UniqueViolationError as exc:
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, ValueError)
