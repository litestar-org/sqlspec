from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)


def test_new_exception_hierarchy():
    """Test new exception classes inherit correctly."""
    assert issubclass(UniqueViolationError, IntegrityError)
    assert issubclass(ForeignKeyViolationError, IntegrityError)
    assert issubclass(CheckViolationError, IntegrityError)
    assert issubclass(NotNullViolationError, IntegrityError)

    assert issubclass(ConnectionError, SQLSpecError)
    assert issubclass(TransactionError, SQLSpecError)
    assert issubclass(DataError, SQLSpecError)
    assert issubclass(OperationalError, SQLSpecError)


def test_exception_instantiation():
    """Test exceptions can be instantiated with messages."""
    exc = UniqueViolationError("Duplicate key")
    assert str(exc) == "Duplicate key"
    assert isinstance(exc, Exception)


def test_exception_chaining():
    """Test exceptions support chaining with 'from'."""
    try:
        try:
            raise ValueError("Original error")
        except ValueError as e:
            raise UniqueViolationError("Mapped error") from e
    except UniqueViolationError as exc:
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, ValueError)
