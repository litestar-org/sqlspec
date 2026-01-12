from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    SQLSpecError,
    StackExecutionError,
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


def test_stack_execution_error_includes_context() -> None:
    base = StackExecutionError(
        2,
        "SELECT * FROM users",
        ValueError("boom"),
        adapter="asyncpg",
        mode="continue-on-error",
        native_pipeline=False,
        downgrade_reason="operator_override",
    )

    detail = str(base)
    assert "operation 2" in detail
    assert "asyncpg" in detail
    assert "pipeline=disabled" in detail
    assert "boom" in detail


def test_sqlspec_error_single_arg_populates_args() -> None:
    """Test single positional arg is stored in both args and detail."""
    exc = SQLSpecError("test message")
    assert exc.args == ("test message",)
    assert exc.detail == "test message"


def test_sqlspec_error_multiple_args_all_preserved() -> None:
    """Test multiple args are all stored in args tuple."""
    exc = SQLSpecError("first", "second", "third")
    assert exc.args == ("first", "second", "third")
    assert exc.detail == "first"


def test_sqlspec_error_explicit_detail_separate() -> None:
    """Test explicit detail kwarg is separate from args."""
    exc = SQLSpecError("arg1", detail="explicit detail")
    assert exc.args == ("explicit detail", "arg1")
    assert exc.detail == "explicit detail"


def test_sqlspec_error_detail_only() -> None:
    """Test detail-only initialization."""
    exc = SQLSpecError(detail="detail only")
    assert exc.args == ("detail only",)
    assert exc.detail == "detail only"


def test_sqlspec_error_empty() -> None:
    """Test empty initialization."""
    exc = SQLSpecError()
    assert exc.args == ()
    assert exc.detail == ""


def test_sqlspec_error_str_no_duplication() -> None:
    """Test str() does not duplicate when detail matches args[0]."""
    exc = SQLSpecError("message")
    assert str(exc) == "message"


def test_sqlspec_error_str_multiple_args() -> None:
    """Test str() joins multiple args correctly."""
    exc = SQLSpecError("first", "second")
    assert str(exc) == "first second"


def test_sqlspec_error_str_explicit_detail_appended() -> None:
    """Test str() appends explicit detail that differs from args."""
    exc = SQLSpecError("arg", detail="extra")
    assert str(exc) == "extra arg"


def test_sqlspec_error_str_detail_only() -> None:
    """Test str() with only detail."""
    exc = SQLSpecError(detail="only detail")
    assert str(exc) == "only detail"


def test_sqlspec_error_str_empty() -> None:
    """Test str() with no args or detail."""
    exc = SQLSpecError()
    assert str(exc) == ""


def test_sqlspec_error_standard_exception_compatibility() -> None:
    """Test standard Python exception patterns work."""
    exc = SQLSpecError("error message")

    assert exc.args[0] == "error message"

    (message,) = exc.args
    assert message == "error message"

    assert bool(exc.args)


def test_subclass_inherits_args_behavior() -> None:
    """Test subclasses inherit correct args behavior."""
    exc = NotFoundError("item not found")
    assert exc.args == ("item not found",)
    assert exc.detail == "item not found"
    assert str(exc) == "item not found"


def test_stack_execution_error_preserves_args() -> None:
    """Test StackExecutionError correctly populates args."""
    original = ValueError("boom")
    exc = StackExecutionError(operation_index=2, sql="SELECT * FROM users", original_error=original, adapter="asyncpg")
    assert len(exc.args) == 1
    assert "operation 2" in exc.args[0]
    assert exc.args[0] == exc.detail
