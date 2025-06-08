# Type System

## Overview

SQLSpec's type system is a cornerstone of its design, providing compile-time safety, excellent IDE support, and runtime guarantees. Built on Python's advanced typing features including Protocols, Generics, and TypeVars, it ensures type information flows seamlessly through the entire stack - from configuration to query execution to result handling.

## Core Type Philosophy

### Type Information Never Lost

One of SQLSpec's key principles is that type information should never be lost as data flows through the system:

```mermaid
graph LR
    A[User Code<br/>schema_type=User] --> B[SQL Execution<br/>Generic[User]]
    B --> C[Result Processing<br/>SQLResult[User]]
    C --> D[User Consumption<br/>User instance]

    style A fill:#f9f,stroke:#333,stroke-width:4px
    style D fill:#f9f,stroke:#333,stroke-width:4px
```

### Generic Type Variables

SQLSpec uses several key type variables throughout:

```python
from typing import TypeVar, Protocol, Generic

# Connection type for each database
ConnectionT = TypeVar("ConnectionT", contravariant=True)

# Driver type for each adapter
DriverT = TypeVar("DriverT", covariant=True)

# Row type for results (dict, tuple, model)
RowT = TypeVar("RowT", bound=Union[dict, tuple, BaseModel])

# Schema type for automatic conversion
SchemaT = TypeVar("SchemaT", bound=BaseModel)

# Parameter type for SQL execution
ParamT = TypeVar("ParamT", bound=Union[dict, tuple, list])
```

## Protocol-Based Design

### Driver Protocol

The driver protocol defines the contract all database adapters must implement:

```python
from typing import Protocol, Optional, Any

class SyncDriverProtocol(Protocol[ConnectionT, RowT]):
    """Protocol for synchronous database drivers."""

    @property
    def instrumentation_config(self) -> InstrumentationConfig: ...

    def execute(
        self,
        sql: Union[str, SQL],
        parameters: Optional[Any] = None,
        *,
        schema_type: Optional[type[SchemaT]] = None,
        **kwargs: Any
    ) -> SQLResult[Union[SchemaT, RowT]]: ...

    def execute_many(
        self,
        sql: Union[str, SQL],
        parameters: list[Any],
        **kwargs: Any
    ) -> SQLResult[RowT]: ...

    def fetch_arrow_table(
        self,
        query: str
    ) -> pa.Table: ...
```

### Configuration Protocol

Configuration protocols ensure type-safe config handling:

```python
class DatabaseConfig(Protocol[ConnectionT, PoolT, DriverT]):
    """Protocol for database configurations."""

    # Direct field access for all configuration
    dsn: str
    min_size: int
    max_size: int

    @property
    def connection_config_dict(self) -> dict[str, Any]: ...

    def provide_connection(self) -> ContextManager[ConnectionT]: ...

    def provide_session(self) -> ContextManager[DriverT]: ...

    @property
    def pool_config_dict(self) -> dict[str, Any]: ...

    def provide_pool(self) -> ContextManager[PoolT]: ...
```

## Result Type System

### Generic Result Container

The `SQLResult` class maintains type information from execution to consumption:

```python
from typing import Generic, Optional, Sequence

@dataclass
class SQLResult(Generic[RowT]):
    """Type-safe container for SQL execution results."""

    statement: SQL
    data: Optional[Sequence[RowT]]
    rows_affected: int
    column_names: list[str]
    operation_type: str

    def one(self) -> RowT:
        """Get exactly one row, raise if not exactly one."""
        if not self.data or len(self.data) != 1:
            raise UnexpectedResultError(f"Expected 1 row, got {len(self.data)}")
        return self.data[0]

    def one_or_none(self) -> Optional[RowT]:
        """Get one row or None if no results."""
        if not self.data:
            return None
        if len(self.data) > 1:
            raise UnexpectedResultError(f"Expected 0-1 rows, got {len(self.data)}")
        return self.data[0]

    def all(self) -> Sequence[RowT]:
        """Get all rows."""
        return self.data or []

    def scalar(self) -> Any:
        """Get first column of first row."""
        row = self.one()
        if isinstance(row, dict):
            return next(iter(row.values()))
        return row[0]
```

### Schema Type Conversion

Automatic conversion to user-defined types:

```python
from pydantic import BaseModel
from dataclasses import dataclass

# Define schema
@dataclass
class User:
    id: int
    name: str
    email: str
    active: bool

# Execute with schema type
result = session.execute(
    "SELECT * FROM users WHERE id = ?",
    (1,),
    schema_type=User  # Type parameter here
)

# Result is fully typed
user: User = result.one()  # Type checker knows this is User
print(user.name)  # IDE autocomplete works!
```

## Type Flow Examples

### Complete Type Flow

Here's how types flow through a complete operation:

```python
from typing import Optional
from dataclasses import dataclass

@dataclass
class Order:
    id: int
    user_id: int
    total: float
    status: str

class OrderRepository:
    def __init__(self, spec: SQLSpec):
        self.spec = spec

    def find_by_id(self, order_id: int) -> Optional[Order]:
        with self.spec.provide_session("main") as session:
            # Type flows: int → SQL → SQLResult[Order] → Optional[Order]
            result: SQLResult[Order] = session.execute(
                "SELECT * FROM orders WHERE id = ?",
                (order_id,),
                schema_type=Order
            )
            return result.one_or_none()

    def find_by_user(self, user_id: int) -> list[Order]:
        with self.spec.provide_session("main") as session:
            # Type flows: int → SQL → SQLResult[Order] → list[Order]
            result: SQLResult[Order] = session.execute(
                "SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
                schema_type=Order
            )
            return list(result.all())
```

### Builder Type Safety

Query builders maintain type safety:

```python
from sqlspec import sql

# Builder knows about types
query = (
    sql.select("id", "name", "email")
    .from_("users")
    .where("active", "=", True)  # Type checked!
    .where("age", ">", 18)       # Type checked!
    .order_by("created_at", "DESC")
)

# Execute with type safety
result: SQLResult[dict] = session.execute(query)
```

## Advanced Type Patterns

### Overloaded Methods

SQLSpec uses overloading for better type inference:

```python
from typing import overload, Literal

class Driver:
    @overload
    def execute(
        self,
        sql: str,
        parameters: None = None,
        *,
        schema_type: None = None
    ) -> SQLResult[dict]: ...

    @overload
    def execute(
        self,
        sql: str,
        parameters: Any,
        *,
        schema_type: type[SchemaT]
    ) -> SQLResult[SchemaT]: ...

    def execute(self, sql, parameters=None, *, schema_type=None):
        # Implementation that returns appropriate type
        ...
```

### Conditional Types

Type system handles conditional returns:

```python
from typing import Union, Literal

class ResultSet(Generic[RowT]):
    def fetch(
        self,
        mode: Literal["one", "all", "scalar"]
    ) -> Union[RowT, list[RowT], Any]:
        if mode == "one":
            return self.one()  # Returns RowT
        elif mode == "all":
            return self.all()  # Returns list[RowT]
        else:
            return self.scalar()  # Returns Any
```

### Type Guards

Custom type guards for runtime checking:

```python
from typing import TypeGuard

def is_select_result(result: SQLResult[Any]) -> TypeGuard[SQLResult[dict]]:
    """Check if result is from a SELECT query."""
    return result.operation_type == "SELECT" and result.data is not None

# Usage
result = session.execute(sql)
if is_select_result(result):
    # Type checker knows result.data is not None and contains dicts
    for row in result.data:
        print(row["column"])  # Safe!
```

## Type Constraints

### Bounded Type Variables

Type variables with constraints ensure safety:

```python
# Row types must be dict-like or tuple-like
RowT = TypeVar("RowT", bound=Union[Mapping[str, Any], Sequence[Any]])

# Schema types must be convertible
SchemaT = TypeVar("SchemaT", bound=Union[BaseModel, DataclassProtocol])

# Connection types must be context managers
ConnectionT = TypeVar("ConnectionT", bound=ContextManager[Any])
```

### Variance

Proper variance for type safety:

```python
# Contravariant for inputs
InputT = TypeVar("InputT", contravariant=True)

# Covariant for outputs
OutputT = TypeVar("OutputT", covariant=True)

class Processor(Generic[InputT, OutputT]):
    def process(self, input: InputT) -> OutputT: ...
```

## Type Validation

### Runtime Type Checking

While Python's type system is mostly compile-time, SQLSpec validates critical types at runtime:

```python
from typing import get_type_hints, get_origin, get_args

def validate_schema_type(schema_type: type) -> None:
    """Validate that schema type is properly structured."""
    hints = get_type_hints(schema_type)

    for field_name, field_type in hints.items():
        origin = get_origin(field_type)

        # Check for unsupported types
        if origin is list:
            args = get_args(field_type)
            if not args:
                raise TypeError(f"List field {field_name} must specify element type")
```

### Type Coercion

Safe type coercion with validation:

```python
from typing import Any, Type, cast

def coerce_to_type(value: Any, target_type: Type[T]) -> T:
    """Safely coerce value to target type."""
    if isinstance(value, target_type):
        return value

    if target_type is int:
        return cast(T, int(value))
    elif target_type is str:
        return cast(T, str(value))
    elif target_type is bool:
        return cast(T, bool(value))
    else:
        raise TypeError(f"Cannot coerce {type(value)} to {target_type}")
```

## IDE Integration

### Type Stubs

SQLSpec provides comprehensive type stubs:

```python
# sqlspec/__init__.pyi
from typing import TypeVar, Generic, Optional

_T = TypeVar("_T")

class SQLSpec:
    def get_session(self, name: str) -> ContextManager[Session[_T]]: ...

class Session(Generic[_T]):
    def execute(
        self,
        sql: str,
        params: Optional[Any] = None,
        *,
        schema_type: Optional[type[_T]] = None
    ) -> SQLResult[_T]: ...
```

### Type Comments

For Python 3.8 compatibility:

```python
# Type comment syntax
result = session.execute(sql)  # type: SQLResult[User]

# Variable annotation
result: SQLResult[User] = session.execute(sql, schema_type=User)
```

## Best Practices

### 1. Always Specify Schema Types

```python
# Good - full type safety
result = session.execute(sql, schema_type=User)
user: User = result.one()

# Bad - loses type information
result = session.execute(sql)
user = result.one()  # Type is Any
```

### 2. Use Type Aliases

```python
from typing import TypeAlias

# Define domain-specific types
UserId: TypeAlias = int
OrderId: TypeAlias = int
Money: TypeAlias = Decimal

@dataclass
class Order:
    id: OrderId
    user_id: UserId
    total: Money
```

### 3. Leverage Protocol Inheritance

```python
class Timestamped(Protocol):
    created_at: datetime
    updated_at: datetime

class SoftDeletable(Protocol):
    deleted_at: Optional[datetime]

class User(Timestamped, SoftDeletable):
    id: int
    name: str
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime]
```

## Performance Considerations

### Type Checking Overhead

Type checking is primarily compile-time:

```python
# No runtime overhead
def get_user(id: int) -> User:
    ...

# Minimal runtime overhead
def get_user(id: int) -> User:
    assert isinstance(id, int)  # Only in debug mode
    ...
```

### Generic Instantiation

Generic types have minimal overhead:

```python
# These are equivalent at runtime
result1: SQLResult[dict] = SQLResult(...)
result2 = SQLResult(...)  # type: SQLResult[dict]
```

## Next Steps

- [Driver Architecture](../drivers/05-driver-architecture.md) - How types flow through drivers
- [Pipeline Overview](../pipeline/08-pipeline-overview.md) - Type preservation in pipelines
- [Security Architecture](../security/12-security-architecture.md) - Type safety as security

---

[← Configuration Architecture](./03-configuration-architecture.md) | [Driver Architecture →](../drivers/05-driver-architecture.md)
