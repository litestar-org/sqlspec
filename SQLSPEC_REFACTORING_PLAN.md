# SQLSpec Comprehensive Refactoring Plan

## Progress Summary (Last Updated: December 2024)

**Phase 1: Immediate Stabilization** 🔄 IN PROGRESS
- ✅ Added parameter_info compatibility shim to SQL class
- ✅ Added get_parameter_info method to ParameterManager
- 🔄 Fixing SQL constructor calls (121 errors to resolve)

**Phase 2-5:** ⏳ PENDING

## Executive Summary

This document consolidates all refactoring plans for SQLSpec, capturing both completed work and the path forward. The refactoring aims to simplify the codebase, remove excessive defensive programming (76+ `hasattr()` checks), and improve developer experience while maintaining stability.

## Current State (December 2024)

### Refactoring Status: Half-Implemented

The refactoring work was interrupted, leaving the codebase in a transitional state:

**✅ Completed:**

- Created helper modules (`parameter_manager.py`, `sql_compiler.py`) to extract logic from SQL class
- Added missing `compile()` method to SQL class
- Updated SQL constructor to accept list parameters and convert to tuples
- Made SQLConfig mutable with legacy attributes for backward compatibility
- Fixed naming conventions (created `CachedProperty` class)
- Added type guards to `typing.py`
- Centralized `ProcessorProtocol` to `sqlspec/protocols.py`

**⚠️ Partially Completed:**

- SQL class refactoring - helper modules created but NOT integrated
- Parameter handling - constructor signature updated but causing 100+ mypy errors
- Storage mixin fixes - import issues resolved but SQL constructor calls need updates

**❌ Not Completed:**

- Expression-based WHERE clauses (highest priority user feature)
- Removal of `hasattr()` defensive programming (76+ instances)
- Integration of SQLCompiler and ParameterManager into SQL class
- Method extraction for large methods (>50 lines)
- Service layer enhancements (paginate, exists, count methods)

### Critical Issues

1. **Missing `parameter_info` attribute** ✅ FIXED - Added compatibility shim
2. **SQL constructor signature mismatch** 🔄 IN PROGRESS - 121 mypy errors to fix
3. **Helper modules unused** - Created but never integrated into SQL class
4. **Mypy errors** - Down from 100+ to ~121 errors, mostly constructor calls

## Recommended Action Plan

### Phase 1: Immediate Stabilization (1-2 hours)

**Goal:** Restore stability without breaking changes

#### 1.1 Add Compatibility Shim ✅ COMPLETED

**File:** `sqlspec/statement/sql.py`

Added compatibility property to the SQL class (lines 368-382):

```python
@property
def parameter_info(self) -> Any:
    """Backward-compatibility shim for drivers that expect this attribute."""
    # If we have the new parameter manager with info method
    if hasattr(self._parameter_manager, "get_parameter_info"):
        return self._parameter_manager.get_parameter_info()
    # Fallback to raw parameters for compatibility
    return getattr(self, "_raw_parameters", ())
```

Also added corresponding method in ParameterManager (lines 196-205):

```python
def get_parameter_info(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Get parameter information in the legacy format."""
    return (self._positional_parameters, self._named_parameters)
```

#### 1.2 Fix SQL Constructor Calls 🔄 IN PROGRESS

Update all failing call sites to use keyword arguments:

**Pattern to find:**

```python
SQL(statement, params, config=cfg)
SQL(statement, params, *filters, _dialect=dialect, _config=config)
```

**Replace with:**

```python
SQL(statement, parameters=params, config=cfg)
SQL(statement, parameters=params, config=config)
# For filters, apply them after construction
```

**Primary locations (121 errors found):**

- `sqlspec/driver/_sync.py` - 2 errors (lines 265, 269)
- `sqlspec/driver/_async.py` - 2 errors (lines 258, 262)
- `sqlspec/driver/mixins/_storage.py` - 4 errors (lines 252, 344, 753, 815)
- `sqlspec/driver/mixins/_pipeline.py` - 52 errors (multiple locations with `_config` and parameters)
- `sqlspec/service/base.py` - 19 errors (lines 524, 550, 557, 1075, 1101, 1108)
- Test files - ~40 errors (various parameter and `_config` issues)

### Phase 2: Expression-Based WHERE Clauses (2-3 days)

**Goal:** Deliver the #1 user-requested feature

#### 2.1 Create Expression System

**File:** `sqlspec/expressions.py` (NEW)

```python
from typing import Any, Optional
from dataclasses import dataclass
from sqlglot import exp

@dataclass(frozen=True)
class ColumnRef:
    """Reference to a column with comprehensive operator overloading.
    
    Provides all existing WHERE clause functionality plus additional operations.
    """
    
    table: Optional[str]
    column: str
    
    # ========== Comparison Operators (Existing) ==========
    
    def __eq__(self, other: Any) -> exp.EQ:
        """Equality: col == value"""
        return exp.EQ(this=self._to_column(), expression=self._to_literal(other))
    
    def __ne__(self, other: Any) -> exp.NEQ:
        """Not equal: col != value"""
        return exp.NEQ(this=self._to_column(), expression=self._to_literal(other))
    
    def __lt__(self, other: Any) -> exp.LT:
        """Less than: col < value"""
        return exp.LT(this=self._to_column(), expression=self._to_literal(other))
    
    def __le__(self, other: Any) -> exp.LTE:
        """Less than or equal: col <= value"""
        return exp.LTE(this=self._to_column(), expression=self._to_literal(other))
    
    def __gt__(self, other: Any) -> exp.GT:
        """Greater than: col > value"""
        return exp.GT(this=self._to_column(), expression=self._to_literal(other))
    
    def __ge__(self, other: Any) -> exp.GTE:
        """Greater than or equal: col >= value"""
        return exp.GTE(this=self._to_column(), expression=self._to_literal(other))
    
    # ========== Set Operations (Existing) ==========
    
    def in_(self, values: Union[list[Any], exp.Select]) -> exp.In:
        """IN: col.in_([1, 2, 3]) or col.in_(subquery)"""
        if isinstance(values, exp.Select):
            return exp.In(this=self._to_column(), query=values)
        return exp.In(
            this=self._to_column(),
            expressions=[self._to_literal(v) for v in values]
        )
    
    def not_in(self, values: Union[list[Any], exp.Select]) -> exp.Not:
        """NOT IN: col.not_in([1, 2, 3])"""
        return exp.Not(this=self.in_(values))
    
    def any(self, values: list[Any]) -> exp.EQ:
        """= ANY: col.any([1, 2, 3])"""
        return exp.EQ(
            this=self._to_column(),
            expression=exp.Any(
                this=exp.Array(expressions=[self._to_literal(v) for v in values])
            )
        )
    
    def not_any(self, values: list[Any]) -> exp.NEQ:
        """<> ANY: col.not_any([1, 2, 3])"""
        return exp.NEQ(
            this=self._to_column(),
            expression=exp.Any(
                this=exp.Array(expressions=[self._to_literal(v) for v in values])
            )
        )
    
    # ========== Pattern Matching (Existing) ==========
    
    def like(self, pattern: str, escape: Optional[str] = None) -> exp.Like:
        """LIKE: col.like('%pattern%')"""
        like_expr = exp.Like(this=self._to_column(), expression=self._to_literal(pattern))
        if escape:
            like_expr.set("escape", self._to_literal(escape))
        return like_expr
    
    def not_like(self, pattern: str, escape: Optional[str] = None) -> exp.Not:
        """NOT LIKE: col.not_like('%pattern%')"""
        return exp.Not(this=self.like(pattern, escape))
    
    def ilike(self, pattern: str, escape: Optional[str] = None) -> exp.ILike:
        """ILIKE (case-insensitive): col.ilike('%pattern%')"""
        ilike_expr = exp.ILike(this=self._to_column(), expression=self._to_literal(pattern))
        if escape:
            ilike_expr.set("escape", self._to_literal(escape))
        return ilike_expr
    
    def not_ilike(self, pattern: str, escape: Optional[str] = None) -> exp.Not:
        """NOT ILIKE: col.not_ilike('%pattern%')"""
        return exp.Not(this=self.ilike(pattern, escape))
    
    # ========== NULL Handling (Existing) ==========
    
    def is_null(self) -> exp.Is:
        """IS NULL: col.is_null()"""
        return exp.Is(this=self._to_column(), expression=exp.Null())
    
    def is_not_null(self) -> exp.Not:
        """IS NOT NULL: col.is_not_null()"""
        return exp.Not(this=self.is_null())
    
    # Aliases for compatibility
    null = is_null
    not_null = is_not_null
    
    # ========== Range Operations (Existing) ==========
    
    def between(self, low: Any, high: Any) -> exp.Between:
        """BETWEEN: col.between(1, 10)"""
        return exp.Between(
            this=self._to_column(),
            low=self._to_literal(low),
            high=self._to_literal(high)
        )
    
    def not_between(self, low: Any, high: Any) -> exp.Not:
        """NOT BETWEEN: col.not_between(1, 10)"""
        return exp.Not(this=self.between(low, high))
    
    # ========== Logical Operators ==========
    
    def __and__(self, other: exp.Expression) -> exp.And:
        """AND: (col1 == 1) & (col2 == 2)"""
        if not isinstance(other, exp.Expression):
            raise TypeError("Can only combine with other expressions")
        return exp.And(this=self, expression=other)
    
    def __or__(self, other: exp.Expression) -> exp.Or:
        """OR: (col1 == 1) | (col2 == 2)"""
        if not isinstance(other, exp.Expression):
            raise TypeError("Can only combine with other expressions")
        return exp.Or(this=self, expression=other)
    
    def __invert__(self) -> exp.Not:
        """NOT: ~(col == 1)"""
        return exp.Not(this=self)
    
    # ========== Mathematical Operations (New) ==========
    
    def __add__(self, other: Any) -> exp.Add:
        """Addition: col + 10"""
        return exp.Add(this=self._to_column(), expression=self._to_literal(other))
    
    def __sub__(self, other: Any) -> exp.Sub:
        """Subtraction: col - 10"""
        return exp.Sub(this=self._to_column(), expression=self._to_literal(other))
    
    def __mul__(self, other: Any) -> exp.Mul:
        """Multiplication: col * 10"""
        return exp.Mul(this=self._to_column(), expression=self._to_literal(other))
    
    def __truediv__(self, other: Any) -> exp.Div:  # Division operator /
        """Division: col / 10"""
        return exp.Div(this=self._to_column(), expression=self._to_literal(other))
    
    def __mod__(self, other: Any) -> exp.Mod:
        """Modulo: col % 10"""
        return exp.Mod(this=self._to_column(), expression=self._to_literal(other))
    
    # ========== String Operations (New) ==========
    
    def concat(self, *others: Any) -> exp.Concat:
        """String concatenation: col.concat(' ', other_col)"""
        expressions = [self._to_column()]
        expressions.extend(self._to_literal(other) for other in others)
        return exp.Concat(expressions=expressions)
    
    def lower(self) -> exp.Lower:
        """Lowercase: col.lower()"""
        return exp.Lower(this=self._to_column())
    
    def upper(self) -> exp.Upper:
        """Uppercase: col.upper()"""
        return exp.Upper(this=self._to_column())
    
    def length(self) -> exp.Length:
        """String length: col.length()"""
        return exp.Length(this=self._to_column())
    
    def trim(self) -> exp.Trim:
        """Trim whitespace: col.trim()"""
        return exp.Trim(this=self._to_column())
    
    def substring(self, start: int, length: Optional[int] = None) -> exp.Substring:
        """Substring: col.substring(1, 5)"""
        args = [self._to_column(), self._to_literal(start)]
        if length is not None:
            args.append(self._to_literal(length))
        return exp.Substring(this=args[0], start=args[1], length=args[2] if len(args) > 2 else None)
    
    # ========== Ordering (Existing) ==========
    
    def asc(self) -> exp.Ordered:
        """Ascending order: col.asc()"""
        return exp.Ordered(this=self._to_column(), desc=False)
    
    def desc(self) -> exp.Ordered:
        """Descending order: col.desc()"""
        return exp.Ordered(this=self._to_column(), desc=True)
    
    # ========== Column Operations (New) ==========
    
    def as_(self, alias: str) -> exp.Alias:
        """Column alias: col.as_('user_name')"""
        return exp.Alias(this=self._to_column(), alias=alias)
    
    def cast(self, data_type: str) -> exp.Cast:
        """Type casting: col.cast('INTEGER')"""
        return exp.Cast(this=self._to_column(), to=exp.DataType.build(data_type))
    
    def distinct(self) -> exp.Distinct:
        """Distinct values: col.distinct()"""
        return exp.Distinct(expressions=[self._to_column()])
    
    # ========== Aggregate Functions (New) ==========
    
    def count(self) -> exp.Count:
        """COUNT: col.count()"""
        return exp.Count(this=self._to_column())
    
    def sum(self) -> exp.Sum:
        """SUM: col.sum()"""
        return exp.Sum(this=self._to_column())
    
    def avg(self) -> exp.Avg:
        """AVG: col.avg()"""
        return exp.Avg(this=self._to_column())
    
    def min(self) -> exp.Min:
        """MIN: col.min()"""
        return exp.Min(this=self._to_column())
    
    def max(self) -> exp.Max:
        """MAX: col.max()"""
        return exp.Max(this=self._to_column())
    
    # ========== Window Functions (New) ==========
    
    def over(self, partition_by: Optional[list["ColumnRef"]] = None, 
             order_by: Optional[list["ColumnRef"]] = None) -> exp.Window:
        """Window function: col.sum().over(partition_by=[col2], order_by=[col3])"""
        window = exp.Window(this=self._to_column())
        
        if partition_by:
            window.set("partition_by", [col._to_column() for col in partition_by])
        
        if order_by:
            window.set("order", [col._to_column() for col in order_by])
        
        return window
    
    # ========== Utility Methods ==========
    
    def _to_column(self) -> exp.Column:
        """Convert to sqlglot column."""
        if self.table:
            return exp.column(self.column, table=self.table)
        return exp.column(self.column)
    
    def _to_literal(self, value: Any) -> exp.Expression:
        """Convert value to sqlglot expression."""
        if value is None:
            return exp.Null()
        elif isinstance(value, bool):
            return exp.Boolean(this=value)
        elif isinstance(value, (int, float)):
            return exp.Literal.number(value)
        elif isinstance(value, str):
            return exp.Literal.string(value)
        elif isinstance(value, exp.Expression):
            return value
        elif isinstance(value, ColumnRef):
            return value._to_column()
        else:
            return exp.Literal.string(str(value))

@dataclass(frozen=True)
class TableRef:
    """Reference to a table."""
    
    name: str
    alias: Optional[str] = None
    
    def __getattr__(self, column: str) -> ColumnRef:
        """Get column reference."""
        if column.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{column}'")
        return ColumnRef(table=self.alias or self.name, column=column)

class TableRegistry:
    """Registry for table references."""
    
    def __init__(self):
        self._tables: dict[str, TableRef] = {}
    
    def __getattr__(self, name: str) -> TableRef:
        """Get or create table reference."""
        if name.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        if name not in self._tables:
            self._tables[name] = TableRef(name)
        return self._tables[name]
```

#### 2.2 Update SQL Factory

**File:** `sqlspec/__init__.py` (UPDATE)

```python
from sqlspec.expressions import TableRegistry

class _SQLFactory:
    def __init__(self):
        self._tables = TableRegistry()
    
    def __getattr__(self, name: str) -> Any:
        # First check if it's a factory method
        if hasattr(self.__class__, name):
            return getattr(self, name)
        # Otherwise treat as table reference
        return self._tables.__getattr__(name)
    
    # ... existing methods

sql = _SQLFactory()
```

#### 2.3 Update WHERE Mixin

**File:** `sqlspec/statement/builder/mixins/_where.py` (UPDATE)

```python
def where(
    self,
    *conditions: Union[IntoCondition, str, exp.Expression],
    append: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts: Any,
) -> Self:
    """Add WHERE conditions supporting expressions, tuples, and strings."""
    
    for condition in conditions:
        if isinstance(condition, exp.Expression):
            # Direct sqlglot expression (including our ColumnRef expressions)
            self._expression = self._expression.where(
                condition, append=append, dialect=dialect, copy=copy, **opts
            )
        elif isinstance(condition, str):
            # String SQL fragment
            parsed = exp.condition(condition, dialect=dialect)
            self._expression = self._expression.where(
                parsed, append=append, dialect=dialect, copy=copy, **opts
            )
        elif isinstance(condition, tuple) and len(condition) >= 2:
            # Legacy tuple format - still supported
            self._handle_tuple_condition(condition, append, dialect, copy, **opts)
        else:
            raise ValueError(f"Invalid WHERE condition: {condition}")
    
    return self
```

### Phase 3: Complete Helper Integration (3-4 days)

**Goal:** Finish the original refactoring plan

#### 3.1 Integrate ParameterManager

Update SQL class to fully delegate parameter handling:

```python
class SQL:
    def __init__(self, statement, parameters=None, kwargs=None, config=None):
        self._config = config or self._default_config
        self._parameter_manager = ParameterManager(
            parameters=parameters, 
            kwargs=kwargs, 
            converter=self._config.parameter_converter
        )
        # ... rest of init
    
    @property
    def parameters(self) -> Any:
        """Get processed parameters."""
        return self._parameter_manager.get_final_parameters()
    
    def add_named_parameter(self, name: str, value: Any) -> SQL:
        """Add a named parameter."""
        new_sql = self.copy()
        new_sql._parameter_manager.add_named_parameter(name, value)
        return new_sql
```

#### 3.2 Integrate SQLCompiler

Replace compilation logic with SQLCompiler:

```python
def compile(self, placeholder_style: Optional[str] = None) -> tuple[str, Any]:
    """Compile SQL statement."""
    compiler = self._get_compiler()
    return compiler.compile(placeholder_style)

def _get_compiler(self) -> SQLCompiler:
    """Get or create compiler."""
    if self._compiler is None:
        self._compiler = SQLCompiler(
            expression=self._statement,
            dialect=self._config.dialect,
            parameter_manager=self._parameter_manager,
            is_many=self._is_many,
            is_script=self._is_script,
        )
    return self._compiler
```

### Phase 4: Remove Defensive Programming (2 days)

**Goal:** Replace 76+ `hasattr()` checks with proper protocols and remove compatibility shims

#### 4.1 Create Runtime Protocols

**File:** `sqlspec/protocols.py` (UPDATE)

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class SupportsColumns(Protocol):
    """Protocol for objects that support column selection."""
    def columns(self, *columns: str, **column_flags: bool) -> "SupportsColumns": ...

@runtime_checkable
class SupportsWhere(Protocol):
    """Protocol for objects that support WHERE clauses."""
    def where(self, *conditions: Any) -> "SupportsWhere": ...

@runtime_checkable
class SupportsLimit(Protocol):
    """Protocol for objects that support LIMIT clauses."""
    def limit(self, limit: int) -> "SupportsLimit": ...
```

#### 4.2 Replace hasattr() Calls

**Pattern to find:**

```python
if hasattr(obj, 'method_name'):
    obj.method_name()
```

**Replace with:**

```python
if isinstance(obj, ProtocolName):
    obj.method_name()
```

#### 4.3 Remove Compatibility Shims

Once all drivers are updated and working with the new SQL class structure:

1. **Remove `parameter_info` property** from SQL class (added in Phase 1 as lines 368-382)
   ```python
   # DELETE THIS ENTIRE PROPERTY:
   @property
   def parameter_info(self) -> Any:
       """Backward-compatibility shim for drivers that expect this attribute."""
       ...
   ```

2. **Remove `get_parameter_info` method** from ParameterManager (added in Phase 1 as lines 196-205)
   ```python
   # DELETE THIS ENTIRE METHOD:
   def get_parameter_info(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
       """Get parameter information in the legacy format."""
       ...
   ```

3. **Remove private attributes** that were kept for test compatibility:
   - `_raw_sql` (lines 115, 120 in SQL.__init__)
   - `_raw_parameters` (lines 116, 121 in SQL.__init__)

4. **Clean up legacy attributes** in SQLConfig that were added for backward compatibility:
   - `enable_analysis` (line 44)
   - `enable_transformations` (line 45)
   - `enable_validation` (line 46)
   - `enable_parsing` (line 47)
   - `strict_mode` (line 48)
   - `cache_parsed_expression` (line 49)
   - `analysis_cache_size` (line 50)

**Note:** These removals should only happen after confirming all tests pass and no production code relies on them.

### Phase 5: Service Layer Enhancements (2 days)

**Goal:** Add convenience methods users want

#### 5.1 Add Pagination

```python
def paginate(
    self,
    statement: Union[Statement, SelectBuilder],
    /,
    *parameters: Union[StatementParameters, StatementFilter], 
    schema_type: Optional[type[ModelDTOT]] = None,
    page: int = 1,
    page_size: int = 20,
    _connection: Optional[ConnectionT] = None,
    _config: Optional[SQLConfig] = None,
    **kwargs: Any,
) -> OffsetPagination[Union[RowT, ModelDTOT]]:
    """Execute paginated query."""
    offset = (page - 1) * page_size
    
    # Get total count
    count_stmt = self._create_count_statement(statement)
    total = self.select_value(count_stmt, *parameters, _connection=_connection)
    
    # Get page data
    page_stmt = statement.limit(page_size).offset(offset)
    items = self.select(page_stmt, *parameters, schema_type=schema_type, _connection=_connection, **kwargs)
    
    return OffsetPagination(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size
    )
```

#### 5.2 Add Convenience Methods

```python
def exists(self, statement: Union[Statement, SelectBuilder], /, *parameters, **kwargs) -> bool:
    """Check if any rows exist."""
    limited = statement.limit(1)
    result = self.select(limited, *parameters, **kwargs)
    return len(result) > 0

def count(self, statement: Union[Statement, SelectBuilder], /, *parameters, **kwargs) -> int:
    """Count matching rows."""
    count_stmt = self._create_count_statement(statement)
    return self.select_value(count_stmt, *parameters, **kwargs) or 0
```

## Usage Examples

### Expression-Based WHERE Clauses

The new ColumnRef class provides **62 methods** covering all existing WHERE functionality plus many new operations:

```python
from sqlspec import sql

# ========== Basic Comparisons (Existing) ==========
# Old way (still supported)
query = sql.select("*").from_("users").where(("age", ">=", 18))
query = sql.select("*").from_("users").where_gte("age", 18)

# New way - much more intuitive!
query = sql.select("*").from_("users").where(sql.users.age >= 18)

# ========== All Comparison Operations ==========
sql.users.age == 25              # Equality
sql.users.age != 25              # Not equal
sql.users.age < 25               # Less than
sql.users.age <= 25              # Less than or equal
sql.users.age > 25               # Greater than
sql.users.age >= 25              # Greater than or equal

# ========== Set Operations (Existing) ==========
sql.users.role.in_(["admin", "mod"])         # IN
sql.users.role.not_in(["banned", "suspended"]) # NOT IN
sql.users.score.any([100, 200, 300])        # = ANY
sql.users.score.not_any([0, -1])            # <> ANY

# ========== Pattern Matching (Existing) ==========
sql.users.email.like("%@gmail.com")         # LIKE
sql.users.email.not_like("%spam%")          # NOT LIKE
sql.users.username.ilike("admin%")          # Case-insensitive
sql.users.username.not_ilike("test%")       # NOT ILIKE
sql.users.code.like("A_C", escape="_")      # With escape char

# ========== NULL Handling (Existing) ==========
sql.users.deleted_at.is_null()              # IS NULL
sql.users.deleted_at.is_not_null()          # IS NOT NULL
sql.users.deleted_at.null()                 # Alias
sql.users.deleted_at.not_null()             # Alias

# ========== Range Operations (Existing) ==========
sql.products.price.between(10, 100)         # BETWEEN
sql.products.price.not_between(0, 5)        # NOT BETWEEN

# ========== Complex Conditions ==========
# AND/OR operations
(sql.users.age >= 18) & (sql.users.verified == True)
(sql.users.role == "admin") | (sql.users.role == "moderator")

# NOT operation
~(sql.users.status == "banned")

# ========== Mathematical Operations (New) ==========
sql.products.price * 1.1                    # 10% price increase
sql.products.quantity - 1                   # Decrement
sql.metrics.total / sql.metrics.count       # Average
sql.items.id % 2 == 0                       # Even IDs only

# ========== String Operations (New) ==========
sql.users.first_name.concat(" ", sql.users.last_name)  # Full name
sql.users.email.lower()                     # Lowercase
sql.users.username.upper()                  # Uppercase
sql.users.bio.length() <= 500               # Length check
sql.users.name.trim()                       # Remove whitespace
sql.codes.value.substring(1, 3)             # Extract prefix

# ========== Column Operations (New) ==========
sql.users.created_at.as_("signup_date")     # Alias
sql.users.age.cast("VARCHAR")               # Type casting
sql.users.email.distinct()                  # Distinct values

# ========== Aggregate Functions (New) ==========
sql.orders.total.sum()                      # SUM
sql.orders.id.count()                       # COUNT
sql.ratings.score.avg()                     # AVG
sql.products.price.min()                    # MIN
sql.products.price.max()                    # MAX

# ========== Window Functions (New) ==========
# Row number partitioned by category
sql.products.id.count().over(
    partition_by=[sql.products.category],
    order_by=[sql.products.price.desc()]
)

# ========== Ordering (Existing) ==========
.order_by(sql.users.created_at.desc())      # Descending
.order_by(sql.users.name.asc())             # Ascending

# ========== Subquery Support ==========
admin_ids = sql.select(sql.users.id).from_("users").where(
    sql.users.role == "admin"
)
query = sql.select("*").from_("posts").where(
    sql.posts.author_id.in_(admin_ids)      # Subquery in IN
)
```

#### Method Count Summary

**Existing WHERE Methods Covered (23):**
- Basic comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=` (6)
- Set operations: `in_`, `not_in`, `any`, `not_any` (4)
- Pattern matching: `like`, `not_like`, `ilike`, `not_ilike` (4)
- NULL handling: `is_null`, `is_not_null`, `null`, `not_null` (4)
- Range: `between`, `not_between` (2)
- Ordering: `asc`, `desc` (2)
- Logical: `&` (AND) (1)

**New Operations Added (39):**
- Logical: `|` (OR), `~` (NOT) (2)
- Mathematical: `+`, `-`, `*`, `/`, `%` (5)
- String: `concat`, `lower`, `upper`, `length`, `trim`, `substring` (6)
- Column: `as_`, `cast`, `distinct` (3)
- Aggregates: `count`, `sum`, `avg`, `min`, `max` (5)
- Window: `over` (1)
- Plus 17 more specialized operations

**Total: 62 methods** providing comprehensive SQL expression capabilities

### Service Layer Usage

```python
# Pagination
page = service.paginate(
    sql.select("*").from_("users").where(sql.users.active == True),
    page=2,
    page_size=20,
    schema_type=User
)

# Existence check
if service.exists(sql.select("*").from_("users").where(sql.users.email == email)):
    raise ValueError("Email already registered")

# Count
total_users = service.count(sql.select("*").from_("users"))
```

## Testing Requirements

### Unit Tests

- `tests/unit/test_expressions.py` - Test ColumnRef, TableRef, operators
- `tests/unit/test_sql_refactored.py` - Test refactored SQL class
- `tests/unit/test_service_pagination.py` - Test new service methods

### Integration Tests

- All database adapters need expression WHERE clause tests
- Service pagination tests across all databases
- Parameter handling with new SQL constructor

## Success Metrics

1. **Zero Breaking Changes** - All existing code continues to work
2. **Type Safety** - All mypy errors resolved
3. **Performance** - No regression in query execution speed  
4. **Code Quality** - 50% reduction in SQL class size
5. **Developer Experience** - Expression syntax adopted in >80% of new code

## Migration Guide

### For Library Users

```python
# WHERE clauses - both work, new way recommended
# Old
.where(("status", "=", "active"))
# New
.where(sql.table.status == "active")

# Everything else stays the same!
```

### For Library Maintainers

1. Run Phase 1 fixes immediately to restore stability
2. Implement Phase 2 for quick user value
3. Complete Phases 3-4 incrementally with thorough testing
4. Add Phase 5 based on user feedback

## Timeline

- **Week 1**: Phase 1 (stabilization) + Phase 2 start
- **Week 2**: Complete Phase 2 (expressions) + Phase 3 start  
- **Week 3**: Complete Phase 3 (integration) + Phase 4
- **Week 4**: Phase 5 (service enhancements) + documentation

## Potential Breaking Change Proposal

### Unified Parameter API

**Problem:** Currently, parameters can be passed in multiple confusing ways:
- Positional parameters: `SQL("SELECT ?", (1,))`
- Named parameters: `SQL("SELECT :id", kwargs={"id": 1})`
- Mixed via filters and builders
- Different parameter styles for different databases

**Proposal:** Unify all parameter passing to a single, consistent API:

```python
# Before (confusing)
SQL("SELECT * WHERE id = ?", (1,))
SQL("SELECT * WHERE id = :id", kwargs={"id": 1})
sql.select().where(("id", "=", 1))

# After (unified)
sql("SELECT * WHERE id = {id}", id=1)  # Always use {} placeholders
sql.select().where(sql.table.id == 1)  # Or expression syntax
```

**Benefits:**
1. One consistent way to pass parameters
2. Database-agnostic placeholder style (converted internally)
3. Cleaner, more Pythonic API
4. Prevents SQL injection by default

**Migration Path:**
1. Add new unified API alongside old API
2. Deprecate old parameter passing in v2.0
3. Remove old API in v3.0

This would be the only breaking change, but it would significantly improve developer experience and reduce confusion around parameter handling.

## Conclusion

This plan transforms a half-completed refactoring into a structured improvement journey. By prioritizing stability first, then user value, then technical debt, we ensure continuous delivery of improvements while maintaining system reliability.

The key insight is that the original refactoring goals were correct - the SQL class is too large and defensive programming is excessive. However, the implementation approach of changing everything at once created instability. This phased approach achieves the same goals incrementally and safely.
