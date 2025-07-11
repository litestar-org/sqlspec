# SQLGlot Best Practices Cheat Sheet

*A comprehensive guide for working with SQLGlot in SQLSpec and beyond*

## Table of Contents

1. [Core Architecture](#core-architecture)
2. [Parsing & Basic Operations](#parsing--basic-operations)
3. [AST Traversal & Manipulation](#ast-traversal--manipulation)
4. [Expression Construction](#expression-construction)
5. [Dialect Handling](#dialect-handling)
6. [Optimization & Transformation](#optimization--transformation)
7. [Security Patterns](#security-patterns)
8. [Performance Best Practices](#performance-best-practices)
9. [Common Patterns in SQLSpec](#common-patterns-in-sqlspec)
10. [Anti-Patterns to Avoid](#anti-patterns-to-avoid)

---

## Core Architecture

SQLGlot's three-layer architecture transforms SQL through these stages:

```
Raw SQL → Tokenizer → Parser → AST → Generator → SQL
```

### Key Components

- **Tokenizer**: Converts raw SQL into tokens with metadata
- **Parser**: Builds Abstract Syntax Tree (AST) from tokens
- **Generator**: Converts AST back to SQL with dialect-specific formatting
- **Optimizer**: Transforms AST for performance and canonicalization

---

## Parsing & Basic Operations

### 1. Basic Parsing

```python
import sqlglot
import sqlglot.expressions as exp

# Simple parsing
ast = sqlglot.parse_one("SELECT * FROM users WHERE id = 1")

# Parse with specific dialect
ast = sqlglot.parse_one("SELECT * FROM users", dialect="postgres")

# Safe parsing (returns None on failure)
ast = exp.maybe_parse("SELECT * FROM users")

# Parse multiple statements
statements = sqlglot.parse("SELECT 1; SELECT 2;")
```

### 2. Generation

```python
# Generate SQL from AST
sql = ast.sql()

# Generate with specific dialect
sql = ast.sql(dialect="snowflake")

# Pretty printing
sql = ast.sql(pretty=True)

# Copy before modifying
modified_ast = ast.copy()
```

### 3. Transpilation

```python
# Direct transpilation between dialects
converted = sqlglot.transpile(
    "SELECT * FROM table",
    read="postgres",
    write="snowflake"
)[0]
```

---

## AST Traversal & Manipulation

### 1. Basic Traversal Methods

```python
# Find first occurrence of expression type
select_node = ast.find(exp.Select)
table_node = ast.find(exp.Table)

# Find all occurrences
all_tables = list(ast.find_all(exp.Table))
all_columns = list(ast.find_all(exp.Column))

# Walk through all nodes
for node in ast.walk():
    if isinstance(node, exp.Column):
        print(f"Found column: {node.name}")
```

### 2. Advanced Traversal

```python
# Walk with filtering
for node in ast.walk(exp.Select):
    print(f"Found SELECT: {node.sql()}")

# Find with conditions
first_user_table = ast.find(
    exp.Table,
    lambda table: table.name == "users"
)

# Find parent nodes
for column in ast.find_all(exp.Column):
    parent_select = column.find_ancestor(exp.Select)
    if parent_select:
        print(f"Column {column.name} in SELECT")
```

### 3. Scope-Based Analysis

```python
# For complex semantic analysis
from sqlglot.optimizer.scope import build

root = build(ast)[0]
for scope in root.traverse():
    for alias, (node, source) in scope.selected_sources.items():
        print(f"Table alias: {alias}, Source: {source}")

    for column in scope.columns:
        print(f"Column: {column}")
```

---

## Expression Construction

### 1. Using Builder Methods (Preferred)

```python
# Column references
col = exp.column("name")
qualified_col = exp.column("name", table="users")

# Table references
table = exp.to_table("users")
aliased_table = exp.alias_("users", "u")

# Conditions
condition = exp.condition("id = 1")
eq_condition = exp.EQ(this=exp.column("id"), expression=exp.Literal.number("1"))

# Functions
count_expr = exp.func("COUNT", exp.Star())
max_expr = exp.func("MAX", exp.column("price"))
```

### 2. Literal Construction

```python
# String literals
str_literal = exp.Literal.string("John")

# Number literals
num_literal = exp.Literal.number("42")
float_literal = exp.Literal.number("3.14")

# Boolean literals
true_literal = exp.Boolean(this=True)
false_literal = exp.Boolean(this=False)

# NULL literal
null_literal = exp.null()
```

### 3. Complex Expression Building

```python
# SELECT statement
select = exp.Select()
select = select.select("id", "name")
select = select.from_("users")
select = select.where("active = true")

# JOIN operations
select = select.join("orders", on="users.id = orders.user_id")

# Subqueries
subquery = exp.select("user_id").from_("orders").where("total > 100")
select = select.where(f"id IN ({subquery.sql()})")
```

---

## Dialect Handling

### 1. Dialect-Aware Parsing

```python
# PostgreSQL-specific features
pg_ast = sqlglot.parse_one(
    "SELECT * FROM users WHERE data @> '{\"key\": \"value\"}'::jsonb",
    dialect="postgres"
)

# MySQL-specific syntax
mysql_ast = sqlglot.parse_one(
    "SELECT * FROM users LIMIT 10 OFFSET 20",
    dialect="mysql"
)

# BigQuery-specific
bq_ast = sqlglot.parse_one(
    "SELECT EXTRACT(DATE FROM timestamp_col) FROM table",
    dialect="bigquery"
)
```

### 2. Dialect-Specific Generation

```python
# Generate for different targets
postgres_sql = ast.sql(dialect="postgres")
mysql_sql = ast.sql(dialect="mysql")
snowflake_sql = ast.sql(dialect="snowflake")

# Check dialect support
if hasattr(sqlglot.dialects, "postgres"):
    # Postgres dialect is available
    pass
```

### 3. Cross-Dialect Compatibility

```python
# Use base dialect for maximum compatibility
base_ast = sqlglot.parse_one("SELECT * FROM users", dialect="")

# Check for dialect-specific features
if ast.find(exp.DataType, lambda dt: dt.this == "JSONB"):
    print("Contains PostgreSQL-specific JSONB type")
```

---

## Optimization & Transformation

### 1. Built-in Optimizations

```python
from sqlglot.optimizer import optimize

# Apply all optimizations
optimized = optimize(ast, dialect="postgres")

# Specific optimizations
from sqlglot.optimizer.simplify import simplify
simplified = simplify(ast)

from sqlglot.optimizer.qualify import qualify
qualified = qualify(ast, dialect="postgres")
```

### 2. Custom Transformations

```python
# Transform using visitor pattern
def remove_comments(node):
    if isinstance(node, exp.Comment):
        return None
    return node

cleaned_ast = ast.transform(remove_comments)

# Conditional transformations
def parameterize_literals(node):
    if isinstance(node, exp.Literal) and isinstance(node.this, str):
        # Replace with placeholder
        return exp.Placeholder(this="?")
    return node

parameterized = ast.transform(parameterize_literals)
```

### 3. Node Manipulation

```python
# Set node properties
node.set("alias", "new_alias")

# Append to collections
select_node.expressions.append(exp.column("new_column"))

# Replace nodes
old_table = ast.find(exp.Table)
new_table = exp.to_table("new_table_name")
old_table.replace(new_table)
```

---

## Security Patterns

### 1. Parameter Validation

```python
def validate_identifiers(node):
    """Ensure identifiers don't contain suspicious patterns."""
    if isinstance(node, (exp.Table, exp.Column)):
        name = node.name
        if name and any(char in name for char in [';', '--', '/*']):
            raise ValueError(f"Suspicious identifier: {name}")
    return node

# Apply validation
safe_ast = ast.transform(validate_identifiers)
```

### 2. Injection Detection

```python
def detect_tautologies(node):
    """Detect always-true conditions."""
    if isinstance(node, exp.EQ):
        left, right = node.this, node.expression
        if (isinstance(left, exp.Literal) and isinstance(right, exp.Literal)
            and left.this == right.this):
            print(f"Tautology detected: {node.sql()}")
    return node

ast.transform(detect_tautologies)
```

### 3. Safe Parameter Binding

```python
def bind_parameters(ast, params):
    """Safely bind parameters to placeholders."""
    param_index = 0

    def replace_placeholder(node):
        nonlocal param_index
        if isinstance(node, exp.Placeholder):
            if param_index < len(params):
                value = params[param_index]
                param_index += 1
                if isinstance(value, str):
                    return exp.Literal.string(value)
                elif isinstance(value, (int, float)):
                    return exp.Literal.number(str(value))
        return node

    return ast.transform(replace_placeholder)
```

---

## Performance Best Practices

### 1. Efficient AST Operations

```python
# ✅ Good: Use find() for single occurrence
first_table = ast.find(exp.Table)

# ❌ Avoid: Converting generator to list unnecessarily
all_tables = list(ast.find_all(exp.Table))
first_table = all_tables[0] if all_tables else None

# ✅ Good: Use generator directly
first_table = next(ast.find_all(exp.Table), None)
```

### 2. Minimize Copying

```python
# ✅ Good: Modify in place when safe
node.set("alias", "new_alias")

# ❌ Avoid: Unnecessary copying
copy = node.copy()
copy.set("alias", "new_alias")
```

### 3. Cache Compiled Patterns

```python
import re

# ✅ Good: Pre-compile regex patterns
COMMENT_PATTERN = re.compile(r'/\*.*?\*/', re.DOTALL)

def remove_comments(sql):
    return COMMENT_PATTERN.sub('', sql)

# ❌ Avoid: Compiling patterns repeatedly
def remove_comments(sql):
    return re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
```

---

## Common Patterns in SQLSpec

### 1. Expression Parsing Utilities

```python
# Based on sqlspec/statement/builder/_parsing_utils.py
def parse_column_expression(column_input):
    """Parse column input handling various formats."""
    if isinstance(column_input, exp.Expression):
        return column_input

    # Try parsing as expression first
    parsed = exp.maybe_parse(column_input)
    if parsed:
        return parsed

    # Fallback to column
    return exp.column(str(column_input))

def parse_condition_expression(condition_input):
    """Parse condition with tuple and string support."""
    if isinstance(condition_input, exp.Expression):
        return condition_input

    if isinstance(condition_input, tuple) and len(condition_input) == 2:
        column, value = condition_input
        column_expr = parse_column_expression(column)

        if value is None:
            return exp.Is(this=column_expr, expression=exp.null())

        if isinstance(value, str):
            return exp.EQ(this=column_expr, expression=exp.Literal.string(value))
        elif isinstance(value, (int, float)):
            return exp.EQ(this=column_expr, expression=exp.Literal.number(str(value)))

    # Parse as condition string
    return exp.condition(str(condition_input))
```

### 2. Literal Parameterization

```python
# Based on sqlspec/statement/pipelines/transformers/_literal_parameterizer.py
class ParameterizeLiterals:
    """Extract literals and replace with placeholders."""

    def process(self, expression):
        parameters = []

        def extract_literal(node):
            if isinstance(node, exp.Literal):
                if isinstance(node.this, str):
                    # String literal
                    param_name = f"param_{len(parameters)}"
                    parameters.append(node.this)
                    return exp.Placeholder(this=param_name)
                elif isinstance(node.this, (int, float)):
                    # Numeric literal
                    param_name = f"param_{len(parameters)}"
                    parameters.append(node.this)
                    return exp.Placeholder(this=param_name)
            return node

        parameterized = expression.transform(extract_literal)
        return parameterized, parameters
```

### 3. Security Validation

```python
# Based on sqlspec/statement/pipelines/validators/_security.py
def validate_security(expression):
    """Comprehensive security validation."""
    issues = []

    for node in expression.walk():
        # Check for suspicious functions
        if isinstance(node, exp.Func):
            func_name = node.name.lower() if node.name else ""
            if func_name in SUSPICIOUS_FUNCTIONS:
                issues.append(f"Suspicious function: {func_name}")

        # Check for UNION injection patterns
        if isinstance(node, exp.Union):
            # Analyze UNION structure
            if isinstance(node.right, exp.Select):
                select_expr = node.right
                if hasattr(select_expr, 'expressions'):
                    null_count = sum(1 for expr in select_expr.expressions
                                   if isinstance(expr, exp.Null))
                    if null_count > 3:  # Suspicious NULL padding
                        issues.append("Potential UNION injection detected")

        # Check for tautologies
        if isinstance(node, exp.EQ):
            left, right = node.this, node.expression
            if (isinstance(left, exp.Literal) and isinstance(right, exp.Literal)
                and left.this == right.this):
                issues.append("Tautology condition detected")

    return issues
```

---

## Anti-Patterns to Avoid

### 1. String Manipulation Instead of AST

```python
# ❌ BAD: String-based SQL manipulation
def add_where_clause(sql, condition):
    if "WHERE" in sql.upper():
        return sql + f" AND {condition}"
    else:
        return sql + f" WHERE {condition}"

# ✅ GOOD: AST-based manipulation
def add_where_clause(ast, condition):
    condition_expr = exp.condition(condition)
    if ast.args.get("where"):
        # AND with existing WHERE
        existing = ast.args["where"]
        new_where = exp.And(this=existing, expression=condition_expr)
        ast.set("where", new_where)
    else:
        # Add new WHERE
        ast.set("where", condition_expr)
    return ast
```

### 2. Ignoring Dialect Differences

```python
# ❌ BAD: Assuming all dialects support same syntax
def create_limit_query(table, limit):
    return f"SELECT * FROM {table} LIMIT {limit}"

# ✅ GOOD: Dialect-aware construction
def create_limit_query(table, limit, dialect=""):
    select = exp.Select().select("*").from_(table).limit(limit)
    return select.sql(dialect=dialect)
```

### 3. Manual AST Construction

```python
# ❌ BAD: Manual node construction
def create_select():
    select = exp.Select()
    select.args = {
        "expressions": [exp.Star()],
        "from": exp.From(this=exp.Table(this="users"))
    }
    return select

# ✅ GOOD: Use builder methods
def create_select():
    return exp.Select().select("*").from_("users")
```

### 4. Not Handling Parse Failures

```python
# ❌ BAD: Assuming parse always succeeds
def process_sql(sql):
    ast = sqlglot.parse_one(sql)
    return ast.sql()

# ✅ GOOD: Handle parse failures gracefully
def process_sql(sql):
    try:
        ast = sqlglot.parse_one(sql)
        return ast.sql()
    except Exception:
        # Log error and return original or None
        logger.warning(f"Failed to parse SQL: {sql}")
        return sql
```

### 5. Modifying Shared AST Nodes

```python
# ❌ BAD: Modifying shared references
def process_queries(queries):
    base_ast = sqlglot.parse_one("SELECT * FROM users")
    results = []
    for query in queries:
        # This modifies the shared base_ast!
        modified = base_ast.where(query["condition"])
        results.append(modified.sql())
    return results

# ✅ GOOD: Copy before modifying
def process_queries(queries):
    base_ast = sqlglot.parse_one("SELECT * FROM users")
    results = []
    for query in queries:
        # Create independent copy
        modified = base_ast.copy().where(query["condition"])
        results.append(modified.sql())
    return results
```

---

## Quick Reference

### Essential Imports

```python
import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer import optimize, simplify
```

### Most Used Patterns

```python
# Parse
ast = sqlglot.parse_one(sql, dialect="postgres")

# Find elements
tables = list(ast.find_all(exp.Table))
first_column = ast.find(exp.Column)

# Transform
def transformer(node):
    # Your transformation logic
    return node
transformed = ast.transform(transformer)

# Generate
sql = ast.sql(dialect="postgres", pretty=True)
```

### Common Checks

```python
# Check node type
if isinstance(node, exp.Select):
    pass

# Check for attributes safely
if hasattr(node, 'expressions') and node.expressions:
    pass

# Get SQL representation
sql_repr = node.sql() if hasattr(node, 'sql') else str(node)
```

---

## Contributing to SQLSpec

When adding new SQLGlot patterns to SQLSpec:

1. **Follow existing patterns** in `sqlspec/statement/`
2. **Use type guards** from `sqlspec.utils.type_guards`
3. **Handle parse failures** gracefully
4. **Add comprehensive tests** including edge cases
5. **Document security implications** for new transformations
6. **Consider performance impact** of AST operations
7. **Validate against multiple SQL dialects**

Remember: SQLGlot's power comes from its AST-based approach. Always prefer AST manipulation over string operations for reliable, dialect-aware SQL processing.
