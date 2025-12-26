# Custom SQLglot Dialects

<a id="custom-sqlglot-dialect"></a>

This guide explains how SQLSpec implements and uses custom SQLglot dialects for database-specific SQL features.

## Overview

SQLSpec relies on [sqlglot](https://sqlglot.com/) for SQL parsing, validation, and dialect conversion. While sqlglot supports 20+ dialects out of the box, some databases require custom dialect extensions for proprietary features.

## When to Create a Custom Dialect

Create a custom SQLglot dialect when:

1. **Database-specific syntax** - The database has unique DDL or DML syntax not supported by existing dialects
2. **Feature validation** - You need to parse and validate database-specific keywords (INTERLEAVE, TTL, etc.)
3. **SQL generation** - You need to generate database-specific SQL from AST
4. **Inheritance opportunity** - An existing dialect provides 80%+ compatibility

**Do NOT create a custom dialect if:**
- Standard SQL with minor parameter style differences (use parameter profiles instead)
- Only type conversion differences (use type converters)
- Only connection management differences (use config/driver only)

## Inheritance Strategy

SQLglot dialects form an inheritance hierarchy. Choose the closest base dialect:

```
Dialect (base)
├── ANSI
│   ├── PostgreSQL
│   │   └── Spangres (Spanner PostgreSQL mode)
│   └── MySQL
└── BigQuery
    └── Spanner (Spanner GoogleSQL mode)
```

### Spanner Example: Why Inherit from BigQuery?

The Spanner GoogleSQL dialect inherits from BigQuery because:

1. **Common foundation** - Both use GoogleSQL (ANSI 2011 with Google extensions)
2. **Parameter style** - Both use `@param` named parameters
3. **Type system** - Similar types (INT64, FLOAT64, STRING, BYTES, JSON)
4. **Less work** - Inheriting BigQuery reduces implementation by ~60%

**Key differences**:
- Spanner adds: INTERLEAVE, transactions, foreign keys
- BigQuery adds: Clustering, partitioning, ML functions

## Implementation Pattern

### 1. Basic Structure

```python
from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.tokens import TokenType

class Spanner(BigQuery):
    """Custom dialect inheriting from BigQuery."""

    class Tokenizer(BigQuery.Tokenizer):
        """Extend tokenizer with new keywords."""
        KEYWORDS = {
            **BigQuery.Tokenizer.KEYWORDS,
            "INTERLEAVE": TokenType.INTERLEAVE,
        }

    class Parser(BigQuery.Parser):
        """Override parser for custom syntax."""
        def _parse_table_parts(self, schema=False, is_db_reference=False, wildcard=False):
            table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference, wildcard=wildcard)
            # Custom parsing logic
            return table

    class Generator(BigQuery.Generator):
        """Override generator for custom SQL output."""
        def table_sql(self, expression, sep=" "):
            sql = super().table_sql(expression, sep=sep)
            # Custom generation logic
            return sql
```

### 2. Tokenizer: Adding Keywords

The tokenizer converts SQL text into tokens. Add database-specific keywords:

```python
class Tokenizer(BigQuery.Tokenizer):
    KEYWORDS = {
        **BigQuery.Tokenizer.KEYWORDS,
        "INTERLEAVE": TokenType.INTERLEAVE,
        "TTL": TokenType.TTL,
    }
```

**Note**: Check if TokenType exists before using. Some keywords may not have corresponding token types in sqlglot.

### 3. Parser: Handling Custom Syntax

The parser converts tokens into an AST (Abstract Syntax Tree). Override parser methods to handle custom clauses:

```python
class Parser(BigQuery.Parser):
    def _parse_table_parts(self, schema=False, is_db_reference=False, wildcard=False):
        """Parse table with INTERLEAVE clause."""
        table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference, wildcard=wildcard)

        # Check for custom clause
        if self._match_text_seq("INTERLEAVE", "IN", "PARENT"):
            parent = self._parse_table(schema=True, is_db_reference=True)
            on_delete = None

            if self._match_text_seq("ON", "DELETE"):
                if self._match_text_seq("CASCADE"):
                    on_delete = "CASCADE"
                elif self._match_text_seq("NO", "ACTION"):
                    on_delete = "NO ACTION"

            # Store in AST
            table.set("interleave_parent", parent)
            if on_delete:
                table.set("interleave_on_delete", on_delete)

        return table
```

**Common parser methods**:
- `_match_text_seq(*tokens)` - Match sequence of tokens
- `_parse_table()` - Parse table reference
- `_parse_id_var()` - Parse identifier/variable
- `_parse_expression()` - Parse general expression
- `_match(TokenType.*)` - Match specific token type

### 4. Generator: Outputting Custom SQL

The generator converts AST back to SQL. Override generator methods to output custom syntax:

```python
class Generator(BigQuery.Generator):
    def table_sql(self, expression, sep=" "):
        """Generate table SQL with INTERLEAVE clause."""
        sql = super().table_sql(expression, sep=sep)

        # Check for custom metadata
        parent = expression.args.get("interleave_parent")
        if parent:
            sql = f"{sql}\nINTERLEAVE IN PARENT {self.sql(parent)}"
            on_delete = expression.args.get("interleave_on_delete")
            if on_delete:
                sql = f"{sql} ON DELETE {on_delete}"

        return sql
```

### 5. Registration

Register the dialect with sqlglot in your adapter's `__init__.py`:

```python
from sqlglot.dialects.dialect import Dialect
from sqlspec.adapters.spanner import dialect

# Register both GoogleSQL and PostgreSQL modes
Dialect.classes["spanner"] = dialect.Spanner
Dialect.classes["spangres"] = dialect.Spangres
```

## Testing Strategy

### Unit Tests

Test parsing and generation separately:

```python
import sqlglot

def test_parse_interleave_clause():
    """Test parsing INTERLEAVE IN PARENT."""
    sql = """
    CREATE TABLE child (
        id INT64,
        parent_id INT64
    ) INTERLEAVE IN PARENT parent ON DELETE CASCADE
    """
    ast = sqlglot.parse_one(sql, dialect="spanner")
    assert ast.args.get("interleave_parent") is not None

def test_generate_interleave_clause():
    """Test generating INTERLEAVE SQL."""
    # Build AST programmatically
    # Generate SQL
    # Verify output
    pass

def test_roundtrip():
    """Parse → Generate → Parse should be idempotent."""
    original = "CREATE TABLE t (...) INTERLEAVE IN PARENT p"
    ast = sqlglot.parse_one(original, dialect="spanner")
    generated = ast.sql(dialect="spanner")
    reparsed = sqlglot.parse_one(generated, dialect="spanner")
    # Verify AST equivalence
```

### Integration Tests

Test with real database to verify generated SQL works:

```python
def test_interleave_with_real_spanner(spanner_session):
    """Verify INTERLEAVE DDL works."""
    spanner_session.execute("""
        CREATE TABLE parent (id INT64) PRIMARY KEY (id)
    """)
    spanner_session.execute("""
        CREATE TABLE child (
            parent_id INT64,
            child_id INT64
        ) PRIMARY KEY (parent_id, child_id),
          INTERLEAVE IN PARENT parent ON DELETE CASCADE
    """)
    # Verify table created successfully
```

## Best Practices

### 1. Minimal Override Philosophy

Only override what's necessary. Don't duplicate parent dialect logic:

```python
# ❌ BAD: Duplicates parent logic
def _parse_table_parts(self, schema=False):
    # Reimplements entire table parsing
    pass

# ✅ GOOD: Extends parent logic
def _parse_table_parts(self, schema=False, is_db_reference=False, wildcard=False):
    table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference, wildcard=wildcard)
    # Only add custom logic
    if self._match_text_seq("CUSTOM", "CLAUSE"):
        # Handle custom clause
        pass
    return table
```

### 2. Store Metadata in AST

Use `expression.set(key, value)` to attach custom metadata to AST nodes:

```python
# Store in AST
table.set("interleave_parent", parent)
table.set("interleave_on_delete", "CASCADE")

# Retrieve in generator
parent = expression.args.get("interleave_parent")
on_delete = expression.args.get("interleave_on_delete")
```

### 3. Handle Missing Token Types

Not all keywords have TokenType constants. Handle gracefully:

```python
_SPANNER_KEYWORDS = {}
interleave_token = getattr(TokenType, "INTERLEAVE", None)
if interleave_token is not None:
    _SPANNER_KEYWORDS["INTERLEAVE"] = interleave_token

class Tokenizer(BigQuery.Tokenizer):
    KEYWORDS = {**BigQuery.Tokenizer.KEYWORDS, **_SPANNER_KEYWORDS}
```

### 4. Use Text Sequence Matching

For multi-word keywords, use `_match_text_seq()`:

```python
# Match "ROW DELETION POLICY"
if self._match_text_seq("ROW", "DELETION", "POLICY"):
    # Parse policy
    pass
```

### 5. Graceful Degradation

If sqlglot limitations prevent full validation, parse minimally and document:

```python
def _parse_property(self):
    """Parse TTL property (minimal validation)."""
    if self._match_text_seq("TTL"):
        # Basic parsing without deep validation
        # Document limitation in docstring
        pass
    return super()._parse_property()
```

## Reference Implementation: Spanner

The Spanner adapter provides a complete reference implementation:

- **GoogleSQL mode**: `/sqlspec/adapters/spanner/dialect/_spanner.py`
- **PostgreSQL mode**: `/sqlspec/adapters/spanner/dialect/_spangres.py`

Key features implemented:
- INTERLEAVE IN PARENT parsing and generation
- ROW DELETION POLICY (TTL) support
- Inherits 95%+ from BigQuery/Postgres base dialects

## Future Enhancements

### Contributing Upstream

If your dialect additions are generally useful, consider contributing to sqlglot:

1. Add TokenType constants for new keywords
2. Add expression types for new clauses
3. Submit PR with tests

Benefits:
- Better validation
- Community maintenance
- Available to all sqlglot users

### Multi-Dialect Support

Some databases support multiple SQL dialects (e.g., Spanner's GoogleSQL and PostgreSQL modes):

```python
# Create separate dialect classes
class Spanner(BigQuery):
    """GoogleSQL mode."""
    pass

class Spangres(Postgres):
    """PostgreSQL mode."""
    pass

# Register both
Dialect.classes["spanner"] = Spanner
Dialect.classes["spangres"] = Spangres
```

## See Also

- [sqlglot Documentation](https://sqlglot.com/)
- [Spanner Adapter Guide](/guides/adapters/spanner.md)
- [Architecture Overview](/guides/architecture/architecture.md)
- [Creating Adapters](/contributing/creating_adapters.md)
