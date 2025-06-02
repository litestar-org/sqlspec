## [REF-002] Psycopg Driver: ModelDTOT and Schema Type Patterns

**DECISION**: Preserve exact `ModelDTOT` and `schema_type` behavior from main branch.

**IMPLEMENTATION**:

- `SelectResult.rows` always contains `dict[str, Any]` objects
- Schema conversion handled by type system and result converter patterns
- `_wrap_select_result` uses conditional return types based on `schema_type` parameter

**USER BENEFIT**:

- Type-safe result conversion with intelligent typing
- Seamless integration with DTO patterns
- Backwards compatibility with existing code

**CODE EXAMPLES**:

```python
# With schema type - gets SelectResult[User]
users = driver.execute("SELECT * FROM users", schema_type=User)

# Without schema type - gets SelectResult[dict[str, Any]]
raw_data = driver.execute("SELECT * FROM users")

# Both work, but typing provides safety
user_name = users.rows[0].name        # ✅ Type-safe
user_name = raw_data.rows[0]["name"]  # ✅ Dict access
```

**OVERLOAD PATTERNS**:

```python
@overload
def execute(statement: SelectBuilder, *, schema_type: type[ModelDTOT]) -> SelectResult[ModelDTOT]: ...

@overload
def execute(statement: SelectBuilder, *, schema_type: None = None) -> SelectResult[dict[str, Any]]: ...
```

---
