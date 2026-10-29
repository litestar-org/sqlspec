# Schema Mapping

By default, `select`, `select_one`, and `select_one_or_none` return `dict[str, Any]` rows. Passing `schema_type=` hydrates rows directly into typed Python structures while preserving static type inference in `mypy` and `pyright`.

## Mapping Rows to `msgspec.Struct`

`msgspec.Struct` is the fastest and lowest-memory target for typed row hydration in SQLSpec (`uv add "sqlspec[msgspec]"`). Declaring `frozen=True, gc=False` omits CPython cyclic garbage-collection headers on every row object:

```{ .python title="docs/examples/querying/schema_mapping.py" hl_lines="11 30 31 32" }
--8<-- "docs/examples/querying/schema_mapping.py"
```

Notice how `schema_type=User` narrows each method's static return type automatically:

- `session.select(..., schema_type=User)` returns `list[User]`.
- `session.select_one(..., schema_type=User)` returns `User`.
- `session.select_one_or_none(..., schema_type=User)` returns `User | None`.

## Supported Schema Libraries

SQLSpec's schema dispatcher supports five model families out of the box without requiring adapter-specific glue code:

| Model Family | Install Extra | Best When |
| --- | --- | --- |
| `msgspec.Struct` | `sqlspec[msgspec]` | High-throughput APIs, Litestar `MsgspecDTO` endpoints, low GC pressure, strict C-speed decoding |
| `typing.TypedDict` | *(built-in)* | Lightweight static typing over plain `dict` rows at zero runtime conversion cost |
| `dataclasses.dataclass` | *(built-in)* | Standard-library domain value objects with zero external dependencies |
| `pydantic.BaseModel` | `sqlspec[pydantic]` | FastAPI applications or rich field validators (`EmailStr`, custom `@field_validator`) |
| `attrs` (`@define`) | `sqlspec[attrs]` | Codebases standardized on `attrs` and `cattrs` converters |

## Comparing Schema Definitions

All supported schema types plug into the exact same `schema_type=` keyword argument:

=== "msgspec"

    ```python
    import msgspec


    class AccountRow(msgspec.Struct, frozen=True, gc=False):
        id: int
        email: str
        is_active: bool = True
    ```

=== "TypedDict"

    ```python
    from typing import TypedDict


    class AccountRow(TypedDict):
        id: int
        email: str
        is_active: bool
    ```

=== "Pydantic"

    ```python
    from pydantic import BaseModel


    class AccountRow(BaseModel):
        id: int
        email: str
        is_active: bool = True
    ```

=== "dataclass"

    ```python
    from dataclasses import dataclass


    @dataclass(frozen=True, slots=True)
    class AccountRow:
        id: int
        email: str
        is_active: bool = True
    ```

## Column Naming and Case Normalization

Schema mapping matches SQL result column names to schema field names:

- Alias computed expressions explicitly in SQL (`SELECT COUNT(*) AS total_orders`) so column names match your struct or model attributes.
- Enterprise engines that return unquoted column identifiers in uppercase (`Oracle`, `IBM Db2`, and `ADBC` over Oracle/Db2) enable `driver_features={"enable_lowercase_column_names": True}` in SQLSpec so `SELECT id, email FROM accounts` maps cleanly onto lowercase `id` and `email` Python fields without quoting every alias.
- For camelCase JSON APIs backed by `snake_case` SQL columns, configure `rename="camel"` on your `msgspec.Struct` or alias generator on `BaseModel` while keeping SQL column projections in `snake_case`.

## Recap

- Start with `msgspec.Struct(..., frozen=True, gc=False)` for fast, immutable row objects, or `TypedDict` when you want static types with zero runtime allocation beyond the driver dictionary.
- Pass `schema_type=` directly to `select`, `select_one`, `select_one_or_none`, `select_with_total`, or `SQLResult.all(schema_type=...)`.
- Use explicit `AS field_name` SQL aliases for calculated columns so projections remain decoupled from underlying expression text.
