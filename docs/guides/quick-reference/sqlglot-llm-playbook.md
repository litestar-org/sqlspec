---
orphan: true
---

# SQLglot LLM Playbook

Primer for autonomous agents working inside SQLSpec with SQLglot. Favors fast recall, minimal prompting, and accurate outputs.

## When to Reach for SQLglot

- Parse SQL strings into structured trees the core engine can cache and reuse.
- Transpile dialects during adapter work or tests when cross-database parity matters.
- Format SQL or align naming to project conventions before embedding into fixtures.
- Run optimizer passes (simplify, qualify, expand) when you need canonical SQL for comparisons or diffing.

## Speed-First Habits

- Parse once, reuse everywhere. Store the `Expression` object and pass it through builders or statements rather than bouncing back to strings.
- Prefer `sqlglot.transpile` with `read` and `write` dialects instead of parse → stringify → re-parse loops.
- Lean on `SQL.ensure_expression` and existing helpers before importing sqlglot directly—most adapters already expose hooks.
- Cache constant fragments inside module scope (for example predicates or projections) so repeated calls skip parsing cost.
- Keep optimizer pipelines short. Compose only the passes you need (`simplify`, `unnest_joins`, `pushdown_predicates`) to avoid unnecessary tree walks.
- Use `copy=False` for builder mutations by default. Deep copies allocate entire trees and break shared caches; only pass `copy=True` for defensive cloning in edge cases.

## Core API Patterns

- `sqlglot.parse_one(sql, read="dialect")` → canonical AST. Use `error_level="raise"` in tests to surface invalid SQL early.
- `sqlglot.transpile(sql, read="source", write="target", pretty=True)` → string output tailored to the destination engine.
- `sqlglot.optimizer.optimize(expression, rules=(simplify.simplify, qualify.qualify))` → run deterministic rewrites before caching.
- `sqlglot.select("*").from_(table).where(predicate_exp)` → programmatic construction that stays in AST form until execution.
- `expression.find_all(exp.Column)` → inspect projected columns during linting or guard-rail logic.

## Project Integration Hooks

- The SQLSpec `SQL` object accepts `sqlglot.Expression` instances directly. Prefer passing expressions over raw strings when building queries in drivers, loaders, or builders.
- Parameter style conversion happens after sqlglot normalization. Avoid manual placeholder rewrites; rely on `SQL` and `parameters.py` to map to driver-specific tokens.
- For migrations, use sqlglot to validate generated SQL before writing files so that round-trips stay deterministic across adapters.
- In tests, compare ASTs (`parse_one(expected).normalized`) instead of raw SQL strings to dodge formatting noise.

## Common Pitfalls

- Repeated parsing inside hot paths. Move parsing to module constants or initialization blocks.
- Transpiling via manual string manipulation. Always use sqlglot’s dialect system so identifier quoting and functions stay correct.
- Forgetting dialect context in `parse_one`. Missing the `read` argument causes silent default-to-generic parsing that can mask dialect quirks.
- Running every optimizer pass. Some passes (like `optimize_joins`) cost tens of microseconds; include them only when you need the rewrite.
- Importing sqlglot inside functions. Keep imports at module top to respect project conventions and improve cold-start performance.
- Calling builder methods with the default `copy=True`. This triggers deep copies and defeats AST reuse; `copy=False` is the mandatory project default unless you must isolate mutations.

## Retrieval Targets

- Dialect matrix and feature coverage: <https://sqlglot.com/sqlglot/dialects/>
- Expressions catalog (node types, helpers): <https://sqlglot.com/sqlglot/expressions/>
- Optimizer reference (available passes, configuration): <https://sqlglot.com/sqlglot/optimizer/index.html>
- Planner overview (execution planning helpers): <https://sqlglot.com/sqlglot/planner.html>
- Token definitions and lexer behavior: <https://sqlglot.com/sqlglot/tokens.html>
- Library source for deeper examples: <https://github.com/tobymao/sqlglot>

## Ship Checklist

- Parsed artifacts are cached or reused instead of re-parsed in loops.
- Dialect assumptions documented via constants or typed config.
- Tests assert on expressions or structured comparisons, not raw SQL strings.
- Links to official docs included in comments, READMEs, or docstrings for future agents.
- Added guidance captured in `docs/guides/` (this playbook) so retrievers surface it alongside code.
