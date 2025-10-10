# Planning: Example Feature

This is an example planning document created by the Planner agent.

## Research Findings

### Static Guides Consulted

- `docs/guides/performance/sqlglot-best-practices.md` - SQL optimization patterns
- `docs/guides/architecture/data-flow.md` - How data flows through SQLSpec
- `docs/guides/adapters/asyncpg.md` - PostgreSQL patterns

### Context7 Documentation

- `/apache/arrow` - PyArrow conversion best practices
- `/pandas-dev/pandas` - DataFrame creation from Arrow
- `/pola-rs/polars` - Polars DataFrame creation

### WebSearch Findings

- Arrow zero-copy best practices (2025)
- Pandas performance optimization (2025)
- Polars vs Pandas benchmarks

## Implementation Plan

### Phase 1: Core Implementation

1. Add `to_format()` method to `sqlspec/core/result.py`:
   ```python
   def to_format(self, format: str) -> Any:
       """Convert result to specified format."""
       if format == "arrow":
           return self._to_arrow()
       elif format == "pandas":
           return self._to_pandas()
       elif format == "polars":
           return self._to_polars()
       elif format == "dict":
           return self.as_dicts()
       else:
           raise ValueError(f"Unknown format: {format}")
   ```

2. Implement format-specific converters:
   - `_to_arrow()` - Use existing Arrow support
   - `_to_pandas()` - Convert via Arrow (zero-copy)
   - `_to_polars()` - Convert via Arrow (zero-copy)

### Phase 2: Adapter-Specific Code

No adapter-specific code needed - core implementation works for all adapters.

### Phase 3: Testing

See tasks.md for comprehensive test list.

### Phase 4: Documentation

Add examples to quick reference guide.

## Architecture Decisions

### Decision: Use Arrow as Intermediate Format

**Consensus:** Gemini-2.5-Pro ✅, GPT-5 ✅

**Rationale:**
- Arrow is already supported
- Zero-copy conversion to Pandas/Polars
- Efficient for large result sets
- Industry standard

### Decision: Error Handling Strategy

Raise `ValueError` for unsupported formats rather than returning None or empty results.

**Rationale:**
- Fail fast principle
- Clear error messages
- User can catch and handle

## Performance Considerations

### Zero-Copy Conversions

Arrow → Pandas and Arrow → Polars can be zero-copy:

```python
# Zero-copy when possible
arrow_table = result.to_arrow()
pandas_df = arrow_table.to_pandas(zero_copy_only=True)
```

### Large Result Sets

For 1M+ row results:
- Use streaming when possible
- Consider chunked conversion
- Monitor memory usage

## Edge Cases

- Empty result set → Return empty Arrow table / DataFrame
- Single row → Should work same as multi-row
- Single column → Should work
- NULL values → Arrow handles natively

## Testing Checklist

See tasks.md for full testing checklist.

## Next Steps

1. Expert agent implements core functionality
2. Testing agent creates comprehensive tests
3. Docs & Vision documents and validates
