# Feature: Example Feature

This is an example Product Requirements Document (PRD).

## Overview

Brief description of the feature, why it's needed, and what problem it solves.

## Acceptance Criteria

- [ ] Criterion 1 - Feature works as described
- [ ] Criterion 2 - Edge cases handled (empty, None, errors)
- [ ] Criterion 3 - Performance acceptable (no regressions)
- [ ] Criterion 4 - Tests comprehensive (80%+ coverage)
- [ ] Criterion 5 - Documentation complete

## Technical Design

### Affected Components

- `sqlspec/core/result.py` - Add new method
- `sqlspec/adapters/asyncpg/driver.py` - Implement adapter-specific logic

### Database Adapters Affected

- AsyncPG (PostgreSQL)
- Oracle
- DuckDB

### API Changes

```python
# New method on SQLResult
class SQLResult:
    def to_format(self, format: str) -> Any:
        """Convert result to specified format.

        Args:
            format: Output format (arrow, pandas, polars, dict)

        Returns:
            Result in specified format.
        """
        pass
```

## Dependencies

- Requires pyarrow for Arrow format
- Requires pandas for DataFrame format
- Requires polars for Polars format

## Testing Strategy

### Unit Tests
- Test format conversion logic
- Test error handling (invalid format)
- Test empty result sets

### Integration Tests
- Test with real asyncpg connection
- Test with real oracle connection
- Test with real duckdb connection
- Test large result sets (10k+ rows)

## Migration Notes

Non-breaking change - new functionality only.

## Performance Considerations

- Arrow format should be zero-copy when possible
- Pandas/Polars conversion may allocate new memory
- Large result sets (1M+ rows) should stream

## Security Considerations

None - read-only operation.
