# Tasks: Example Feature

## Workflow Stages

- [ ] 1. Research & Planning (Planner agent)
- [ ] 2. Core Implementation (Expert agent)
- [ ] 3. Adapter-Specific Code (Expert agent)
- [ ] 4. Testing (Testing agent)
- [ ] 5. Documentation (Docs & Vision agent)
- [ ] 6. Quality Gate (Docs & Vision agent)
- [ ] 7. Cleanup (Docs & Vision agent)

## Detailed Tasks

### Planning

- [ ] Research Arrow, Pandas, Polars conversion patterns
- [ ] Consult docs/guides/performance/ for optimization
- [ ] Create detailed implementation plan
- [ ] Get consensus on API design (if needed)

### Implementation

- [ ] Add `to_format()` method to SQLResult
- [ ] Implement Arrow format conversion
- [ ] Implement Pandas format conversion
- [ ] Implement Polars format conversion
- [ ] Add error handling for invalid formats
- [ ] Optimize for zero-copy when possible

### Testing

- [ ] Unit test: format conversion logic
- [ ] Unit test: error handling
- [ ] Unit test: empty results
- [ ] Integration test: AsyncPG with Arrow format
- [ ] Integration test: Oracle with Pandas format
- [ ] Integration test: DuckDB with Polars format
- [ ] Integration test: Large result set (10k rows)

### Documentation

- [ ] Update docs/guides/quick-reference/quick-reference.md
- [ ] Update API reference (docs/reference/)
- [ ] Add usage examples
- [ ] Build docs locally

### Quality Gate

- [ ] Run `make lint` - must pass
- [ ] Check for anti-patterns
- [ ] Run full test suite
- [ ] Verify PRD acceptance criteria

### Cleanup

- [ ] Remove tmp/ directory
- [ ] Archive requirement to requirements/archive/
- [ ] Archive planning reports
