# Recovery Guide: {Feature Name}

## To Resume Work

1. **Read this document first**
2. Read [prd.md](prd.md) for full context
3. Check [tasks.md](tasks.md) for current progress
4. Review [research/](research/) for findings
5. Consult [AGENTS.md](../../AGENTS.md) for SQLSpec-specific patterns

## Current Status

**Phase**: {Current phase}
**Last Updated**: {Date}
**Completed**: {X/Y tasks}

## Files Modified

{List of files changed so far}

## Next Steps

{What should be done next}

## Agent-Specific Instructions

### For Expert Agent

**Start Here**:
1. Read PRD (prd.md)
2. Review research questions at end of PRD
3. Consult AGENTS.md for SQLSpec standards:
   - Type annotation standards (stringified, no __future__)
   - Import standards (no nested imports)
   - Clean code principles
   - Adapter patterns
   - driver_features patterns
4. Document findings in `research/`

**Implementation Checklist**:
- [ ] Follow SQLSpec standards (see AGENTS.md)
- [ ] Stringified type hints for non-builtins
- [ ] No `from __future__ import annotations`
- [ ] Write tests as you go
- [ ] Test with affected adapters
- [ ] Update workspace continuously
- [ ] Will auto-invoke Testing agent when implementation complete
- [ ] Will auto-invoke Docs & Vision for docs and archival

**SQLSpec-Specific Reminders**:
- Use protocols and type guards, not inheritance
- Follow parameter style abstraction patterns
- Test against real databases using pytest-databases
- Consider mypyc compilation impacts

### For Testing Agent

**Testing Strategy**:

```bash
# Run tests
make test
# or
uv run pytest -n 2 --dist=loadgroup tests

# Run with coverage
make coverage
# or
uv run pytest --cov -n 2 --dist=loadgroup

# Run integration tests for specific adapter
uv run pytest tests/integration/test_adapters/test_asyncpg/ -v
```

**Test Patterns** (from AGENTS.md):

- **MANDATORY**: Function-based tests (NOT class-based)
- Use descriptive test names
- Cover edge cases (empty, None, errors, boundaries)
- Test both success and failure paths
- Test all affected adapters
- Use pytest markers: `@pytest.mark.postgres`, `@pytest.mark.duckdb`, etc.

### For Docs & Vision Agent

**Documentation System**:

- System: Sphinx
- Location: docs/
- Build: `make docs`

**Workflow**:

1. **Documentation Phase**:
   - Update API documentation (docs/reference/)
   - Create/update guides in docs/guides/
   - Add working code examples
   - Update CHANGELOG

2. **Quality Gate Phase**:
   - Verify all PRD acceptance criteria met
   - Verify all tests passing
   - Check AGENTS.md compliance
   - BLOCK if criteria not met

3. **Knowledge Capture Phase** (NEW!):
   - Extract patterns from implementation
   - Update AGENTS.md with new patterns
   - Update relevant docs/guides/ with patterns
   - Document with working examples

4. **Re-validation Phase** (NEW!):
   - Re-run tests after updates
   - Rebuild docs (`make docs`)
   - Check pattern consistency
   - BLOCK if validation fails

5. **Cleanup & Archive Phase**:
   - Clean tmp/ directory
   - Move to specs/archive/
   - Generate completion report

## Blockers

{Any blockers or dependencies}

## Questions

{Open questions for user or other agents}

## SQLSpec-Specific Context

**Adapters Affected**:
- {List adapters}

**Core Components Affected**:
- {List components}

**Mypyc Compatibility**:
- {Any special considerations}

## Progress Log

{Running log of changes - see tasks.md for details}
