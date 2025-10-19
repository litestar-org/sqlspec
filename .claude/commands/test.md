Create comprehensive tests for the implemented feature.

Invoke the Testing agent to:

1. **Read Implementation** - Load prd.md, recovery.md from specs/active/{requirement}/
2. **Create Unit Tests** - Test individual components in isolation
3. **Create Integration Tests** - Test with real database connections
4. **Test Edge Cases** - Empty inputs, None values, errors, concurrency
5. **Validate Coverage** - Ensure >80% coverage for adapters, >90% for core
6. **Update Workspace** - Mark test tasks complete

The testing agent should:

- Use function-based tests (NO class-based tests)
- Follow pytest patterns from docs/guides/testing/testing.md
- Test all affected database adapters
- Use pytest markers (@pytest.mark.postgres, etc.)
- Verify all tests pass before returning

**Note:** This command is typically not needed manually because `/implement` automatically invokes the Testing agent. Use this only if you need to:
- Re-create tests after manual changes
- Add additional test coverage
- Debug test failures

After testing, Expert agent will auto-invoke Docs & Vision for documentation, quality gate, knowledge capture, and archival.
