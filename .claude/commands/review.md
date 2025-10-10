Run documentation, quality gate, and mandatory cleanup.

Invoke the Docs & Vision agent to run all 3 phases:

## Phase 1: Documentation
- Update docs/guides/ as needed
- Update API reference (docs/reference/)
- Add usage examples
- Build docs locally to verify

## Phase 2: Quality Gate (MANDATORY)
- Run `make lint` - must pass
- Check for anti-patterns (hasattr, workaround naming, class tests)
- Run full test suite - must pass
- Verify PRD acceptance criteria met

**Quality gate MUST pass before proceeding to cleanup.**

## Phase 3: Cleanup (MANDATORY)
- Remove all .agents/*/tmp/ directories
- Archive completed requirement to .agents/archive/
- Keep only last 3 active requirements in .agents/
- Archive planning reports to .claude/reports/archive/
- Verify workspace is clean

**Cleanup is MANDATORY - never skip this phase.**

After review complete, work is ready for commit/PR.

Final step: Run `make lint && make test` one more time, then commit!
