# {Feature Name}

This workspace contains the planning, implementation, testing, and documentation for the {Feature Name} feature in SQLSpec.

## Quick Links

- [PRD (Product Requirements Document)](prd.md)
- [Tasks Checklist](tasks.md)
- [Recovery Guide](recovery.md)
- [Research Findings](research/)
- [SQLSpec Standards](../../AGENTS.md)

## Status

**Current Phase**: {Phase}
**Status**: {In Progress / Complete}
**Last Updated**: {Date}

## Workflow

This spec follows the automated SQLSpec workflow:

1. **PRD** (`/prd`) - PRD agent creates this workspace
2. **Implement** (`/implement`) - Expert agent:
   - Implements feature following AGENTS.md standards
   - Auto-invokes Testing agent (creates comprehensive tests)
   - Auto-invokes Docs & Vision agent (docs, QA, knowledge capture, archive)
3. **Complete** - Everything done automatically!

## Files

- `prd.md` - Full requirements and acceptance criteria
- `tasks.md` - Phase-by-phase checklist
- `recovery.md` - Resume guide for any agent
- `research/` - Expert findings and analysis
- `tmp/` - Temporary files (cleaned by Docs & Vision)

## Automated Phases

When you run `/implement`, the Expert agent automatically handles:

✅ Implementation (following SQLSpec patterns)
✅ Testing (via Testing agent - all adapters)
✅ Documentation (via Docs & Vision agent)
✅ Quality Gate (via Docs & Vision agent)
✅ **Knowledge Capture** (patterns added to AGENTS.md)
✅ **Re-validation** (after knowledge updates)
✅ Archival (moved to specs/archive/)

No manual steps required!

## SQLSpec-Specific Context

**Key Standards** (from AGENTS.md):
- Type hints: Stringified for non-builtins, NO `from __future__ import annotations`
- Tests: Function-based ONLY (not class-based)
- Imports: No nested imports unless preventing circular imports
- Patterns: Protocol-based, not inheritance
- Documentation: Google-style docstrings

**Testing Against**:
- Multiple database adapters (AsyncPG, Oracle, DuckDB, etc.)
- Real databases via pytest-databases
- Mypyc compilation compatibility

**Resources**:
- [Architecture Guide](../../docs/guides/architecture/architecture.md)
- [Testing Guide](../../docs/guides/testing/testing.md)
- [Adapter Patterns](../../docs/guides/adapters/)
- [AGENTS.md](../../AGENTS.md) - **READ THIS FIRST**
