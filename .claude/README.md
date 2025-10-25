# SQLSpec Agent System

Enhanced agent system for AI-assisted SQLSpec development, compatible with Claude, Gemini, Codex, and other AI coding assistants.

**Last updated:** 2025-10-09 (Enhancement)

## Quick Start

```bash
# 1. Create PRD for a feature
/prd implement vector search for Oracle

# 2. Implement it
/implement

# 3. Create tests
/test

# 4. Review, validate, and cleanup
/review
```

## Core Agents (Enhanced System)

### 1. **PRD** - Product Requirements and Design

- Research-grounded planning (guides + Context7 + WebSearch)
- Structured multi-step plans via zen.planner
- Multi-model consensus via zen.consensus
- Creates workspace in `specs/active/{requirement}/`

**Invoke:** `/prd {feature description}`

### 2. **Expert** - Implementation

- Implements features following AGENTS.md standards
- Uses zen.debug for systematic debugging
- Uses zen.thinkdeep for complex decisions
- Uses zen.analyze for code analysis
- Updates workspace continuously

**Invoke:** `/implement`

### 3. **Testing** - Comprehensive Testing

- Creates function-based pytest tests (no classes)
- Unit tests (tests/unit/) + Integration tests (tests/integration/)
- Tests edge cases: empty, None, errors, concurrency
- Verifies coverage: 80%+ adapters, 90%+ core

**Invoke:** `/test`

### 4. **Docs & Vision** - Documentation, Quality Gate, Cleanup

Three sequential phases:

1. **Documentation** - Updates docs/guides/, API reference
2. **Quality Gate** - Validates code quality (BLOCKS if fails)
3. **Cleanup** - MANDATORY workspace cleanup and archiving

**Invoke:** `/review`

## Workspace Structure

```
specs/active/
├── {requirement-slug}/      # Active requirement
│   ├── prd.md               # Product Requirements Document
│   ├── tasks.md             # Implementation checklist
│   ├── recovery.md          # Session resume guide
│   ├── research/            # Research findings
│   │   └── plan.md
│   └── tmp/                 # Temporary files (cleaned by Docs & Vision)
├── archive/                 # Completed requirements
└── README.md
```

## Guides (Shared Across All AIs)

All guides consolidated in `docs/guides/`:

```
docs/guides/
├── adapters/           # Database adapter patterns (10 adapters)
├── performance/        # SQLglot and mypyc optimization
├── testing/            # Pytest strategies
├── architecture/       # Core design patterns
└── quick-reference/    # Common code patterns
```

These guides are the **canonical source of truth** for SQLSpec patterns.

## Workflow

### Full Feature Development

1. **PRD:** Research → Structured planning → Create workspace

   ```bash
   /prd add connection pooling for asyncpg
   ```

2. **Implement:** Read plan → Implement → Test → Update workspace

   ```bash
   /implement
   ```

3. **Test:** Create tests → Verify coverage → All tests pass

   ```bash
   /test
   ```

4. **Review:** Document → Quality gate → Cleanup

   ```bash
   /review
   ```

### Bug Fix

1. **PRD (optional):** Quick PRD for complex bugs
2. **Implement:** Use zen.debug for systematic investigation
3. **Test:** Add regression test
4. **Review:** Quality gate + cleanup

### Session Continuity

Resume work across sessions/context resets:

```python
# Find active work
Read("specs/active/{requirement}/recovery.md")  # Shows status, next steps
Read("specs/active/{requirement}/tasks.md")      # Shows what's done
Read("specs/active/{requirement}/prd.md")        # Full context
```

## Code Quality Standards

All agents enforce [AGENTS.md](../AGENTS.md) standards:

### ✅ ALWAYS

- Stringified type hints: `def foo(config: "SQLConfig"):`
- Type guards from `sqlspec.utils.type_guards`
- Clean names: `process_query()`, not `process_query_optimized()`
- Function-based tests: `def test_something():`

### ❌ NEVER

- `from __future__ import annotations`
- Defensive patterns: `hasattr()`, `getattr()`
- Workaround naming: `_optimized`, `_with_cache`, `_fallback`
- Class-based tests: `class TestSomething:`

## MCP Tools

### zen.planner

Structured multi-step planning workflow

**Used by:** PRD

### zen.consensus

Multi-model decision verification (gemini-2.5-pro, gpt-5)

**Used by:** PRD, Expert

### zen.debug

Systematic debugging workflow

**Used by:** Expert

### zen.thinkdeep

Deep analysis for complex decisions

**Used by:** Expert

### zen.analyze

Code analysis (architecture, performance, security)

**Used by:** Expert

### Context7

Up-to-date library documentation

**Used by:** All agents

### WebSearch

Current best practices (2025+)

**Used by:** All agents

## Quality Gate

**MANDATORY before work completion:**

✅ `make lint` passes
✅ No defensive patterns (hasattr/getattr)
✅ No workaround naming (_optimized, etc.)
✅ No class-based tests
✅ All tests pass
✅ Documentation complete
✅ PRD acceptance criteria met

**If quality gate fails, work is NOT complete.**

## Cleanup Protocol

**MANDATORY after every `/review`:**

1. Remove all `tmp/` directories
2. Archive completed requirement to `specs/active/archive/`
3. Keep only last 3 active requirements
4. Archive planning reports to `.claude/reports/archive/`

**Never skip cleanup.**

## Documentation

- **[AGENTS.md](AGENTS.md)** - Comprehensive agent coordination guide
- **[../docs/guides/README.md](../docs/guides/README.md)** - Index of all development guides
- **[../specs/active/README.md](../specs/active/README.md)** - Workspace structure and usage
- **[../AGENTS.md](../AGENTS.md)** - Code quality standards (mandatory reading)

## Archive

Old agent definitions and guides archived in:

- `.claude/archive/enhancement_20251009_231818/` - Previous system snapshot

## Maintenance

Run enhancement bootstrap again when:

- Tech stack changes significantly
- Want to add new MCP tools
- Agents feel outdated
- Need better cleanup protocols

## Enhancement History

- **2025-10-09:** Major enhancement
    - Consolidated guides to `docs/guides/`
    - Created 4 core agents (PRD, Expert, Testing, Docs & Vision)
    - Added `specs/active/` workspace system
    - Added mandatory cleanup protocols
    - Created workflow commands (`/prd`, `/implement`, `/test`, `/review`)
    - Archived old agents and guides

## See Also

- [Agent Coordination Guide](AGENTS.md) - Detailed agent workflows
- [Code Quality Standards](../AGENTS.md) - MANDATORY coding standards
- [Development Guides](../docs/guides/) - Canonical pattern reference
- [Workspace Guide](../specs/active/README.md) - Workspace management
