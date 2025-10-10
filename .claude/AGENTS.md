# Agent Coordination Guide

Comprehensive guide for the SQLSpec agent system, covering agent responsibilities, workflow patterns, MCP tool usage, and workspace management.

## Agent Responsibilities Matrix

| Responsibility | Planner | Expert | Testing | Docs & Vision |
|----------------|---------|--------|---------|---------------|
| **Research** | ✅ Primary | ✅ Implementation details | ✅ Test patterns | ✅ Doc standards |
| **Planning** | ✅ Primary | ❌ | ❌ | ❌ |
| **Implementation** | ❌ | ✅ Primary | ✅ Tests only | ❌ |
| **Testing** | ❌ | ✅ Verify own code | ✅ Primary | ✅ Run quality gate |
| **Documentation** | ✅ PRD/tasks | ✅ Code comments | ✅ Test docs | ✅ Primary |
| **Quality Gate** | ❌ | ❌ | ❌ | ✅ Primary |
| **Cleanup** | ❌ | ❌ | ❌ | ✅ MANDATORY |
| **Multi-Model Consensus** | ✅ Primary | ✅ Complex decisions | ❌ | ❌ |
| **Workspace Management** | ✅ Create | ✅ Update | ✅ Update | ✅ Archive & Clean |

## Workflow Phases

### Phase 1: Planning (`/plan`)

**Agent:** Planner
**Purpose:** Research-grounded planning and workspace creation

**Steps:**

1. Research guides, Context7, WebSearch
2. Create structured plan with zen.planner
3. Get consensus on complex decisions (zen.consensus)
4. Create workspace in `.agents/{requirement}/`
5. Write PRD, tasks, research, recovery docs

**Output:**

```
.agents/{requirement-slug}/
├── prd.md          # Product Requirements Document
├── tasks.md        # Implementation checklist
├── research/       # Research findings
│   └── plan.md    # Detailed plan
├── tmp/            # Temporary files
└── recovery.md     # Session resume guide
```

**Hand off to:** Expert agent for implementation

### Phase 2: Implementation (`/implement`)

**Agent:** Expert
**Purpose:** Write clean, type-safe, performant code

**Steps:**

1. Read workspace (prd.md, tasks.md, research/plan.md)
2. Research implementation details (guides, Context7)
3. Implement following CLAUDE.md standards
4. Run tests to verify
5. Update workspace (tasks.md, recovery.md)

**Tools Used:**

- zen.debug (systematic debugging)
- zen.thinkdeep (complex decisions)
- zen.analyze (code analysis)
- Context7 (library docs)

**Output:**

- Production code in sqlspec/
- Updated workspace files

**Hand off to:** Testing agent for comprehensive tests

### Phase 3: Testing (`/test`)

**Agent:** Testing
**Purpose:** Create comprehensive unit and integration tests

**Steps:**

1. Read implementation
2. Consult testing guide
3. Create unit tests (tests/unit/)
4. Create integration tests (tests/integration/)
5. Test edge cases
6. Verify coverage (80%+ adapters, 90%+ core)
7. Update workspace

**Output:**

- Unit tests in tests/unit/
- Integration tests in tests/integration/
- Updated workspace files

**Hand off to:** Docs & Vision for documentation and quality gate

### Phase 4: Review (`/review`)

**Agent:** Docs & Vision
**Purpose:** Documentation, quality gate, and MANDATORY cleanup

**3 Sequential Phases:**

1. **Documentation:**
   - Update docs/guides/
   - Update API reference
   - Build docs locally

2. **Quality Gate (MANDATORY):**
   - Run `make lint` (must pass)
   - Check for anti-patterns (hasattr, workaround naming, class tests)
   - Run full test suite (must pass)
   - Verify PRD acceptance criteria
   - **BLOCKS if quality gate fails**

3. **Cleanup (MANDATORY):**
   - Remove all tmp/ directories
   - Archive requirement to .agents/archive/
   - Keep only last 3 active requirements
   - Archive planning reports

**Output:**

- Complete documentation
- Clean workspace
- Archived requirement
- Work ready for PR/commit

## Agent Invocation Patterns

### Planner Invoking Consensus

For complex architectural decisions:

```python
Task(
    subagent_type="general-purpose",
    description="Get multi-model consensus on API design",
    prompt="""
Use zen.consensus to get multi-model agreement:

Question: Should we use Protocol or ABC for driver base class?

Models to consult:
- gemini-2.5-pro (neutral stance)
- openai/gpt-5 (neutral stance)

Include relevant files for context:
- sqlspec/protocols.py
- sqlspec/adapters/asyncpg/driver.py

Write consensus findings to .agents/{requirement}/research/consensus.md
"""
)
```

### Expert Invoking Debugging

For systematic debugging:

```python
Task(
    subagent_type="general-purpose",
    description="Debug asyncpg connection pool deadlock",
    prompt="""
Use zen.debug for systematic debugging:

Problem: AsyncPG connection pool deadlocks under high load

Use zen.debug to:
1. State hypothesis about root cause
2. Investigate code paths
3. Check for leaked connections
4. Verify pool configuration
5. Test fix

Write findings to .agents/{requirement}/tmp/debug-{issue}.md
"""
)
```

### Testing Invoking Test Generation

Testing agent is usually NOT invoked by other agents - it's invoked directly via `/test` command.

### Docs & Vision Blocking on Quality Gate

Quality gate BLOCKS completion if standards not met:

```markdown
❌ QUALITY GATE FAILED

Issues found:
- 3 files with hasattr() defensive patterns
- 2 tests using class-based structure
- make lint has 5 errors

⚠️ WORK NOT COMPLETE
Do NOT proceed to cleanup phase.
Fix issues above and re-run quality gate.
```

## MCP Tools Matrix

### Tool: zen.planner

**Who uses:** Planner agent
**Purpose:** Structured, multi-step planning
**When:** Creating detailed implementation plans

**Example:**

```python
mcp__zen__planner(
    step="Plan vector search implementation for Oracle and PostgreSQL",
    step_number=1,
    total_steps=6,
    next_step_required=True
)
```

### Tool: zen.consensus

**Who uses:** Planner, Expert
**Purpose:** Multi-model decision verification
**When:** Complex architectural decisions, significant API changes

**Example:**

```python
mcp__zen__consensus(
    step="Evaluate: Protocol vs ABC for driver base class",
    models=[
        {"model": "gemini-2.5-pro", "stance": "neutral"},
        {"model": "openai/gpt-5", "stance": "neutral"}
    ],
    relevant_files=["sqlspec/protocols.py"],
    next_step_required=False
)
```

### Tool: zen.debug

**Who uses:** Expert
**Purpose:** Systematic debugging workflow
**When:** Complex bugs, mysterious errors, performance issues

**Example:**

```python
mcp__zen__debug(
    step="Investigate memory leak in long-running connections",
    step_number=1,
    total_steps=5,
    hypothesis="Result cache holds strong references to connection objects",
    findings="Cache never evicts old entries",
    confidence="medium",
    next_step_required=True
)
```

### Tool: zen.thinkdeep

**Who uses:** Expert
**Purpose:** Deep analysis for complex decisions
**When:** Architecture decisions, complex refactoring

**Example:**

```python
mcp__zen__thinkdeep(
    step="Analyze if we should use Protocol vs ABC for driver base class",
    step_number=1,
    total_steps=3,
    hypothesis="Protocol better for runtime type checking without inheritance",
    findings="Protocols work with type guards, avoid diamond problem",
    focus_areas=["architecture", "performance"],
    confidence="high",
    next_step_required=True
)
```

### Tool: zen.analyze

**Who uses:** Expert
**Purpose:** Code analysis (architecture, performance, security)
**When:** Code review, performance optimization, security audit

**Example:**

```python
mcp__zen__analyze(
    step="Analyze oracle adapter for performance bottlenecks",
    step_number=1,
    total_steps=3,
    analysis_type="performance",
    findings="Found N+1 query pattern in result mapping",
    confidence="high",
    next_step_required=True
)
```

### Tool: Context7

**Who uses:** All agents
**Purpose:** Get up-to-date library documentation
**When:** Need current API reference for libraries (asyncpg, oracledb, etc.)

**Example:**

```python
# Step 1: Resolve library ID
mcp__context7__resolve-library-id(libraryName="asyncpg")

# Step 2: Get docs
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/MagicStack/asyncpg",
    topic="connection pooling"
)
```

### Tool: WebSearch

**Who uses:** All agents
**Purpose:** Research current best practices (2025+)
**When:** Need recent best practices, database-specific patterns

**Example:**

```python
WebSearch(query="PostgreSQL 16 connection pooling best practices 2025")
```

## Workspace Management

### Structure

```
.agents/
├── {requirement-1}/      # Active requirement
│   ├── prd.md
│   ├── tasks.md
│   ├── recovery.md
│   ├── research/
│   │   └── plan.md
│   └── tmp/              # Cleaned by Docs & Vision
├── {requirement-2}/      # Active requirement
├── {requirement-3}/      # Active requirement
├── archive/              # Completed requirements
│   └── {old-requirement}/
└── README.md
```

### Cleanup Protocol (MANDATORY)

**When:** After every `/review` (Docs & Vision agent)

**Steps:**

1. Remove all tmp/ directories:

   ```bash
   find .agents/*/tmp -type d -exec rm -rf {} +
   ```

2. Archive completed requirement:

   ```bash
   mv .agents/{requirement} .agents/archive/{requirement}
   ```

3. Keep only last 3 active requirements:

   ```bash
   # If more than 3 active, move oldest to archive
   ```

**This is MANDATORY - never skip cleanup.**

### Session Continuity

To resume work across sessions/context resets:

```python
# 1. List active requirements
Glob(".agents/*/prd.md")

# 2. Read recovery.md to understand status
Read(".agents/{requirement}/recovery.md")

# 3. Check task progress
Read(".agents/{requirement}/tasks.md")

# 4. Review PRD for full context
Read(".agents/{requirement}/prd.md")

# 5. Review planning details
Read(".agents/{requirement}/research/plan.md")
```

## Code Quality Standards

All agents MUST enforce CLAUDE.md standards:

### ✅ ALWAYS DO

- **Type hints:** Stringified for custom types: `def foo(config: "SQLConfig"):`
- **Type guards:** `if supports_where(obj):` from `sqlspec.utils.type_guards`
- **Clean names:** `process_query()`, `execute_batch()`
- **Top-level imports:** Except TYPE_CHECKING
- **Function-based tests:** `def test_something():`
- **Early returns:** Guard clauses for edge cases
- **Functions under 75 lines:** Extract helpers if longer

### ❌ NEVER DO

- **Future annotations:** `from __future__ import annotations`
- **Defensive patterns:** `hasattr()`, `getattr()`
- **Workaround naming:** `_optimized`, `_with_cache`, `_fallback`
- **Nested imports:** Except TYPE_CHECKING
- **Class-based tests:** `class TestSomething:`
- **Magic numbers:** Use named constants
- **Comments:** Use docstrings instead

## Guides Reference

All agents should consult guides before implementing:

### Adapter Guides

```
docs/guides/adapters/
├── adbc.md
├── aiosqlite.md
├── asyncmy.md
├── bigquery.md
├── duckdb.md
├── mysql.md
├── oracle.md          # Most comprehensive
├── postgres.md
├── psqlpy.md
└── sqlite.md
```

### Performance Guides

```
docs/guides/performance/
├── sqlglot-best-practices.md
├── sqlglot-cheat-sheet.md
├── mypyc-optimizations.md
└── mypyc-guide.md
```

### Architecture Guides

```
docs/guides/architecture/
├── architecture.md
└── data-flow.md
```

### Testing Guide

```
docs/guides/testing/
└── testing.md
```

### Quick Reference

```
docs/guides/quick-reference/
└── quick-reference.md
```

## Recovery Patterns

### After Context Reset

```python
# 1. Find active work
active_requirements = Glob(".agents/*/prd.md")

# 2. For each active requirement, check status
for req_prd in active_requirements:
    req_dir = req_prd.parent
    recovery = Read(f"{req_dir}/recovery.md")
    # Shows: Status, Last updated, Next steps

# 3. Resume from most recent
Read("{most_recent_requirement}/recovery.md")  # Clear next steps
Read("{most_recent_requirement}/tasks.md")      # See what's done
```

### After Session Timeout

Same as context reset - recovery.md has all needed info.

### After Cleanup

If requirement archived:

```python
# Find in archive
Glob(".agents/archive/*/prd.md")

# Can still read archived requirements
Read(".agents/archive/{requirement}/recovery.md")
```

## Command Workflow

### Full Feature Development

```bash
# 1. Plan
/plan implement vector search for Oracle and PostgreSQL

# Creates: .agents/vector-search/

# 2. Implement
/implement

# Modifies code, updates workspace

# 3. Test
/test

# Creates tests, verifies passing

# 4. Review (3 phases: docs → quality gate → cleanup)
/review

# Phase 1: Documentation
# Phase 2: Quality gate (must pass)
# Phase 3: Cleanup (mandatory)

# Result: .agents/vector-search/ → .agents/archive/vector-search/
```

### Bug Fix Workflow

```bash
# 1. Plan (optional for simple bugs)
/plan fix connection pool deadlock in asyncpg

# 2. Debug and implement
/implement

# Expert uses zen.debug for systematic investigation

# 3. Test
/test

# Add regression test

# 4. Review
/review

# Quality gate + cleanup
```

## Best Practices

### For Planner

1. **Always research first** - guides, Context7, WebSearch
2. **Use zen.planner** for complex work
3. **Get consensus** on significant decisions
4. **Create complete workspace** - don't skip files
5. **Write clear recovery.md** - enable easy resume

### For Expert

1. **Read the plan first** - don't guess
2. **Consult guides** - adapters, performance, architecture
3. **Use zen tools** for complex work (debug, thinkdeep, analyze)
4. **Follow CLAUDE.md** ruthlessly
5. **Update workspace** continuously
6. **Test as you go** - don't wait for Testing agent

### For Testing

1. **Consult testing guide** before creating tests
2. **Function-based tests** always (no classes)
3. **Mark appropriately** - @pytest.mark.asyncio, @pytest.mark.postgres, etc.
4. **Test edge cases** - empty, None, errors, concurrency
5. **Verify coverage** - 80%+ adapters, 90%+ core
6. **All tests must pass** before handoff

### For Docs & Vision

1. **Phase 1 (Docs)** - Comprehensive and clear
2. **Phase 2 (Quality Gate)** - BLOCK if standards not met
3. **Phase 3 (Cleanup)** - MANDATORY, never skip
4. **Archive systematically** - maintain clean workspace
5. **Final verification** - one last `make lint && make test`

## Troubleshooting

### Quality Gate Failing

```markdown
**Problem:** Quality gate keeps failing

**Solution:**
1. Check specific failure reasons
2. Fix anti-patterns (hasattr, workaround naming, class tests)
3. Run `make fix` to auto-fix lint issues
4. Re-run quality gate
5. DO NOT proceed to cleanup until passing
```

### Workspace Getting Cluttered

```markdown
**Problem:** .agents/ has too many folders

**Solution:**
1. Run `/review` on oldest requirements
2. Let Docs & Vision archive them
3. Manually archive if needed:
   mv .agents/{old-requirement} .agents/archive/
4. Keep only 3 active requirements
```

### Lost Context Across Sessions

```markdown
**Problem:** Can't remember what I was working on

**Solution:**
1. Read .agents/*/recovery.md for all active requirements
2. Each recovery.md has:
   - Current status
   - Last updated date
   - Next steps
3. Resume from most recent
```

## Summary

This agent system provides:

✅ **Structured workflow** - Plan → Implement → Test → Review
✅ **Quality enforcement** - CLAUDE.md standards mandatory
✅ **Research-grounded** - Guides + Context7 + WebSearch
✅ **Session continuity** - Workspace enables resume
✅ **Cleanup protocol** - Mandatory workspace management
✅ **MCP tool integration** - zen.planner, zen.debug, zen.consensus, Context7

All agents work together to ensure high-quality, well-tested, well-documented code that follows SQLSpec's strict standards.
