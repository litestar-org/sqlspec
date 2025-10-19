# Agent Coordination Guide

Comprehensive guide for the SQLSpec multi-agent workflow system. All agents (Claude Code, Gemini, Codex, etc.) should follow this workflow.

## Quick Reference

- **Active work**: `specs/active/{requirement}/`
- **Archived work**: `specs/archive/{requirement}/`
- **Templates**: `specs/template-spec/`
- **Main standards**: `AGENTS.md` (SQLSpec-specific patterns)

## Agent Responsibilities

| Agent | Primary Role | Key Tools | Auto-Invoked By |
|-------|--------------|-----------|-----------------|
| **Planner** | Research & planning | zen.planner, zen.consensus, Context7, WebSearch | User (`/plan`) |
| **Expert** | Implementation & orchestration | zen.debug, zen.thinkdeep, zen.analyze, Context7 | User (`/implement`) |
| **Testing** | Comprehensive test creation | pytest, Bash | Expert (automatic) |
| **Docs & Vision** | Documentation, QA, knowledge capture, cleanup | Sphinx, Bash | Expert (automatic) |

## Complete Workflow

```
User runs: /plan {feature-description}
    ↓
┌─────────────────────────────────────────────────────────────┐
│                    PLANNER AGENT                             │
│  • Research (guides, Context7, WebSearch)                   │
│  • Use zen.planner for structured planning                  │
│  • Get zen.consensus on complex decisions                   │
│  • Create workspace: specs/active/{requirement}/            │
│  • Write: prd.md, tasks.md, research/plan.md, recovery.md  │
└─────────────────────────────────────────────────────────────┘
    ↓
User runs: /implement
    ↓
┌─────────────────────────────────────────────────────────────┐
│                    EXPERT AGENT                              │
│  1. Read plan (prd.md, tasks.md, research/plan.md)         │
│  2. Research (guides, Context7)                             │
│  3. Implement following AGENTS.md standards                 │
│  4. Self-test & verify                                      │
│  5. ──► Auto-Invoke Testing Agent (subagent)               │
│         ├─► Create unit tests                              │
│         ├─► Create integration tests                       │
│         ├─► Test edge cases                                │
│         └─► Verify coverage & all tests pass               │
│  6. ──► Auto-Invoke Docs & Vision Agent (subagent)         │
│         ├─► Phase 1: Update documentation                  │
│         ├─► Phase 2: Quality gate validation               │
│         ├─► Phase 3: Knowledge capture (AGENTS.md+guides)  │
│         ├─► Phase 4: Re-validate after updates             │
│         ├─► Phase 5: Clean tmp/ and archive                │
│         └─► Generate completion report                      │
│  7. Return complete summary                                 │
└─────────────────────────────────────────────────────────────┘
```

## Agent-Specific Guidance

### For Planner Agent

**Responsibilities:**

- Research-grounded planning
- Structured planning with zen.planner
- Multi-model consensus for complex decisions
- Workspace creation in `specs/active/`

**Workflow:**

```python
# 1. Research first
Read("docs/guides/...")
mcp__context7__get-library-docs(...)
WebSearch(query="...")

# 2. Use zen.planner for structured planning
mcp__zen__planner(
    step="Plan vector search implementation",
    step_number=1,
    total_steps=6,
    next_step_required=True
)

# 3. Get consensus on complex decisions
mcp__zen__consensus(
    step="Evaluate: Protocol vs ABC for driver base",
    models=[
        {"model": "gemini-2.5-pro", "stance": "neutral"},
        {"model": "openai/gpt-5", "stance": "neutral"}
    ],
    relevant_files=["sqlspec/protocols.py"],
    next_step_required=False
)

# 4. Create workspace
Write("specs/active/{requirement}/prd.md", ...)
Write("specs/active/{requirement}/tasks.md", ...)
Write("specs/active/{requirement}/research/plan.md", ...)
Write("specs/active/{requirement}/recovery.md", ...)
```

**Output:** Complete workspace in `specs/active/{requirement}/`

### For Expert Agent

**Responsibilities:**

- Implementation following AGENTS.md standards
- Auto-invoke Testing agent when implementation complete
- Auto-invoke Docs & Vision agent after tests pass
- Orchestrate entire development lifecycle

**Workflow:**

```python
# 1. Read the plan
Read("specs/active/{requirement}/prd.md")
Read("specs/active/{requirement}/tasks.md")
Read("specs/active/{requirement}/research/plan.md")

# 2. Research implementation details
Read(f"docs/guides/adapters/{adapter}.md")
Read("docs/guides/performance/sqlglot-best-practices.md")
Read("AGENTS.md")  # Code quality standards
mcp__context7__get-library-docs(...)  # Library-specific docs

# 3. Implement with quality standards (see AGENTS.md)
Edit(file_path="...", old_string="...", new_string="...")

# 4. Self-test
Bash(command="uv run pytest tests/integration/test_adapters/test_asyncpg/ -v")
Bash(command="make lint")

# 5. AUTO-INVOKE Testing Agent (MANDATORY)
Task(
    subagent_type="testing",
    description="Create comprehensive test suite",
    prompt=f"""
Create comprehensive tests for specs/active/{requirement}.

Requirements:
1. Read specs/active/{requirement}/prd.md for acceptance criteria
2. Create unit tests for all new functionality
3. Create integration tests for affected adapters
4. Test edge cases (empty, errors, boundaries)
5. Achieve >80% coverage
6. Update specs/active/{requirement}/tasks.md
7. Update specs/active/{requirement}/recovery.md

All tests must pass before returning control.
"""
)

# 6. AUTO-INVOKE Docs & Vision Agent (MANDATORY)
Task(
    subagent_type="docs-vision",
    description="Documentation, quality gate, knowledge capture, archive",
    prompt=f"""
Complete 5-phase workflow for specs/active/{requirement}:

Phase 1 - Documentation:
• Update Sphinx documentation
• Create/update guides in docs/guides/
• Validate code examples
• Build docs without errors

Phase 2 - Quality Gate:
• Verify all PRD acceptance criteria met
• Verify all tests passing
• Check AGENTS.md standards compliance
• BLOCK if any criteria not met

Phase 3 - Knowledge Capture (MANDATORY):
• Analyze implementation for new patterns
• Extract best practices and conventions
• Update AGENTS.md with new patterns/examples
• Update relevant guides in docs/guides/
• Document with working code examples

Phase 4 - Re-validation (MANDATORY):
• Re-run tests after documentation updates
• Rebuild documentation
• Check pattern consistency
• Verify no breaking changes
• BLOCK if re-validation fails

Phase 5 - Cleanup & Archive (MANDATORY):
• Remove all tmp/ directories
• Move specs/active/{requirement} to specs/archive/
• Generate completion report

Return comprehensive summary when complete.
"""
)

# 7. Update workspace
Edit("specs/active/{requirement}/tasks.md", ...)
Edit("specs/active/{requirement}/recovery.md", ...)
```

**IMPORTANT:** Expert MUST NOT mark work complete until Docs & Vision agent confirms:

- Quality gate passed
- Knowledge captured in AGENTS.md and guides
- Spec archived to specs/archive/

### For Testing Agent

**Responsibilities:**

- Create comprehensive unit tests
- Create integration tests for all affected adapters
- Test edge cases and error conditions
- Achieve required coverage (>80% adapters, >90% core)

**Auto-invoked by:** Expert agent

**Workflow:**

```python
# 1. Read implementation context
Read("specs/active/{requirement}/prd.md")
Read("specs/active/{requirement}/recovery.md")
Read("docs/guides/testing/testing.md")

# 2. Create unit tests
Write("tests/unit/test_{module}/test_{feature}.py", ...)

# 3. Create integration tests
Write("tests/integration/test_adapters/test_{adapter}/test_{feature}.py", ...)

# 4. Verify all tests pass
Bash(command="uv run pytest -n 2 --dist=loadgroup")
Bash(command="uv run pytest --cov")

# 5. Update workspace
Edit("specs/active/{requirement}/tasks.md", ...)
Edit("specs/active/{requirement}/recovery.md", ...)
```

**Must verify:** All tests passing before returning control to Expert

### For Docs & Vision Agent

**Responsibilities:**

- Update documentation (Sphinx, guides)
- Quality gate validation (BLOCKS if fails)
- **Knowledge capture** - Update AGENTS.md and guides with new patterns
- **Re-validation** - Verify consistency after documentation updates
- Cleanup and archive (MANDATORY)

**Auto-invoked by:** Expert agent

**5-Phase Workflow:**

#### Phase 1: Documentation

```python
# Update API reference
Edit("docs/reference/adapters.rst", ...)

# Create/update guides
Write("docs/guides/{category}/{guide}.md", ...)

# Build docs
Bash(command="make docs")
```

#### Phase 2: Quality Gate (BLOCKS IF FAILS)

```bash
# Run all checks - MUST PASS
make lint
uv run pytest -n 2 --dist=loadgroup

# Verify PRD acceptance criteria
# Check AGENTS.md standards compliance
# BLOCK if any failures
```

#### Phase 3: Knowledge Capture (MANDATORY - NEW)

```python
# 1. Analyze implementation for new patterns
Read("sqlspec/adapters/{adapter}/{module}.py")

# 2. Extract patterns and add to AGENTS.md
Edit(
    file_path="AGENTS.md",
    old_string="### Compliance Table",
    new_string="""### New Pattern: {Pattern Name}

When implementing {pattern}:

```python
# Example code showing the pattern
class ExampleClass:
    def example_method(self):
        # Pattern implementation
        pass
```

**Why this pattern:**

- Reason 1
- Reason 2

**Example from {adapter}:**
See `sqlspec/adapters/{adapter}/{file}.py:{line}`

### Compliance Table"""

)

# 3. Update relevant guides

Edit("docs/guides/{category}/{guide}.md", ...)

# 4. Document with working examples

```

**What to capture in AGENTS.md:**
- New design patterns discovered
- Performance optimizations applied
- Type annotation patterns
- Error handling approaches
- Testing strategies
- Database-specific patterns

**How to update AGENTS.md:**
- Add to existing sections (don't create new top-level sections)
- Include working code examples
- Reference actual files with line numbers
- Explain WHY not just WHAT

#### Phase 4: Re-validation (MANDATORY - NEW)

```bash
# Re-run tests after doc updates
uv run pytest -n 2 --dist=loadgroup

# Rebuild docs
make docs

# Verify pattern consistency across project
# Check no breaking changes introduced

# BLOCK if re-validation fails
```

#### Phase 5: Cleanup & Archive (MANDATORY)

```bash
# Remove tmp files
find specs/active/{requirement}/tmp -type f -delete
rmdir specs/active/{requirement}/tmp

# Archive requirement
mv specs/active/{requirement} specs/archive/{requirement}

# Generate completion report
```

## Workspace Structure

```
specs/
├── active/                    # Current work (gitignored)
│   └── {requirement}/
│       ├── prd.md            # Product requirements
│       ├── tasks.md          # Implementation checklist
│       ├── recovery.md       # Session resume guide
│       ├── research/         # Research findings
│       │   └── plan.md
│       └── tmp/              # Temp files (cleaned by Docs & Vision)
├── archive/                   # Completed work (gitignored)
│   └── {old-requirement}/
└── template-spec/             # Template files (committed)
    ├── prd.md
    ├── tasks.md
    ├── recovery.md
    └── README.md
```

## Cross-Agent Patterns

### Session Continuity

To resume work after context reset:

```python
# 1. Find active work
Glob("specs/active/*/prd.md")

# 2. Read recovery.md for each
Read("specs/active/{requirement}/recovery.md")

# 3. Resume from most recent
Read("specs/active/{requirement}/tasks.md")
```

### Quality Standards

All agents enforce AGENTS.md standards:

✅ **ALWAYS:**

- Stringified type hints: `def foo(config: "SQLConfig"):`
- Type guards: `if supports_where(obj):`
- Function-based tests: `def test_something():`
- Functions under 75 lines

❌ **NEVER:**

- `from __future__ import annotations`
- Defensive patterns: `hasattr()`, `getattr()`
- Class-based tests: `class TestSomething:`
- Nested imports (except TYPE_CHECKING)

See `AGENTS.md` for complete standards.

## Model-Specific Instructions

### For Codex

Codex can emulate slash commands by following agent workflows:

**To run implementation phase:**

```
Follow every step in expert.md workflow, then auto-invoke Testing and Docs & Vision agents as subagents.
Always read specs/active/{requirement}/ before making changes.
```

See `.claude/agents/expert.md` lines 22-23 for details.

### For Gemini

Gemini can use the same workflow patterns. When asked to "implement feature X":

1. **Check for existing workspace**: `Glob("specs/active/*/prd.md")`
2. **If workspace exists**: Follow Expert workflow (read plan → research → implement → auto-invoke Testing → auto-invoke Docs & Vision)
3. **If no workspace**: Suggest user run `/plan` first, or create minimal workspace yourself

**Gemini-specific macro for implementation:**

```
You are implementing {feature} for SQLSpec. Follow this workflow:

PHASE 1 - Read Plan:
• Read specs/active/{requirement}/prd.md
• Read specs/active/{requirement}/tasks.md
• Read specs/active/{requirement}/research/plan.md

PHASE 2 - Research:
• Read AGENTS.md for code standards
• Read docs/guides/adapters/{adapter}.md
• Read docs/guides/performance/sqlglot-best-practices.md
• Use zen.chat or Context7 for library docs

PHASE 3 - Implement:
• Follow AGENTS.md standards ruthlessly
• Self-test with: uv run pytest tests/...
• Update specs/active/{requirement}/tasks.md

PHASE 4 - Auto-Invoke Testing Agent:
• Use zen.chat with testing agent instructions
• Pass requirement name and acceptance criteria
• Wait for confirmation all tests pass

PHASE 5 - Auto-Invoke Docs & Vision Agent:
• Use zen.chat with docs-vision agent instructions
• Pass requirement name
• Wait for 5-phase completion (docs, QA, knowledge, re-validation, archive)

PHASE 6 - Complete:
• Verify spec archived to specs/archive/
• Return comprehensive summary
```

### For Claude Code

Claude Code uses native slash commands:

- `/plan {feature}` - Invokes Planner agent
- `/implement` - Invokes Expert agent (auto-invokes Testing and Docs & Vision)
- `/test` - Directly invokes Testing agent (rarely needed)
- `/review` - Directly invokes Docs & Vision agent (rarely needed)

## MCP Tools Reference

### zen.planner

- **Who**: Planner agent
- **When**: Structured multi-step planning
- **Pattern**: Iterative steps with branching/revision

### zen.consensus

- **Who**: Planner, Expert
- **When**: Complex architectural decisions
- **Pattern**: Multi-model consultation with stances

### zen.debug

- **Who**: Expert
- **When**: Systematic debugging, root cause analysis
- **Pattern**: Hypothesis-driven investigation

### zen.thinkdeep

- **Who**: Expert
- **When**: Deep analysis for complex decisions
- **Pattern**: Multi-step reasoning with evidence

### zen.analyze

- **Who**: Expert
- **When**: Code analysis (architecture, performance, security)
- **Pattern**: Systematic code review

### zen.chat

- **Who**: Any agent (especially Gemini)
- **When**: General collaboration, brainstorming, sub-agent emulation
- **Pattern**: Conversational with context files

### Context7

- **Who**: All agents
- **When**: Need library documentation
- **Pattern**: Resolve library ID → Get docs with topic

### WebSearch

- **Who**: All agents
- **When**: Research current best practices (2025+)
- **Pattern**: Search query with date filtering

## Summary

This workflow ensures:

✅ **Automated orchestration** - Expert agent handles entire lifecycle
✅ **Knowledge preservation** - Every feature updates AGENTS.md and guides
✅ **Quality assurance** - Multi-phase validation before completion
✅ **Session continuity** - Workspace enables cross-session resume
✅ **Multi-model support** - Works with Claude Code, Gemini, Codex

The key innovation: **Knowledge Capture (Phase 3)** and **Re-validation (Phase 4)** ensure every feature improves the project's documentation and maintains consistency.
