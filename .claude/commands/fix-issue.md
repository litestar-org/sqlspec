---
description: Fix a GitHub issue with full workflow automation
allowed-tools: Read, Write, Edit, Glob, Grep, Bash, Task, mcp__github__*, mcp__zen__debug
---

# Fix GitHub Issue

Fixing issue: **$ARGUMENTS**

## Workflow

### Step 1: Fetch Issue Details

Extract issue number from $ARGUMENTS and fetch details:

```bash
# Parse issue number (supports "#123", "123", or full URL)
ISSUE_NUM=$(echo "$ARGUMENTS" | grep -oE '[0-9]+' | head -n1)

# Fetch issue details using GitHub CLI
gh issue view $ISSUE_NUM --repo litestar-org/sqlspec --json title,body,labels,assignees
```

**Key information to extract:**
- Issue title and description
- Labels (bug, enhancement, documentation, etc.)
- Current status and assignees
- Linked PRs or related issues

### Step 2: Analyze Issue

Categorize the issue type and determine approach:

| Issue Type | Labels | Approach | Auto-Invoke After Fix |
|------------|--------|----------|----------------------|
| **Bug Fix** | `bug`, `type: bug` | Use zen.debug for root cause analysis | Testing agent → Docs & Vision agent |
| **Feature** | `enhancement`, `feature` | Create workspace → implement | Testing agent → Docs & Vision agent |
| **Documentation** | `documentation`, `docs` | Update docs/guides/ directly | Docs & Vision agent only |
| **Performance** | `performance`, `optimization` | Use zen.analyze for profiling | Testing agent → Docs & Vision agent |
| **Adapter** | `adapter: *` | Follow adapter implementation pattern | Testing agent → Docs & Vision agent |
| **Test** | `test`, `testing` | Create/update tests directly | None (self-validating) |

**Analysis questions:**
1. Is this a bug or feature request?
2. Which components are affected? (adapter, core, builder, storage, etc.)
3. What's the expected vs actual behavior?
4. Are there reproduction steps or test cases?
5. Does this require breaking changes?

### Step 3: Create Workspace

For bugs and features, create workspace structure:

```bash
# Create workspace directory
ISSUE_NUM=123
WORKSPACE_NAME="gh-${ISSUE_NUM}"
mkdir -p /home/cody/code/litestar/sqlspec/specs/active/${WORKSPACE_NAME}/research

# Create prd.md
cat > /home/cody/code/litestar/sqlspec/specs/active/${WORKSPACE_NAME}/prd.md <<'EOF'
# GitHub Issue #${ISSUE_NUM}: [Title]

## Issue Link
https://github.com/litestar-org/sqlspec/issues/${ISSUE_NUM}

## Problem Statement
[Extract from issue description]

## Expected Behavior
[What should happen]

## Actual Behavior
[What currently happens]

## Acceptance Criteria
- [ ] [Criterion 1 from issue]
- [ ] [Criterion 2 from issue]
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] No breaking changes (or documented)

## Technical Scope
Components affected:
- [List affected files/modules]

## References
- Related issues: [links]
- Related PRs: [links]
EOF

# Create tasks.md
cat > /home/cody/code/litestar/sqlspec/specs/active/${WORKSPACE_NAME}/tasks.md <<'EOF'
# Implementation Tasks

## Analysis
- [ ] Reproduce issue locally
- [ ] Identify root cause
- [ ] Design solution

## Implementation
- [ ] Core changes
- [ ] Adapter changes (if applicable)
- [ ] Update type annotations

## Testing
- [ ] Unit tests
- [ ] Integration tests
- [ ] Manual verification

## Documentation
- [ ] Code comments/docstrings
- [ ] Update guides
- [ ] Update CHANGELOG

## Quality Gates
- [ ] make lint passes
- [ ] make test passes
- [ ] No breaking changes
EOF

# Create recovery.md
cat > /home/cody/code/litestar/sqlspec/specs/active/${WORKSPACE_NAME}/recovery.md <<'EOF'
# Recovery Guide

## Current Status
Status: Analysis
Last updated: $(date +%Y-%m-%d)

## Progress Summary
Starting work on GitHub issue #${ISSUE_NUM}

## Next Steps
1. Reproduce issue
2. Debug root cause
3. Implement fix
EOF
```

**For documentation-only issues**, skip workspace creation and proceed directly to documentation updates.

### Step 4: Debug (if bug)

For bug reports, use systematic debugging workflow:

```python
# Step 1: Reproduce the issue
mcp__zen__debug(
    step="Reproduce issue #123: Connection pool exhaustion under load",
    step_number=1,
    total_steps=5,
    hypothesis="Initial hypothesis from issue description",
    findings="Attempting to reproduce with provided test case",
    files_checked=[],
    confidence="exploring",
    next_step_required=True
)

# Step 2: Identify root cause
mcp__zen__debug(
    step="Investigate connection lifecycle in asyncpg adapter",
    step_number=2,
    total_steps=5,
    hypothesis="Pool not releasing connections on exception",
    findings="Found 3 code paths missing pool.release() in finally blocks",
    files_checked=[
        "/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py",
        "/home/cody/code/litestar/sqlspec/sqlspec/driver/base.py"
    ],
    confidence="high",
    next_step_required=True
)

# Step 3: Verify fix approach
mcp__zen__debug(
    step="Test fix with proper exception handling",
    step_number=3,
    total_steps=5,
    hypothesis="Adding try-finally blocks will resolve pool exhaustion",
    findings="Local test confirms fix works - pool now releases properly",
    files_checked=[
        "/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py"
    ],
    relevant_files=[
        "/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py"
    ],
    confidence="very_high",
    next_step_required=False
)
```

**Debug workflow principles:**
- Start with reproduction (never assume issue is valid)
- Use zen.debug for systematic investigation
- Document findings in recovery.md
- Update hypothesis as evidence emerges
- Verify fix before implementation

### Step 5: Implement Fix

Follow AGENTS.md standards for implementation:

**A. Read relevant code:**

```python
# Identify affected files from debug session
Read("/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py")
Read("/home/cody/code/litestar/sqlspec/sqlspec/driver/base.py")

# Read related tests
Read("/home/cody/code/litestar/sqlspec/tests/integration/test_adapters/test_asyncpg/test_driver.py")

# Consult guides
Read("/home/cody/code/litestar/sqlspec/docs/guides/adapters/postgres.md")
Read("/home/cody/code/litestar/sqlspec/AGENTS.md")
```

**B. Apply fix with quality standards:**

```python
# Example: Fix connection pool leak
Edit(
    file_path="/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py",
    old_string="""async def execute(self, sql: str) -> None:
    connection = await self._pool.acquire()
    await connection.execute(sql)
    await self._pool.release(connection)""",
    new_string="""async def execute(self, sql: str) -> None:
    connection = await self._pool.acquire()
    try:
        await connection.execute(sql)
    finally:
        await self._pool.release(connection)"""
)
```

**C. Update workspace:**

```python
# Update recovery.md with implementation details
Edit(
    file_path="/home/cody/code/litestar/sqlspec/specs/active/gh-123/recovery.md",
    old_string="Status: Analysis",
    new_string="Status: Implementation Complete"
)

# Mark tasks complete
Edit(
    file_path="/home/cody/code/litestar/sqlspec/specs/active/gh-123/tasks.md",
    old_string="- [ ] Core changes",
    new_string="- [x] Core changes"
)
```

### Step 6: Quality Gates

Run all quality checks before creating PR:

```bash
# Auto-fix formatting issues
make fix

# Run linting
make lint

# Run full test suite
make test

# Run adapter-specific tests
uv run pytest tests/integration/test_adapters/test_asyncpg/ -v

# Verify no regressions
uv run pytest -n 2 --dist=loadgroup
```

**Quality gate checklist:**
- ✅ `make lint` passes (mypy, pyright, ruff)
- ✅ `make test` passes (all tests)
- ✅ No new warnings or errors
- ✅ Coverage maintained or improved
- ✅ AGENTS.md standards followed

### Step 7: Auto-Invoke Sub-Agents

**For bugs and features**, automatically invoke Testing and Docs & Vision agents:

```python
# A. Invoke Testing Agent (creates comprehensive tests)
Task(
    subagent_type="testing",
    description="Create tests for GitHub issue fix",
    prompt=f"""
Create comprehensive tests for the fix to GitHub issue #{{ISSUE_NUM}}.

Requirements:
1. Read specs/active/gh-{{ISSUE_NUM}}/prd.md for issue details
2. Create regression test that reproduces original bug
3. Test edge cases identified in debug session
4. Create integration tests for affected adapters
5. Verify fix resolves issue without regressions
6. Update specs/active/gh-{{ISSUE_NUM}}/tasks.md
7. All tests must pass

Test focus:
- Reproduce original issue (should now pass)
- Edge cases from debugging
- Integration with affected components
"""
)

# B. Invoke Docs & Vision Agent (docs, QA, knowledge, archive)
Task(
    subagent_type="docs-vision",
    description="Documentation and quality validation",
    prompt=f"""
Complete documentation, quality gate, and knowledge capture for GitHub issue #{{ISSUE_NUM}}.

Phase 1 - Documentation:
1. Read specs/active/gh-{{ISSUE_NUM}}/prd.md
2. Update relevant guides in docs/guides/
3. Update CHANGELOG.md with fix description
4. Add/update code examples if needed

Phase 2 - Quality Gate:
1. Verify all acceptance criteria met
2. Verify all tests passing
3. Check AGENTS.md compliance
4. BLOCK if any criteria not met

Phase 3 - Knowledge Capture:
1. Extract patterns from fix
2. Update AGENTS.md if new pattern discovered
3. Update relevant guides with lessons learned
4. Document edge cases in troubleshooting guides

Phase 4 - Re-validation:
1. Re-run tests after documentation updates
2. Verify consistency across project
3. Check for breaking changes
4. BLOCK if re-validation fails

Phase 5 - Cleanup & Archive:
1. Remove tmp/ files
2. Archive to specs/archive/gh-{{ISSUE_NUM}}/
3. Generate completion report
"""
)
```

**For documentation-only issues**, invoke only Docs & Vision agent:

```python
Task(
    subagent_type="docs-vision",
    description="Documentation updates for issue",
    prompt=f"""
Update documentation for GitHub issue #{{ISSUE_NUM}}.

Requirements:
1. Read issue description for requested changes
2. Update affected guides in docs/guides/
3. Verify examples work
4. Build documentation without errors
5. No workspace needed (direct docs update)
"""
)
```

### Step 8: Create PR

After all quality gates pass and agents complete:

```bash
# Ensure we're on a feature branch
BRANCH_NAME="fix/issue-${ISSUE_NUM}"
git checkout -b $BRANCH_NAME 2>/dev/null || git checkout $BRANCH_NAME

# Stage changes
git add .

# Create commit
git commit -m "$(cat <<'EOF'
fix: resolve issue #${ISSUE_NUM}

[Concise description of fix]

Fixes #${ISSUE_NUM}
EOF
)"

# Push to remote
git push -u origin $BRANCH_NAME

# Create PR using gh CLI
gh pr create \
  --repo litestar-org/sqlspec \
  --title "fix: [concise title] (fixes #${ISSUE_NUM})" \
  --body "$(cat <<'EOF'
## Summary
Fixes #${ISSUE_NUM} by [concise explanation].

## The Problem
[2-4 lines from issue description]

## The Solution
[2-4 lines describing fix approach]

## Key Changes
- [Change 1]
- [Change 2]
- [Change 3]
EOF
)"
```

**PR title format:**
- Bug: `fix: [description] (fixes #123)`
- Feature: `feat: [description] (closes #123)`
- Docs: `docs: [description] (closes #123)`
- Performance: `perf: [description] (closes #123)`

**PR description format (30-40 lines max):**
1. Summary (2-3 sentences)
2. The Problem (2-4 lines)
3. The Solution (2-4 lines)
4. Key Changes (3-5 bullets)

**Prohibited in PR:**
- Test coverage tables
- File change lists
- Quality metrics
- Commit breakdowns

### Step 9: Link Issue and Update

After PR is created:

```bash
# Add comment to issue
gh issue comment ${ISSUE_NUM} \
  --repo litestar-org/sqlspec \
  --body "Fix implemented in #${PR_NUMBER}"

# Update issue labels if needed
gh issue edit ${ISSUE_NUM} \
  --repo litestar-org/sqlspec \
  --add-label "status: in-review"
```

## Issue Categories and Workflows

### Category 1: Bug Fix

**Characteristics:**
- Labels: `bug`, `type: bug`
- Has reproduction steps
- Expected vs actual behavior described

**Workflow:**
1. Create workspace (`specs/active/gh-{issue}/`)
2. Use `mcp__zen__debug` for root cause analysis
3. Implement fix following AGENTS.md
4. Auto-invoke Testing agent (regression tests)
5. Auto-invoke Docs & Vision agent (docs, QA, archive)
6. Create PR with `fix:` prefix

**Example issue: Connection pool leak**
```python
# Debug workflow
mcp__zen__debug(step="Reproduce pool leak", ...)
mcp__zen__debug(step="Identify missing release", ...)
mcp__zen__debug(step="Verify fix", ...)

# Implementation
Edit(file_path="sqlspec/adapters/asyncpg/driver.py", ...)

# Auto-invoke agents
Task(subagent_type="testing", ...)
Task(subagent_type="docs-vision", ...)
```

### Category 2: Feature Request

**Characteristics:**
- Labels: `enhancement`, `feature`
- Describes new functionality
- May have API design discussion

**Workflow:**
1. Create workspace with PRD
2. Use `mcp__zen__thinkdeep` for design decisions
3. Implement following AGENTS.md patterns
4. Auto-invoke Testing agent (comprehensive tests)
5. Auto-invoke Docs & Vision agent (docs, QA, archive)
6. Create PR with `feat:` prefix

**Example issue: Add vector search support**
```python
# Design analysis
mcp__zen__thinkdeep(step="Analyze vector extension patterns", ...)

# Implementation
Write(file_path="sqlspec/builder/_vector_expressions.py", ...)
Edit(file_path="sqlspec/builder/builder.py", ...)

# Auto-invoke agents
Task(subagent_type="testing", ...)
Task(subagent_type="docs-vision", ...)
```

### Category 3: Documentation

**Characteristics:**
- Labels: `documentation`, `docs`
- Requests guide updates or examples
- No code changes needed

**Workflow:**
1. No workspace needed
2. Update docs/guides/ directly
3. Auto-invoke Docs & Vision agent (validation only)
4. Create PR with `docs:` prefix

**Example issue: Add pooling configuration guide**
```python
# Update documentation
Write(file_path="docs/guides/adapters/pooling.md", ...)

# Invoke Docs agent only
Task(
    subagent_type="docs-vision",
    description="Validate documentation changes",
    prompt="Verify docs build, examples work, consistent style"
)
```

### Category 4: Performance

**Characteristics:**
- Labels: `performance`, `optimization`
- Has benchmarks or profiling data
- Focused on speed/memory

**Workflow:**
1. Create workspace
2. Use `mcp__zen__analyze` with `analysis_type="performance"`
3. Implement optimizations
4. Auto-invoke Testing agent (benchmark tests)
5. Auto-invoke Docs & Vision agent (perf docs, QA, archive)
6. Create PR with `perf:` prefix

**Example issue: Optimize SQLglot parsing**
```python
# Performance analysis
mcp__zen__analyze(
    step="Profile statement parsing performance",
    analysis_type="performance",
    ...
)

# Implementation
Edit(file_path="sqlspec/core/statement.py", ...)

# Auto-invoke agents
Task(subagent_type="testing", ...)  # Include benchmark tests
Task(subagent_type="docs-vision", ...)
```

### Category 5: Adapter-Specific

**Characteristics:**
- Labels: `adapter: asyncpg`, `adapter: oracle`, etc.
- Affects single database adapter
- May involve driver library updates

**Workflow:**
1. Create workspace
2. Research library docs with Context7
3. Follow adapter implementation patterns
4. Auto-invoke Testing agent (adapter integration tests)
5. Auto-invoke Docs & Vision agent (adapter guide updates)
6. Create PR with scope prefix

**Example issue: Add asyncpg prepared statement support**
```python
# Research library
mcp__context7__resolve-library-id(libraryName="asyncpg")
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/MagicStack/asyncpg",
    topic="prepared statements"
)

# Implementation
Edit(file_path="sqlspec/adapters/asyncpg/driver.py", ...)

# Auto-invoke agents
Task(subagent_type="testing", ...)
Task(subagent_type="docs-vision", ...)
```

### Category 6: Test Addition

**Characteristics:**
- Labels: `test`, `testing`
- Requests additional test coverage
- No functional changes

**Workflow:**
1. No workspace needed
2. Add tests directly
3. No agent invocation (self-validating)
4. Create PR with `test:` prefix

**Example issue: Add edge case tests for parameter conversion**
```python
# Add tests
Edit(file_path="tests/unit/test_core/test_parameters.py", ...)

# Verify
Bash(command="uv run pytest tests/unit/test_core/test_parameters.py -v")
```

## Automated Workflow Summary

```
┌─────────────────────────────────────────────────────────────┐
│                   FIX ISSUE WORKFLOW                         │
│                                                              │
│  1. Fetch issue from GitHub (gh issue view)                 │
│  2. Analyze type (bug, feature, docs, perf, adapter, test)  │
│  3. Create workspace (if needed)                            │
│  4. Debug/Design                                            │
│     ├─► Bug: mcp__zen__debug                               │
│     ├─► Feature: mcp__zen__thinkdeep                       │
│     ├─► Performance: mcp__zen__analyze                     │
│     └─► Adapter: Context7 + adapter patterns              │
│  5. Implement fix (following AGENTS.md)                     │
│  6. Quality gates (make lint && make test)                  │
│  7. Auto-invoke agents:                                     │
│     ├─► Testing agent (if code changes)                    │
│     └─► Docs & Vision agent (always)                       │
│  8. Create PR (gh pr create)                                │
│  9. Link issue (gh issue comment)                           │
└─────────────────────────────────────────────────────────────┘
```

## Success Criteria

Issue fix is complete when:

✅ **Issue reproduced** - For bugs, confirmed reproduction
✅ **Root cause identified** - Clear understanding of problem
✅ **Fix implemented** - Following AGENTS.md standards
✅ **Tests added** - Regression + edge cases (via Testing agent)
✅ **Quality gates passed** - make lint && make test
✅ **Documentation updated** - Guides and CHANGELOG (via Docs & Vision)
✅ **Knowledge captured** - Patterns added to AGENTS.md/guides
✅ **PR created** - Proper format with fixes/closes #N
✅ **Issue linked** - PR references issue
✅ **Workspace archived** - Moved to specs/archive/

## Example End-to-End Execution

```bash
# Issue: #456 "asyncpg connection pool not releasing on error"

# 1. Fetch issue
gh issue view 456 --repo litestar-org/sqlspec

# 2. Create workspace
mkdir -p specs/active/gh-456/research
# ... create prd.md, tasks.md, recovery.md

# 3. Debug
mcp__zen__debug(step="Reproduce pool leak under error conditions", ...)
mcp__zen__debug(step="Identify missing finally block", ...)
mcp__zen__debug(step="Verify fix resolves issue", ...)

# 4. Implement
Edit(file_path="sqlspec/adapters/asyncpg/driver.py", ...)

# 5. Quality gates
make fix && make lint && make test

# 6. Auto-invoke agents
Task(subagent_type="testing", ...)
Task(subagent_type="docs-vision", ...)

# 7. Create PR
git checkout -b fix/issue-456
git commit -m "fix: ensure connection pool release on error (fixes #456)"
git push -u origin fix/issue-456
gh pr create --title "fix: ensure connection pool release on error (fixes #456)"

# 8. Link issue
gh issue comment 456 --body "Fix implemented in #789"
```
