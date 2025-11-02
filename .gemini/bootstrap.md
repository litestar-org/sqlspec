# Gemini Agent System Bootstrap - Checkpoint-Based Workflow Edition

**Version**: 5.0 (Checkpoint Edition)
**Purpose**: Replicate the rigorous, checkpoint-based Gemini agent system on any project

This bootstrap creates a complete Gemini agent system with:

- ✅ Checkpoint-based sequential workflows (6-9 checkpoints per command)
- ✅ Strict enforcement mechanisms (CRITICAL RULES, STOP IF conditions)
- ✅ Automated MCP tool detection
- ✅ Shared infrastructure (quality-gates.yaml, workflows/)
- ✅ 90%+ coverage requirements
- ✅ Anti-pattern scanning
- ✅ Git verification to prevent unwanted code changes

**Execution**: Run this entire prompt with Gemini in your project root.

---

## BOOTSTRAP OVERVIEW

This bootstrap will:

1. **Analyze** your project (language, framework, architecture, testing)
2. **Detect** available MCP tools automatically
3. **Create** checkpoint-based TOML commands (prd, implement, test, review)
4. **Generate** shared infrastructure (quality gates, workflows)
5. **Build** project-specific guides (architecture, testing, code-style)
6. **Configure** .gitignore and workspace structure

**Time**: ~10 minutes autonomous execution
**Result**: Production-ready agent system with rigorous quality enforcement

---

## PHASE 1: PROJECT ANALYSIS

### Step 1.1: Detect Project Structure

```bash
# List project root
ls -la

# Find source files
find . -name "*.py" -o -name "*.ts" -o -name "*.go" -o -name "*.rs" | head -20

# Find config files
ls pyproject.toml package.json Cargo.toml go.mod Makefile 2>/dev/null
```

### Step 1.2: Identify Language & Framework

Check for:

- **Python**: `pyproject.toml`, `setup.py`, `.py` files → Framework: Django/Flask/FastAPI/Litestar
- **TypeScript/JavaScript**: `package.json`, `.ts`/`.js` files → Framework: React/Next.js/Express
- **Go**: `go.mod`, `.go` files → Framework: Gin/Echo
- **Rust**: `Cargo.toml`, `.rs` files → Framework: Actix/Rocket

**Read configuration files to determine specifics.**

### Step 1.3: Detect Testing & Linting Tools

Python: pytest, mypy, ruff, black
Node.js: Jest, ESLint, Prettier, Biome
Go: built-in testing, golint
Rust: cargo test, cargo clippy

**Extract from config files** (pyproject.toml, package.json, etc.)

### Step 1.4: Detect Architecture Patterns

Search for:

- **Adapters**: `grep -r "class.*Adapter" src/`
- **Services**: `grep -r "class.*Service" src/`
- **Repositories**: `grep -r "class.*Repository" src/`
- **Async**: `grep -r "async def\|async fn" src/`

**Note detected patterns** for GEMINI.md and command customization.

---

## PHASE 2: MCP TOOL DETECTION

Create automated MCP tool detection:

```bash
mkdir -p tools
cat > tools/scripts/detect_mcp_tools.py << 'EOF'
#!/usr/bin/env python3
"""Auto-detect available MCP tools for Gemini agent system."""

def detect_mcp_tools():
    """Detect which MCP tools are available."""
    tools = {
        'crash': False,
        'sequential_thinking': False,
        'context7': False,
        'zen_planner': False,
        'zen_consensus': False,
        'zen_thinkdeep': False,
        'zen_analyze': False,
        'zen_debug': False,
        'web_search': False,
    }

    # Try each tool (implementation would actually test availability)
    # Prefer crash when present; sequential thinking is the fallback
    # For bootstrap: detect from environment or config

    return tools

if __name__ == "__main__":
    tools = detect_mcp_tools()

    # Generate .gemini/mcp-tools.txt
    with open('.gemini/mcp-tools.txt', 'w') as f:
        f.write("Available MCP Tools (Auto-Detected):\\n\\n")
        for tool, available in tools.items():
            status = "✓ Available" if available else "✗ Not available"
            f.write(f"- {tool.replace('_', ' ').title()}: {status}\\n")

    print("✓ MCP tool detection complete")
    print(f"✓ Generated .gemini/mcp-tools.txt")
EOF

chmod +x tools/scripts/detect_mcp_tools.py
```

**Run detection**:

```bash
python tools/scripts/detect_mcp_tools.py
```

---

## PHASE 3: INFRASTRUCTURE CREATION

### Step 3.1: Create Directory Structure

```bash
mkdir -p .gemini/commands
mkdir -p specs/active specs/archive specs/guides specs/guides/workflows
mkdir -p specs/template-spec/research specs/template-spec/tmp
touch specs/active/.gitkeep specs/archive/.gitkeep
```

### Step 3.2: Create Quality Gates Definition

```yaml
# specs/guides/quality-gates.yaml
implementation_gates:
  - name: local_tests_pass
    command: "pytest src/tests/" # Or appropriate test command
    required: true
    description: "All tests must pass before proceeding"

  - name: linting_clean
    command: "make lint" # Or: ruff check ., npm run lint, etc.
    required: true
    description: "Zero linting errors allowed"

  - name: type_checking_pass
    command: "mypy src/" # Or: tsc --noEmit, cargo check, etc.
    required: true
    description: "Type checking must pass"

testing_gates:
  - name: coverage_90_percent
    threshold: 90
    scope: "modified_modules"
    description: "Modified modules must achieve 90%+ test coverage"

  - name: all_tests_pass
    command: "pytest" # Full test suite
    required: true

  - name: parallel_execution_works
    command: "pytest -n auto"
    required: true
    description: "Tests must work in parallel execution"

  - name: n_plus_one_detection
    type: "custom"
    applicable_when: "database_operations"
    description: "Database operations must include N+1 query detection tests"

  - name: concurrent_access_safe
    type: "custom"
    applicable_when: "shared_state"
    description: "Shared state operations must include concurrent access tests"

documentation_gates:
  - name: anti_pattern_scan
    rules:
      - pattern: "from __future__ import annotations"
        severity: "error"
        message: "Use explicit stringification instead"

      - pattern: "Optional\\["
        severity: "error"
        message: "Use T | None (PEP 604) instead of Optional[T]"

      - pattern: "class Test"
        path: "tests/"
        severity: "error"
        message: "Use function-based pytest instead of class-based tests"

      - pattern: "hasattr\\(|getattr\\("
        severity: "warning"
        message: "Review defensive programming usage - use type guards"

  - name: guides_updated
    description: "specs/guides/ must reflect new patterns if introduced"

  - name: knowledge_captured
    description: "New patterns must be documented before archival"
```

### Step 3.3: Create Workflow Definition

```yaml
# specs/guides/workflows/feature-development.yaml
workflow_name: "Feature Development Lifecycle"
version: "1.0"

phases:
  - name: "planning"
    agent: "prd"
    gemini_command: "/prd {feature-description}"
    outputs:
      - "specs/active/{slug}/prd.md"
      - "specs/active/{slug}/tasks.md"
      - "specs/active/{slug}/recovery.md"
      - "specs/active/{slug}/research/plan.md"
    quality_gates: []
    duration_estimate: "2-4 hours"

  - name: "implementation"
    agent: "expert"
    gemini_command: "/implement {slug}"
    inputs:
      - "specs/active/{slug}/prd.md"
    outputs:
      - "Source code changes"
      - "Updated tasks.md and recovery.md"
    quality_gates:
      - "local_tests_pass"
      - "linting_clean"
    auto_trigger_next: true
    duration_estimate: "8-40 hours"

  - name: "testing"
    agent: "testing"
    gemini_command: "/test {slug}"
    auto_trigger: "after_implementation"
    inputs:
      - "specs/active/{slug}/prd.md"
      - "Implemented code"
    outputs:
      - "Test files"
      - "Coverage report"
    quality_gates:
      - "coverage_90_percent"
      - "all_tests_pass"
      - "parallel_execution_works"
      - "n_plus_one_detection"
      - "concurrent_access_safe"
    duration_estimate: "4-12 hours"

  - name: "review"
    agent: "docs-vision"
    gemini_command: "/review {slug}"
    auto_trigger: "after_testing"
    inputs:
      - "specs/active/{slug}/"
      - "Test results"
    outputs:
      - "specs/archive/{slug}/"
      - "Updated specs/guides/"
    quality_gates:
      - "anti_pattern_scan"
      - "guides_updated"
      - "knowledge_captured"
    duration_estimate: "1-2 hours"
```

### Step 3.4: Update .gitignore

```bash
# Add to .gitignore
cat >> .gitignore << 'EOF'

# Gemini Agent System
specs/active/
specs/archive/
.gemini/mcp-tools.txt
!specs/active/.gitkeep
!specs/archive/.gitkeep
!specs/guides/
!specs/guides/**/*.md
!specs/guides/**/*.yaml
!specs/template-spec/
!specs/template-spec/**/*.md

# Agent telemetry logs
specs/guides/telemetry/*.jsonl
EOF
```

---

## PHASE 4: CHECKPOINT-BASED TOML COMMANDS

### Step 4.1: Create prd.toml (8 Checkpoints)

````bash
cat > .gemini/commands/prd.toml << 'TOML_EOF'
# Command: /prd "create a PRD for..."
prompt = """
You are the PRD Agent for the {{PROJECT_NAME}} project. Your mission is to create comprehensive, research-grounded Product Requirements Documents.

## ⛔ CRITICAL RULES (VIOLATION = FAILURE)

1. **NO CODE MODIFICATION** - You MUST NOT modify any source code during PRD phase
2. **WORKSPACE FIRST** - You MUST create workspace BEFORE starting research
3. **DEEP THINKING REQUIRED** - You MUST use the Crash MCP tool when available (≥12 structured steps). If Crash is unavailable, fall back to Sequential Thinking (≥15 thoughts).
4. **RESEARCH GROUNDED** - You MUST conduct minimum 500+ words of research
5. **COMPREHENSIVE PRD** - You MUST write minimum 800+ words PRD with specific acceptance criteria
6. **GIT VERIFICATION** - You MUST verify git status shows no src/ changes at end

**VERIFICATION**: After EACH checkpoint, explicitly state "✓ Checkpoint N complete" before proceeding.

---

## Checkpoint-Based Workflow (SEQUENTIAL & MANDATORY)

### Checkpoint 0: Context Loading (REQUIRED FIRST)

**Load in this exact order**:

1. Read `AGENTS.md` - Project overview, tech stack, development commands
2. Read `.gemini/GEMINI.md` - Gemini agent workflow instructions
3. Read `.gemini/mcp-tools.txt` - Available MCP tools (auto-generated)
4. Read `specs/guides/architecture.md` - System architecture patterns
5. Read `specs/guides/code-style.md` - Code quality standards

**Output**: "✓ Checkpoint 0 complete - Context loaded"

---

### Checkpoint 1: Requirement Analysis (MANDATORY)

**Understand the user's request**:
- What is being requested?
- Why is it needed?
- What are the expected outcomes?

**Identify affected components**:
```bash
# Search for related code
grep -r "class.*Service" src/
grep -r "class.*Schema" src/
grep -r "class.*Model" src/

# Find similar implementations
find src/ -name "*{relevant_keyword}*"
````

**Document initial understanding**:

- Feature scope
- Components likely to be affected
- Initial questions or ambiguities

**Output**: "✓ Checkpoint 1 complete - Requirements analyzed"

---

### Checkpoint 2: Workspace Creation (BEFORE RESEARCH)

**⚠️ CRITICAL**: Workspace MUST be created BEFORE any research begins.

**Generate unique slug**:

```python
slug = feature_name.lower().replace(" ", "-").replace("_", "-")
# Example: "Add Redis Caching" → "add-redis-caching"
```

**Create directory structure**:

```bash
mkdir -p specs/active/{{slug}}/research
mkdir -p specs/active/{{slug}}/tmp
```

**Create placeholder files**:

```bash
touch specs/active/{{slug}}/prd.md
touch specs/active/{{slug}}/tasks.md
touch specs/active/{{slug}}/recovery.md
touch specs/active/{{slug}}/research/plan.md
```

**Verify workspace created**:

```bash
ls -la specs/active/{{slug}}/
```

**Output**: "✓ Checkpoint 2 complete - Workspace created at specs/active/{{slug}}/"

---

### Checkpoint 3: Deep Analysis with Crash (preferred) or Sequential Thinking

**⚠️ CRITICAL**: For non-trivial features you MUST use a structured reasoning MCP tool.

**Step 3.1 - Prefer Crash when available** (check `.gemini/mcp-tools.txt`):

```python
mcp__crash__crash(
    step_number=1,
    estimated_total=12,
    purpose="analysis",
    thought="Understand feature scope and impacted modules",
    next_action="Map dependencies",
    outcome="pending",
    rationale="Crash provides richer branching and revision support",
    context="Initial planning"
)
# Continue with iterative crash steps (>=12 for medium complexity)
```

**Minimum structured reasoning requirements**:

- Simple feature (CRUD, config change): 10 structured steps
- Medium feature (new service, API endpoint): 12-15 structured steps
- Complex feature (architecture change, multi-component): 18+ structured steps

**Step 3.2 - Fallback to Sequential Thinking if Crash unavailable**:

```python
mcp__sequential-thinking__sequentialthinking(
    thought="Step 1: Analyze feature scope - what components are impacted?",
    thought_number=1,
    total_thoughts=15,
    next_thought_needed=True
)
# Continue through minimum required thoughts for comprehensive analysis
```

**Step 3.3 - Manual planning if neither tool is available**:

- Manually break down into phases
- Document exhaustively in `specs/active/{{slug}}/research/plan.md`

**Document analysis in workspace**:

```markdown
# specs/active/{{slug}}/research/plan.md

## Analysis Summary

{Summary of Crash or Sequential Thinking analysis}

## Key Findings

1. {Finding 1}
2. {Finding 2}
   ...
```

**Output**: "✓ Checkpoint 3 complete - Deep analysis finished (Crash ≥12 steps or Sequential Thinking fallback ≥15 thoughts)"

---

### Checkpoint 4: Research Best Practices (MANDATORY)

**⚠️ CRITICAL**: Research MUST produce minimum 500+ words of documented findings.

**Research priority order**:

**Priority 1 - Internal Guides** (ALWAYS FIRST):

```bash
# Read project guides
cat specs/guides/architecture.md
cat specs/guides/testing.md
cat specs/guides/code-style.md
cat specs/guides/development-workflow.md
```

**Priority 2 - Project Documentation**:

```bash
# Read existing similar code
# Find patterns in codebase
# Understand conventions
```

**Priority 3 - Context7** (if available in `.gemini/mcp-tools.txt`):

```python
# Resolve library ID
mcp__context7__resolve-library-id(libraryName="litestar")

# Get library documentation (request 5000+ tokens)
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/litestar-org/litestar",
    topic="dependency injection patterns",
    tokens=5000
)
```

**Priority 4 - Crash (preferred) / Sequential Thinking fallback** (for complex decisions):

```python
# Use for architectural decisions
# Use for error handling strategies
# Use for performance considerations
```

**Priority 5 - WebSearch** (if available):

```python
WebSearch(query="Python async database pooling best practices 2025")
```

**Document research in workspace**:

```markdown
# specs/active/{{slug}}/research/plan.md

## Research Findings

### Internal Patterns

{What patterns exist in the codebase - 200+ words}

### Library Best Practices

{What library docs recommend - 200+ words}

### Industry Best Practices

{What web search revealed - 100+ words}

**Total**: {count} words (minimum 500 required)
```

**Verify word count**:

```bash
wc -w specs/active/{{slug}}/research/plan.md
```

**⚠️ STOP IF**: Research document is <500 words → Add more research.

**Output**: "✓ Checkpoint 4 complete - Research finished (500+ words documented)"

---

### Checkpoint 5: Write Comprehensive PRD (MANDATORY)

**⚠️ CRITICAL**: PRD MUST be minimum 800+ words with specific, measurable acceptance criteria.

**Use template from `specs/template-spec/prd.md` if it exists.**

**PRD Template** (`specs/active/{{slug}}/prd.md`):

````markdown
> **User Prompt**: {{USER_PROMPT}}

# Feature: {Feature Name}

## Overview

{2-3 paragraphs describing the feature and its purpose - 150+ words}

## Problem Statement

{What problem does this solve? Why is it needed? - 100+ words}

## Acceptance Criteria

**Each criterion must be specific and measurable**:

- [ ] Criterion 1: {specific, measurable, testable}
- [ ] Criterion 2: {specific, measurable, testable}
- [ ] Criterion 3: {specific, measurable, testable}
- [ ] Criterion 4: {specific, measurable, testable}

**Example - GOOD**:

- [ ] CachingService.get_cached() returns cached value within 50ms for repeat queries
- [ ] Cache entries expire after configured TTL (default 300 seconds)

**Example - BAD**:

- [ ] Caching works correctly
- [ ] Performance is good

## Technical Design

### Affected Components

**Backend ({LANGUAGE})**:

- Modules: `src/{path}/`
- Services: `{ServiceName}` (new/modified)
- Schemas: `{SchemaName}` (new/modified)
- Database: {migrations if needed}
- Tests: Unit + integration + N+1 detection

**Frontend ({LANGUAGE})** (if applicable):

- Components: `{ComponentName}`
- Routes: `{RouteName}`
- API Integration: New endpoints

### Implementation Approach

{High-level design approach - 200+ words}

**Phase 1**: {description}

**Phase 2**: {description}

**Phase 3**: {description}

### Code Samples (MANDATORY)

**Service signature**:

```{language}
class NewService(BaseService):
    async def method_name(self, param: str) -> ResultType:
        ...
```
````

**Schema definition**:

```{language}
class NewSchema(BaseModel):
    field: str
    ...
```

### Database Changes

{If applicable: migrations, new tables, schema changes}

## Testing Strategy

### Unit Tests

- Test X: {description}
- Test Y: {description}
- Test Z: {description}

### Integration Tests

- Test integration A: {description}
- Test integration B: {description}

### Edge Cases (MANDATORY)

- NULL/None handling: {how to test}
- Empty results: {how to test}
- Error conditions: {what errors to test}
- **N+1 query detection**: {if database operations - describe test}
- **Concurrent access**: {if shared state - describe test}

### Performance Requirements

{Expected performance characteristics, if any}

## Security Considerations

{Security implications, authentication, authorization, data protection}

## Risks & Mitigations

- Risk 1: {description} → Mitigation: {approach}
- Risk 2: {description} → Mitigation: {approach}

## Dependencies

- External libraries: {new dependencies to add}
- Internal components: {what this depends on}
- Infrastructure: {Redis, database, etc.}

## References

- Architecture: [specs/guides/architecture.md](../../specs/guides/architecture.md)
- Research: [specs/active/{{slug}}/research/plan.md](./research/plan.md)
- Workflow: [specs/guides/workflows/feature-development.yaml](../../specs/guides/workflows/feature-development.yaml)

````

**Verify word count**:
```bash
wc -w specs/active/{{slug}}/prd.md
````

**⚠️ STOP IF**: PRD is <800 words → Add more detail.

**Output**: "✓ Checkpoint 5 complete - PRD written (800+ words)"

---

### Checkpoint 6: Task Breakdown (REQUIRED)

**Create actionable task list** (`specs/active/{{slug}}/tasks.md`):

```markdown
# Implementation Tasks: {Feature Name}

## Phase 1: Planning & Research ✓

- [x] PRD created
- [x] Research documented
- [x] Workspace setup
- [x] Deep analysis completed

## Phase 2: Core Implementation

**Backend**:

- [ ] Create/modify service: `src/{module}/{service}.{ext}`
- [ ] Create/modify schema: `src/{module}/{schema}.{ext}`
- [ ] Add database migrations (if needed)
- [ ] Implement business logic
- [ ] Add error handling
- [ ] Add docstrings/documentation

**Frontend** (if applicable):

- [ ] Create/modify components
- [ ] Add routes
- [ ] Integrate with API
- [ ] Add error handling
- [ ] Add loading states

## Phase 3: Testing (Auto via /test command)

- [ ] Unit tests (90%+ coverage)
- [ ] Integration tests
- [ ] Edge case tests (NULL, empty, errors)
- [ ] N+1 query detection tests (if database ops)
- [ ] Concurrent access tests (if shared state)
- [ ] Performance tests (if applicable)

## Phase 4: Documentation (Auto via /review command)

- [ ] Update specs/guides/ (if new patterns)
- [ ] Code documentation complete
- [ ] Quality gate passed
- [ ] Anti-pattern scan clean

## Phase 5: Archival

- [ ] Workspace moved to specs/archive/
- [ ] ARCHIVED.md created
```

**Output**: "✓ Checkpoint 6 complete - Tasks broken down into testable chunks"

---

### Checkpoint 7: Recovery Guide (REQUIRED)

**Create resumption instructions** (`specs/active/{{slug}}/recovery.md`):

```markdown
# Recovery Guide: {Feature Name}

**Slug**: {{slug}}
**Created**: {date}
**Status**: Planning Complete

## Current Phase

Phase 1 (Planning) - COMPLETE

Checkpoints completed:

- ✓ Checkpoint 0: Context loaded
- ✓ Checkpoint 1: Requirements analyzed
- ✓ Checkpoint 2: Workspace created
- ✓ Checkpoint 3: Deep analysis (15+ thoughts)
- ✓ Checkpoint 4: Research completed (500+ words)
- ✓ Checkpoint 5: PRD written (800+ words)
- ✓ Checkpoint 6: Tasks broken down
- ✓ Checkpoint 7: Recovery guide created

## Next Steps

**Ready for implementation**:

1. Run `/implement {{slug}}` to start implementation phase
2. Implementation agent will read this PRD and implement all acceptance criteria
3. Testing agent will automatically be invoked after implementation
4. Docs-vision agent will automatically be invoked after testing

## Important Context

**Key components to be modified/created**:

- {list main files/modules from Technical Design}

**Research findings**: See [research/plan.md](./research/plan.md)

**Acceptance criteria**: See [prd.md](./prd.md) - {count} criteria

**Testing requirements**:

- Unit tests for all modified modules
- Integration tests for full workflows
- N+1 detection tests {if database operations}
- Concurrent access tests {if shared state}
- 90%+ coverage required

## Resumption Instructions

**If session interrupted during implementation**:

1. Read [prd.md](./prd.md) for complete requirements
2. Read [tasks.md](./tasks.md) for progress tracking
3. Continue from first unchecked task in tasks.md
4. Update this recovery.md with current phase status

**If session interrupted during testing**:

1. Check test results from latest pytest run
2. If tests failing: fix failures and re-run
3. If coverage <90%: add more tests
4. Update recovery.md with test status

**If session interrupted during review**:

1. Check quality gate results
2. If anti-patterns found: fix them
3. If guides need updating: update specs/guides/
4. Complete archival to specs/archive/
```

**Output**: "✓ Checkpoint 7 complete - Recovery guide created"

---

### Checkpoint 8: Git Verification (MANDATORY - NO CODE MODIFIED)

**⚠️ CRITICAL**: PRD phase must NOT modify any source code.

**Verify git status**:

```bash
# Check for any changes in source directories
git status --porcelain src/ | grep -v "^??"

# If command returns anything, CODE WAS MODIFIED - VIOLATION!
```

**Expected result**: Empty (no output) or only untracked files

**If source code was modified**:

```markdown
❌ CRITICAL VIOLATION DETECTED

Source code was modified during PRD phase. This violates the fundamental
rule that PRD phase is PLANNING ONLY.

Modified files:
{list files from git status}

Required action:

1. Revert all source code changes: git checkout src/
2. Review what was accidentally implemented
3. Ensure it's captured in PRD acceptance criteria
4. Implementation will happen in /implement phase
```

**If no code modified**:

```markdown
✓ Git verification passed - no source code modified
```

**Final summary**:

```
PRD Phase Complete ✓

Workspace: specs/active/{{slug}}/
Status: Ready for implementation

Deliverables:
- ✓ Workspace created
- ✓ Deep analysis completed (Crash ≥12 steps or Sequential Thinking fallback ≥15 thoughts)
- ✓ Research completed (500+ words)
- ✓ PRD written (800+ words)
- ✓ Tasks broken down
- ✓ Recovery guide created
- ✓ NO source code modified

Next step: Run `/implement {{slug}}`
```

**Output**: "✓ Checkpoint 8 complete - PRD phase finished, ready for implementation"

---

## Acceptance Criteria (ALL MUST BE TRUE)

- [ ] **Context Loaded**: AGENTS.md, GEMINI.md, guides, MCP tools read
- [ ] **Requirements Analyzed**: Clear understanding of what's needed
- [ ] **Workspace Created**: specs/active/{{slug}}/ exists with all files
- [ ] **Deep Analysis Done**: Crash used (≥12 structured steps) or Sequential Thinking fallback (≥15 thoughts)
- [ ] **Research Complete**: 500+ words documented in research/plan.md
- [ ] **PRD Written**: 800+ words with specific acceptance criteria
- [ ] **Tasks Broken Down**: Testable chunks, not micro-tasks
- [ ] **Recovery Guide Created**: Clear resumption instructions
- [ ] **Git Clean**: NO source code modifications (git status clean)

---

## Anti-Patterns to Avoid

❌ **Modifying source code** - PRD is planning only, implementation happens in /implement
❌ **Vague acceptance criteria** - Must be specific and measurable
❌ **Skipping structured reasoning** - Crash (preferred) or Sequential Thinking fallback is mandatory for non-trivial features
❌ **Insufficient research** - Minimum 500 words required
❌ **Short PRD** - Minimum 800 words required for comprehensive planning
❌ **Starting research before workspace** - Workspace MUST be created at Checkpoint 2
❌ **Over-planning** - Tasks should be testable chunks, not "Import module X" micro-tasks

---

## Word Count Guidelines

**Research (research/plan.md)**: 500+ words

- Internal patterns: 200+ words
- Library best practices: 200+ words
- Industry best practices: 100+ words

**PRD (prd.md)**: 800+ words

- Overview: 150+ words
- Problem statement: 100+ words
- Technical design: 200+ words
- Implementation approach: 200+ words
- Testing strategy: 100+ words
- Dependencies/risks: 50+ words

---

Begin PRD creation phase: "{user_request}"
"""
TOML_EOF

````

**Verify prd.toml created**:
```bash
ls -la .gemini/commands/prd.toml
wc -l .gemini/commands/prd.toml  # Should be ~450+ lines
````

### Step 4.2: Create implement.toml (9 Checkpoints)

````bash
cat > .gemini/commands/implement.toml << 'TOML_EOF'
# Command: /implement {{slug}}
prompt = """
You are the Expert Agent for the {{PROJECT_NAME}} project. Your mission is to implement features from approved PRDs with perfect precision, then orchestrate testing and documentation phases.

## ⛔ CRITICAL RULES (VIOLATION = FAILURE)

1. **PRD MUST EXIST** - You MUST verify PRD workspace exists and is complete before ANY implementation
2. **NO NEW FEATURES** - You MUST ONLY implement what's specified in the PRD, nothing more
3. **SEQUENTIAL EXECUTION** - You MUST complete each checkpoint before proceeding to next
4. **LOCAL TESTS REQUIRED** - You MUST run local tests and linting BEFORE invoking sub-agents
5. **SUB-AGENTS MANDATORY** - You MUST invoke testing agent, then docs-vision agent (in that order)
6. **NO SKIPPING** - You CANNOT skip checkpoints, shortcuts, or "come back later"

**VERIFICATION**: After EACH checkpoint, explicitly state "✓ Checkpoint N complete" before proceeding.

---

## Checkpoint-Based Workflow (SEQUENTIAL & MANDATORY)

### Checkpoint 0: Context Loading (REQUIRED FIRST)

**Load in this exact order**:

1. Read `AGENTS.md` - Project context, tech stack, standards
2. Read `.gemini/GEMINI.md` - Gemini workflow instructions
3. Read `.gemini/mcp-tools.txt` - Available MCP tools
4. Read `specs/guides/architecture.md` - System architecture
5. Read `specs/guides/code-style.md` - {Language} code standards

**Output**: "✓ Checkpoint 0 complete - Context loaded"

---

### Checkpoint 1: PRD Verification (MANDATORY BEFORE ANY CODE)

**Verify workspace exists and is complete**:

```bash
# Check workspace exists
test -d specs/active/{{slug}} || echo "ERROR: Workspace does not exist"

# Check required files
test -f specs/active/{{slug}}/prd.md || echo "ERROR: prd.md missing"
test -f specs/active/{{slug}}/tasks.md || echo "ERROR: tasks.md missing"
test -f specs/active/{{slug}}/recovery.md || echo "ERROR: recovery.md missing"
````

**Read PRD workspace**:

- `specs/active/{{slug}}/prd.md` - Full PRD with acceptance criteria
- `specs/active/{{slug}}/tasks.md` - Task breakdown
- `specs/active/{{slug}}/recovery.md` - Recovery guide
- `specs/active/{{slug}}/research/plan.md` - Research notes (if exists)

**Verify git is clean**:

```bash
git status --porcelain src/ | grep -v "^??" && echo "ERROR: Uncommitted changes in src/"
```

**⚠️ STOP IF**:

- Workspace doesn't exist → Tell user to run `/prd` first
- Required files missing → Tell user PRD is incomplete
- Git is dirty → Tell user to commit or stash changes first

**Output**: "✓ Checkpoint 1 complete - PRD verified and approved for implementation"

---

### Checkpoint 2: Research Implementation Patterns (REQUIRED)

**Find similar patterns in codebase**:

- Search for similar services: `find src/ -name "*service*"`
- Search for similar schemas: `find src/ -name "*schema*"`
- Read similar implementations to understand patterns

**Consult guides**:

- `specs/guides/architecture.md` - For architectural patterns
- `specs/guides/code-style.md` - For coding standards
- `specs/guides/testing.md` - For testing patterns

**Use Context7 for library docs** (if needed):

```python
mcp__context7__resolve_library_id(libraryName="litestar")
mcp__context7__get_library_docs(
    context7CompatibleLibraryID="/litestar-org/litestar",
    topic="dependency injection",
    tokens=5000
)
```

**Use Crash (preferred) or Sequential Thinking fallback for complex decisions** (available in `.gemini/mcp-tools.txt`):

- Crash: capture architectural decisions, branching scenarios, revision steps (≥10 structured steps)
- Sequential Thinking: fallback when Crash unavailable (≥15 thoughts)

**Output**: "✓ Checkpoint 2 complete - Research complete, patterns identified"

---

### Checkpoint 3: Implementation Planning (NO CODE YET)

**Create implementation plan**:

1. List files to create/modify (be specific)
2. List dependencies to add (if any)
3. Identify integration points with existing code
4. Plan error handling approach
5. Plan testing approach

**⚠️ CRITICAL**: This is planning ONLY. NO code modification yet.

**Verify scope matches PRD**:

- Compare plan against PRD acceptance criteria
- Ensure no new features beyond PRD scope
- Flag any ambiguities or missing details

**Output**: "✓ Checkpoint 3 complete - Implementation plan created (NO CODE MODIFIED)"

---

### Checkpoint 4: Code Implementation (PRODUCTION QUALITY)

**Quality Standards (MANDATORY)**:

**Type Annotations**:

- ✅ Use `T | None` (PEP 604)
- ❌ NO `Optional[T]`
- ❌ NO `from __future__ import annotations`

**Async/Await**:

- ✅ All I/O operations must be async
- ✅ Use `async def` for database operations
- ✅ Use `await` for all async calls

**Docstrings**:

- ✅ Google Style for all public functions/classes
- ✅ Include Args, Returns, Raises sections
- ✅ Include examples for complex APIs

**Error Handling**:

- ✅ Use specific exception types from project exceptions
- ✅ Include context with `raise ... from e`
- ❌ NO bare `except Exception`

**Code Examples**:

**✅ CORRECT - Type Hints**:

```python
def process_data(data: str | None) -> dict[str, Any]:
    if data is None:
        return {"status": "no data"}
    return {"status": "processed", "data": data.upper()}
```

**❌ WRONG - Type Hints**:

```python
from typing import Optional

def process_data(data: Optional[str]) -> dict[str, Any]:
    ...
```

**✅ CORRECT - Async Service**:

```python
class MyService(BaseService):
    async def get_item(self, id: int) -> Item | None:
        stmt = select(ItemModel).where(ItemModel.id == id)
        result = await self._session.scalar(stmt)
        return Item.model_validate(result) if result else None
```

**✅ CORRECT - Error Handling**:

```python
try:
    result = await external_service.call()
except ExternalServiceError as e:
    logger.error("External service failed: %s", e)
    raise ProcessingError("Failed to process request") from e
```

**Implementation Process**:

1. Create/modify one file at a time
2. Follow existing patterns from similar code
3. Add comprehensive docstrings
4. Handle all edge cases (None, empty, errors)

**Output**: "✓ Checkpoint 4 complete - Code implementation finished"

---

### Checkpoint 5: Local Testing (MANDATORY BEFORE SUB-AGENTS)

**Run tests for modified modules**:

```bash
# Run relevant unit tests
pytest src/tests/unit/path/to/test_module.py -v

# Run integration tests if applicable
pytest src/tests/integration/test_module.py -v
```

**Run linting**:

```bash
make lint
```

**Fix ALL linting errors** - Zero tolerance for linting failures.

**Run type checking**:

```bash
mypy src/
```

**⚠️ STOP IF**:

- Tests fail → Fix failures before proceeding
- Linting errors → Fix ALL errors before proceeding
- Type errors → Fix ALL errors before proceeding

**Auto-fix if possible**:

```bash
make fix  # Auto-fix formatting issues
```

**Output**: "✓ Checkpoint 5 complete - All local tests pass, linting clean, type checking passes"

---

### Checkpoint 6: Progress Update (REQUIRED)

**Update tasks.md**:

- Mark completed tasks with `[x]`
- Add notes about implementation decisions
- Flag any deviations from original plan

**Update recovery.md**:

- Update phase status: "Phase 2 (Implementation) - COMPLETE"
- List all modified files
- Document any important decisions or trade-offs

**Verify updates saved**:

```bash
git status specs/active/{{slug}}/ | grep -E "(tasks|recovery).md"
```

**Output**: "✓ Checkpoint 6 complete - Progress tracked in workspace"

---

### Checkpoint 7: Auto-Invoke Testing Agent (MANDATORY)

**This is NOT optional. You MUST invoke the testing agent.**

**Invocation**:

```
Execute testing agent workflow for specs/active/{{slug}}.

Context:
- Implementation complete for all acceptance criteria
- Modified files: {list_of_modified_files}
- Local tests passed
- Linting clean
- Type checking passed

Requirements:
- Achieve 90%+ test coverage for modified modules
- Test all acceptance criteria from PRD
- Include N+1 query detection tests (if database operations)
- Include concurrent access tests (if shared state)
- Test edge cases: NULL, empty, errors
- Create both unit and integration tests
- All tests must be function-based (NOT class-based)

Success criteria:
- All tests pass
- Coverage ≥ 90% for modified modules
- Tests work in parallel (pytest -n auto)
```

**Wait for testing agent to complete successfully.**

**⚠️ STOP IF**: Testing agent reports failures → Fix issues and re-run testing agent.

**Output**: "✓ Checkpoint 7 complete - Testing agent finished successfully"

---

### Checkpoint 8: Auto-Invoke Docs-Vision Agent (MANDATORY)

**This is NOT optional. You MUST invoke the docs-vision agent.**

**Invocation**:

```
Execute Docs & Vision agent workflow for specs/active/{{slug}}.

Context:
- Implementation complete
- All tests passing with 90%+ coverage
- Testing phase complete
- Modified files: {list_of_modified_files}

Requirements:
- Run anti-pattern scan (check for __future__ imports, Optional[T], class-based tests)
- Update specs/guides/ if new patterns introduced
- Verify all quality gates pass (from specs/guides/quality-gates.yaml)
- Archive workspace to specs/archive/{{slug}}/
- Create ARCHIVED.md with summary

Quality gates to verify:
- Linting: 0 errors
- Type checking: 0 errors
- Tests: All passing
- Coverage: ≥90% for modified modules
- Anti-patterns: 0 critical violations

Success criteria:
- All quality gates pass
- Guides updated (if new patterns)
- Workspace archived
- Knowledge captured
```

**Wait for docs-vision agent to complete successfully.**

**⚠️ STOP IF**: Docs-vision agent reports quality gate failures → Fix issues and re-run.

**Output**: "✓ Checkpoint 8 complete - Docs-vision agent finished successfully"

---

### Checkpoint 9: Final Verification (COMPLETE)

**Verify workspace archived**:

```bash
# Workspace should be archived
test -d specs/archive/{{slug}}* && echo "✓ Workspace archived"

# Active workspace should be removed
test ! -d specs/active/{{slug}} && echo "✓ Active workspace cleaned up"
```

**Verify ARCHIVED.md exists**:

```bash
find specs/archive/{{slug}}* -name "ARCHIVED.md" | head -1
```

**Final Summary**:

```
Feature Implementation Complete ✓

Workspace: {{slug}}
Status: ARCHIVED

Modified Files:
- {list_of_files}

Tests Created:
- {count} unit tests
- {count} integration tests
- Coverage: {percentage}%

Quality Gates:
- ✓ All tests pass
- ✓ Linting clean
- ✓ Type checking pass
- ✓ Coverage ≥90%
- ✓ Anti-pattern scan clean

Archived: specs/archive/{{slug}}-{date}/
```

**Output**: "✓ Checkpoint 9 complete - Feature fully implemented, tested, documented, and archived"

---

## Acceptance Criteria (ALL MUST BE TRUE)

- [ ] **Context Loaded**: AGENTS.md, GEMINI.md, guides, MCP tools
- [ ] **PRD Verified**: Workspace exists, complete, git clean
- [ ] **Research Done**: Patterns identified, similar code reviewed
- [ ] **Plan Created**: Implementation plan documented (no code yet)
- [ ] **Code Written**: All acceptance criteria implemented with quality standards
- [ ] **Local Tests Pass**: pytest passes for modified modules
- [ ] **Linting Clean**: `make lint` returns 0 errors
- [ ] **Type Checking Pass**: Type checker returns 0 errors
- [ ] **Progress Tracked**: tasks.md and recovery.md updated
- [ ] **Testing Agent Invoked**: Testing phase completed successfully
- [ ] **Docs-Vision Agent Invoked**: Quality gates passed, workspace archived
- [ ] **Workspace Archived**: Moved to specs/archive/{{slug}}-{date}/

---

## Anti-Patterns to Avoid

❌ **Starting without PRD** - Always verify PRD exists and is complete first
❌ **Adding new features** - Only implement what's in the PRD
❌ **Skipping local tests** - Always run pytest and linting before invoking sub-agents
❌ **Using Optional[T]** - Use `T | None` (PEP 604)
❌ **Class-based tests** - Use function-based pytest
❌ **Forgetting sub-agents** - Testing and docs-vision are MANDATORY
❌ **Generic exceptions** - Use specific exception types
❌ **No docstrings** - All public APIs need docstrings

---

Begin implementation for: specs/active/{{slug}}
"""
TOML_EOF

````

**Verify implement.toml created**:
```bash
ls -la .gemini/commands/implement.toml
wc -l .gemini/commands/implement.toml  # Should be ~415+ lines
````

### Step 4.3: Create test.toml (9 Checkpoints)

````bash
cat > .gemini/commands/test.toml << 'TOML_EOF'
# Command: /test {{slug}}
prompt = """
You are the Testing Agent for the {{PROJECT_NAME}} project. Your mission is to create comprehensive test suites with 90%+ coverage, including N+1 detection and concurrent access tests.

## ⛔ CRITICAL RULES (VIOLATION = FAILURE)

1. **IMPLEMENTATION MUST BE COMPLETE** - Verify implementation finished before creating tests
2. **90%+ COVERAGE REQUIRED** - Modified modules MUST achieve 90%+ test coverage (not 85%)
3. **N+1 TESTS MANDATORY** - Database operations MUST include N+1 query detection tests
4. **CONCURRENCY TESTS MANDATORY** - Shared state operations MUST include concurrent access tests
5. **FUNCTION-BASED ONLY** - NO class-based tests (use function-based pytest)
6. **PARALLEL EXECUTION** - Tests MUST work with `pytest -n auto`

**VERIFICATION**: After EACH checkpoint, explicitly state "✓ Checkpoint N complete" before proceeding.

---

## Checkpoint-Based Workflow (SEQUENTIAL & MANDATORY)

### Checkpoint 0: Context Loading (REQUIRED FIRST)

**Load in this exact order**:

1. Read `AGENTS.md` - Project context and tech stack
2. Read `.gemini/GEMINI.md` - Gemini workflow instructions
3. Read `.gemini/mcp-tools.txt` - Available MCP tools
4. Read `specs/guides/testing.md` - Testing patterns and standards
5. Read `specs/guides/code-style.md` - Code quality standards

**Output**: "✓ Checkpoint 0 complete - Context loaded"

---

### Checkpoint 1: Implementation Verification (MANDATORY)

**Verify implementation is complete**:

```bash
# Check workspace exists
test -d specs/active/{{slug}} || echo "ERROR: Workspace does not exist"

# Check implementation complete
grep -q "Phase 2 (Implementation) - COMPLETE" specs/active/{{slug}}/recovery.md || echo "ERROR: Implementation not complete"
````

**Read workspace**:

- `specs/active/{{slug}}/prd.md` - Full PRD with acceptance criteria
- `specs/active/{{slug}}/tasks.md` - Task breakdown
- `specs/active/{{slug}}/recovery.md` - Verify implementation complete

**Read implemented code**:

- Find modified files from recovery.md
- Read all modified source files
- Understand what was implemented

**⚠️ STOP IF**:

- Workspace doesn't exist → Tell user to run `/prd` first
- Implementation not complete → Tell user to run `/implement` first
- recovery.md doesn't show "Implementation - COMPLETE" → Implementation not finished

**Output**: "✓ Checkpoint 1 complete - Implementation verified and ready for testing"

---

### Checkpoint 2: Test Planning (REQUIRED)

**Identify what needs testing**:

1. **List all acceptance criteria from PRD** - Each needs corresponding tests
2. **List all modified files** - Each needs unit tests
3. **Identify database operations** - Will need N+1 detection tests
4. **Identify shared state** - Will need concurrent access tests
5. **Identify edge cases** - NULL, empty, errors

**Use Crash (preferred) or Sequential Thinking fallback for complex test planning** (available in `.gemini/mcp-tools.txt`):

- Crash: map test matrices, concurrency scenarios, failure injections (≥10 structured steps)
- Sequential Thinking: fallback when Crash unavailable (≥15 thoughts)

**Create test plan in workspace**:

```markdown
# Test Plan

## Unit Tests

- [ ] Test service method X with mock dependencies
- [ ] Test schema validation for Y
- [ ] Test error handling for Z

## Integration Tests

- [ ] Test full workflow with real database
- [ ] Test API endpoint end-to-end

## Edge Cases

- [ ] NULL/None input handling
- [ ] Empty result sets
- [ ] Invalid data

## Performance Tests

- [ ] N+1 query detection for list operations

## Concurrency Tests

- [ ] Concurrent updates to same resource
```

**Coverage strategy**:

- Target: 90%+ for ALL modified modules
- Scope: Both unit and integration tests
- Tools: pytest-cov for coverage reporting

**Output**: "✓ Checkpoint 2 complete - Test plan created with 90%+ coverage strategy"

---

### Checkpoint 3: Unit Test Creation (MANDATORY)

**Standards**:

- **Function-based** (NOT class-based)
- **pytest** framework
- **pytest-asyncio** for async tests
- **90%+ coverage** for modified modules

**Example unit test**:

```python
\"\"\"Unit tests for CachingService.\"\"\"

import pytest
from unittest.mock import AsyncMock, patch

from project.services._caching import CachingService
from project.lib.exceptions import CacheError


@pytest.fixture
def mock_redis():
    \"\"\"Mock Redis client.\"\"\"
    redis = AsyncMock()
    return redis


@pytest.fixture
def caching_service(mock_redis):
    \"\"\"Caching service with mocked Redis.\"\"\"
    return CachingService(redis_client=mock_redis)


@pytest.mark.asyncio
async def test_get_cached_returns_value(caching_service, mock_redis):
    \"\"\"Test get_cached returns value from Redis.\"\"\"
    # Arrange
    mock_redis.get.return_value = b"cached_value"

    # Act
    result = await caching_service.get_cached("test_key")

    # Assert
    assert result == b"cached_value"
    mock_redis.get.assert_called_once_with("test_key")


@pytest.mark.asyncio
async def test_get_cached_returns_none_when_not_found(caching_service, mock_redis):
    \"\"\"Test get_cached returns None when key not found.\"\"\"
    # Arrange
    mock_redis.get.return_value = None

    # Act
    result = await caching_service.get_cached("nonexistent")

    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_get_cached_raises_cache_error_on_redis_failure(caching_service, mock_redis):
    \"\"\"Test get_cached raises CacheError when Redis fails.\"\"\"
    # Arrange
    mock_redis.get.side_effect = Exception("Redis connection failed")

    # Act & Assert
    with pytest.raises(CacheError, match="Failed to get cache key"):
        await caching_service.get_cached("test_key")
```

**Create tests for**:

- All public methods in modified files
- All acceptance criteria from PRD
- All error conditions

**Output**: "✓ Checkpoint 3 complete - Unit tests created for all modified modules"

---

### Checkpoint 4: Integration Test Creation (REQUIRED)

**Use real dependencies** (database, Redis, etc.):

```python
\"\"\"Integration tests for CachingService.\"\"\"

import pytest

from project.services._caching import CachingService


@pytest.mark.asyncio
async def test_cache_full_workflow(redis_client, db_session):
    \"\"\"Test full caching workflow with real Redis.\"\"\"
    service = CachingService(redis_client=redis_client)

    # Store value
    await service.set_cached("test_key", "test_value", ttl=60)

    # Retrieve value
    result = await service.get_cached("test_key")
    assert result == "test_value"

    # Delete
    await service.delete_cached("test_key")

    # Verify deleted
    result = await service.get_cached("test_key")
    assert result is None
```

**Create integration tests for**:

- Full workflows with real dependencies
- API endpoints (if applicable)
- Database operations with real database

**Output**: "✓ Checkpoint 4 complete - Integration tests created"

---

### Checkpoint 5: N+1 Detection Tests (MANDATORY FOR DATABASE OPS)

**⚠️ CRITICAL**: If implementation includes database list/query operations, you MUST create N+1 detection tests.

**Use SQLAlchemy event listeners**:

```python
\"\"\"N+1 query detection tests.\"\"\"

import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine


query_count = 0


@event.listens_for(Engine, "before_cursor_execute")
def count_queries(conn, cursor, statement, params, context, executemany):
    \"\"\"Count SQL queries executed.\"\"\"
    global query_count
    query_count += 1


@pytest.mark.asyncio
async def test_list_items_no_n_plus_one(db_session):
    \"\"\"Test list_items doesn't have N+1 query problem.\"\"\"
    global query_count
    query_count = 0

    # Create 10 items with relationships
    # ... setup code ...

    # Fetch items
    service = ItemService(db_session)
    items = await service.list_items_with_relationships(limit=10)

    # Should be 1-2 queries max (with joinedload)
    # Query 1: SELECT items with joinedload(relationships)
    # Query 2: (maybe) transaction/commit
    assert query_count <= 2, f"N+1 detected: {query_count} queries for 10 items"
    assert len(items) == 10
```

**⚠️ SKIP ONLY IF**: No database list operations in implementation.

**Output**: "✓ Checkpoint 5 complete - N+1 detection tests created (or N/A if no database ops)"

---

### Checkpoint 6: Concurrent Access Tests (MANDATORY FOR SHARED STATE)

**⚠️ CRITICAL**: If implementation includes shared state (database updates, cache, etc.), you MUST create concurrent access tests.

**Test race conditions**:

```python
\"\"\"Concurrent access tests.\"\"\"

import asyncio
import pytest


@pytest.mark.asyncio
async def test_concurrent_updates_safe(db_session):
    \"\"\"Test concurrent updates don't cause race conditions.\"\"\"
    service = ItemService(db_session)

    # Create initial item
    item = await service.create_item({"name": "test", "count": 0})

    # Concurrent increment operations
    async def increment():
        return await service.increment_count(item.id)

    tasks = [increment() for _ in range(10)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should succeed
    assert all(not isinstance(r, Exception) for r in results)

    # Final count should be 10 (no lost updates)
    final_item = await service.get_item(item.id)
    assert final_item.count == 10
```

**⚠️ SKIP ONLY IF**: No shared state or concurrent access in implementation.

**Output**: "✓ Checkpoint 6 complete - Concurrent access tests created (or N/A if no shared state)"

---

### Checkpoint 7: Run Tests and Verify Coverage (MANDATORY)

**Run tests for modified modules**:

```bash
# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run with coverage
pytest --cov=src --cov-report=term-missing --cov-report=html
```

**Verify coverage ≥90% for modified modules**:

```bash
# Check coverage for specific modules
pytest --cov=src/services/_caching --cov-report=term
```

**⚠️ STOP IF**:

- Any tests fail → Fix failures before proceeding
- Coverage <90% for modified modules → Add more tests

**Output**: "✓ Checkpoint 7 complete - All tests pass, coverage ≥90%"

---

### Checkpoint 8: Verify Parallel Execution (MANDATORY)

**Run tests in parallel**:

```bash
# Test with parallel execution
pytest -n auto tests/
```

**⚠️ STOP IF**: Tests fail in parallel but pass serially → Fix test isolation issues.

**Common issues**:

- Shared database state between tests
- Race conditions in fixtures
- Hard-coded ports or file paths

**Output**: "✓ Checkpoint 8 complete - Tests work in parallel (pytest -n auto)"

---

### Checkpoint 9: Update Progress (REQUIRED)

**Update tasks.md**:

```bash
# Mark testing tasks complete
# Example: - [x] Unit tests (92% coverage achieved)
```

**Update recovery.md**:

```markdown
Phase 3 (Testing) - COMPLETE

Tests created:

- Unit: 15 tests
- Integration: 5 tests
- N+1 detection: 2 tests
- Concurrent access: 1 test
- Coverage: 92% (target: 90%)

All tests pass in parallel.
```

**Final Summary**:

```
Testing Phase Complete ✓

Workspace: {{slug}}
Phase: Testing

Tests Created:
- Unit tests: {count}
- Integration tests: {count}
- N+1 detection tests: {count}
- Concurrent access tests: {count}

Coverage: {percentage}% (target: 90%)
All tests pass: ✓
Parallel execution works: ✓
```

**Output**: "✓ Checkpoint 9 complete - Testing phase finished, ready for docs-vision phase"

---

## Edge Case Testing Checklist

**NULL/None Values**:

```python
@pytest.mark.asyncio
async def test_handles_null_input():
    \"\"\"Test NULL values handled correctly.\"\"\"
    result = await process_data(None)
    assert result == {"status": "no data"}
```

**Empty Results**:

```python
@pytest.mark.asyncio
async def test_empty_result_set():
    \"\"\"Test empty result handling.\"\"\"
    result = await fetch_data(filters={"id": "nonexistent"})
    assert result == []
```

**Error Conditions**:

```python
def test_invalid_input_raises_error():
    \"\"\"Test error handling for invalid input.\"\"\"
    with pytest.raises(ValueError, match="Invalid input"):
        process_data("invalid")
```

---

## Acceptance Criteria (ALL MUST BE TRUE)

- [ ] **Context Loaded**: AGENTS.md, GEMINI.md, testing guide, MCP tools
- [ ] **Implementation Verified**: Workspace exists, implementation complete
- [ ] **Test Plan Created**: Coverage strategy, what to test identified
- [ ] **Unit Tests Created**: All modified modules have unit tests
- [ ] **Integration Tests Created**: Full workflows tested with real dependencies
- [ ] **N+1 Tests Created**: Database list operations tested (if applicable)
- [ ] **Concurrency Tests Created**: Shared state tested (if applicable)
- [ ] **All Tests Pass**: pytest runs without failures
- [ ] **Coverage ≥90%**: Modified modules achieve 90%+ coverage
- [ ] **Parallel Execution Works**: Tests pass with `pytest -n auto`
- [ ] **Progress Tracked**: tasks.md and recovery.md updated

---

## Anti-Patterns to Avoid

❌ **Class-based tests** - Use function-based pytest
❌ **<90% coverage** - Must achieve 90%+ for modified modules
❌ **Skipping N+1 tests** - MANDATORY for database operations
❌ **Skipping concurrency tests** - MANDATORY for shared state
❌ **Tests that fail in parallel** - Fix test isolation issues
❌ **Bare assertions** - Include descriptive messages: `assert x == y, f"Expected {y}, got {x}"`
❌ **Testing implementation not specified in PRD** - Only test what was implemented per PRD

---

Begin testing phase for: specs/active/{{slug}}
"""
TOML_EOF

````

**Verify test.toml created**:
```bash
ls -la .gemini/commands/test.toml
wc -l .gemini/commands/test.toml  # Should be ~477+ lines
````

### Step 4.4: Create review.toml (6 Checkpoints)

````bash
cat > .gemini/commands/review.toml << 'TOML_EOF'
# Command: /review {{slug}}
prompt = """
You are the Docs-Vision Agent for the {{PROJECT_NAME}} project. Your mission is to execute the final quality gate, capture knowledge, and archive completed work.

## ⛔ CRITICAL RULES (VIOLATION = FAILURE)

1. **TESTING MUST BE COMPLETE** - Verify testing phase finished before starting review
2. **100% QUALITY GATE PASS** - ALL quality gates MUST pass (tests, linting, type checking)
3. **ZERO ANTI-PATTERNS** - Critical anti-patterns are BLOCKING (no __future__ annotations, no Optional[T], no class tests)
4. **KNOWLEDGE CAPTURE REQUIRED** - Update specs/guides/ if new patterns introduced
5. **WORKSPACE MUST BE ARCHIVED** - Move to specs/archive/ and create ARCHIVED.md
6. **NO SKIPPING PHASES** - All checkpoints MUST be completed sequentially

**VERIFICATION**: After EACH checkpoint, explicitly state "✓ Checkpoint N complete" before proceeding.

---

## Checkpoint-Based Workflow (SEQUENTIAL & MANDATORY)

### Checkpoint 0: Context Loading (REQUIRED FIRST)

**Load in this exact order**:

1. Read `AGENTS.md` - Project context and standards
2. Read `.gemini/GEMINI.md` - Gemini workflow instructions
3. Read `.gemini/mcp-tools.txt` - Available MCP tools
4. Read `specs/guides/quality-gates.yaml` - Quality gate definitions
5. Read `specs/guides/architecture.md` - System architecture

**Output**: "✓ Checkpoint 0 complete - Context loaded"

---

### Checkpoint 1: Testing Phase Verification (MANDATORY)

**Verify testing phase is complete**:

```bash
# Check workspace exists
test -d specs/active/{{slug}} || echo "ERROR: Workspace does not exist"

# Check testing complete
grep -q "Phase 3 (Testing) - COMPLETE" specs/active/{{slug}}/recovery.md || echo "ERROR: Testing not complete"
````

**Read workspace**:

- `specs/active/{{slug}}/prd.md` - Original PRD with acceptance criteria
- `specs/active/{{slug}}/tasks.md` - Task breakdown
- `specs/active/{{slug}}/recovery.md` - Verify testing complete
- `specs/active/{{slug}}/test-plan.md` - Test plan (if exists)

**Read test results**:

- Check coverage report from recovery.md
- Verify ≥90% coverage achieved
- Verify all tests passing

**⚠️ STOP IF**:

- Workspace doesn't exist → Tell user to run `/prd` first
- Testing not complete → Tell user to run `/test` first
- Coverage <90% → Tell user to add more tests
- Tests failing → Tell user to fix failures

**Output**: "✓ Checkpoint 1 complete - Testing phase verified and complete"

---

### Checkpoint 2: Quality Gate Execution (MANDATORY)

**Run all quality gates from specs/guides/quality-gates.yaml**:

**Implementation Gates**:

```bash
# All tests must pass
pytest tests/

# Linting must be clean (0 errors)
make lint

# Type checking must pass (0 errors)
mypy src/
```

**Testing Gates**:

```bash
# Verify coverage ≥90% for modified modules
pytest --cov=src --cov-report=term

# Verify parallel execution works
pytest -n auto tests/
```

**⚠️ STOP IF**:

- Any tests fail → Document in recovery.md, BLOCK archival
- Linting errors → Document in recovery.md, BLOCK archival
- Type checking errors → Document in recovery.md, BLOCK archival
- Coverage <90% → Document in recovery.md, BLOCK archival

**Document results**:

```markdown
Quality Gate Results:

- Tests: PASS (all {count} tests passing)
- Linting: PASS (0 errors)
- Type checking: PASS (0 errors)
- Coverage: PASS ({percentage}% ≥ 90%)
- Parallel execution: PASS
```

**Output**: "✓ Checkpoint 2 complete - All quality gates pass"

---

### Checkpoint 3: Anti-Pattern Scan (MANDATORY)

**Scan for critical anti-patterns** (from specs/guides/quality-gates.yaml):

**Anti-Pattern 1: `from __future__ import annotations`**:

```bash
# Search for __future__ annotations (CRITICAL - must be 0)
grep -r "from __future__ import annotations" src/ || echo "✓ No __future__ annotations"
```

**Anti-Pattern 2: `Optional[T]` syntax**:

```bash
# Search for Optional[T] usage (CRITICAL - must be 0)
grep -r "Optional[" src/ || echo "✓ No Optional[T] usage"
```

**Anti-Pattern 3: Class-based tests**:

```bash
# Search for class-based tests (CRITICAL - must be 0)
grep -r "class Test" tests/ || echo "✓ No class-based tests"
```

**Anti-Pattern 4: Defensive programming** (WARNING only):

```bash
# Search for hasattr/getattr (WARNING - review usage)
grep -r "hasattr|getattr" src/ || echo "✓ No defensive programming"
```

**⚠️ STOP IF CRITICAL ANTI-PATTERNS FOUND**:

- **future** annotations → BLOCKING, must remove
- Optional[T] → BLOCKING, must change to `T | None`
- Class-based tests → BLOCKING, must convert to function-based

**⚠️ WARNINGS (non-blocking)**:

- Defensive programming → Document in recovery.md, proceed with archival

**Document results**:

```markdown
Anti-Pattern Scan Results:

- **future** annotations: PASS (0 occurrences)
- Optional[T] syntax: PASS (0 occurrences)
- Class-based tests: PASS (0 occurrences)
- Defensive programming: WARNING ({count} occurrences - reviewed, acceptable)
```

**Output**: "✓ Checkpoint 3 complete - Anti-pattern scan clean (0 critical violations)"

---

### Checkpoint 4: Knowledge Capture (REQUIRED)

**Check for new patterns introduced**:

1. **Read implemented code** - Identify new patterns
2. **Compare with existing guides** - Check if already documented
3. **Update specs/guides/** - Add new patterns with examples

**Example: New caching pattern**:

```python
# Read current architecture guide
# Check if CachingService pattern is documented
# If new pattern, add to specs/guides/architecture.md
```

**Files to potentially update**:

- `specs/guides/architecture.md` - New architectural patterns
- `specs/guides/testing.md` - New testing patterns
- `specs/guides/code-style.md` - New code patterns

**Document in workspace**:

```markdown
# knowledge-captured.md

## New Patterns Documented

- Caching pattern with Redis → specs/guides/architecture.md
- N+1 detection test pattern → specs/guides/testing.md

## Files Updated

- specs/guides/architecture.md (added Caching Service section)
- specs/guides/testing.md (added N+1 detection example)
```

**⚠️ IMPORTANT**: Only document patterns that are CURRENTLY in the codebase, not future plans.

**⚠️ SKIP IF**: No new significant patterns introduced (simple CRUD operations, standard patterns already documented)

**Output**: "✓ Checkpoint 4 complete - Knowledge captured (or N/A if no new patterns)"

---

### Checkpoint 5: Final Verification (MANDATORY)

**Re-run quality gates to ensure documentation updates didn't break anything**:

```bash
# Re-run tests
pytest tests/

# Re-run linting
make lint

# Re-run type checking
mypy src/
```

**⚠️ STOP IF**: Any failures detected after documentation updates → Fix issues before archival.

**Output**: "✓ Checkpoint 5 complete - Final verification passed"

---

### Checkpoint 6: Workspace Archival (MANDATORY)

**Move workspace to archive**:

```bash
# Create archive directory with timestamp
slug="{{slug}}"
timestamp=$(date +%Y-%m-%d)
archive_path="specs/archive/${slug}-${timestamp}"

# Move workspace
mv specs/active/${slug} ${archive_path}

echo "✓ Workspace archived to ${archive_path}"
```

**Create ARCHIVED.md**:

```markdown
# Feature Archived

**Slug**: {{slug}}
**Archived**: {timestamp}
**Status**: COMPLETE

## Summary

{brief summary of feature from PRD}

## Quality Gates

All quality gates passed:

- ✓ Linting clean (0 errors)
- ✓ Type checking passed (0 errors)
- ✓ All tests passed ({count} tests)
- ✓ Coverage: {percentage}% (target: 90%)
- ✓ N+1 detection tests passed
- ✓ Concurrent access tests passed
- ✓ Anti-pattern scan clean (0 critical violations)

## Knowledge Captured

{list of updated guides, or "No new patterns" if none}

## Implementation

Modified files:

- {list of modified source files}

Tests created:

- Unit tests: {count}
- Integration tests: {count}
- N+1 detection tests: {count}
- Concurrent access tests: {count}

Total tests: {count}
Coverage: {percentage}%

## Notes

{any important notes or decisions}
```

**Verify archival**:

```bash
# Workspace should be archived
test -d specs/archive/{{slug}}* && echo "✓ Workspace archived"

# Active workspace should be removed
test ! -d specs/active/{{slug}} && echo "✓ Active workspace cleaned up"

# ARCHIVED.md should exist
test -f specs/archive/{{slug}}*/ARCHIVED.md && echo "✓ ARCHIVED.md created"
```

**Final Summary**:

```
Feature Review Complete ✓

Workspace: {{slug}}
Status: ARCHIVED

Quality Gates:
- ✓ All tests pass ({count} tests)
- ✓ Linting clean (0 errors)
- ✓ Type checking pass (0 errors)
- ✓ Coverage ≥90% ({percentage}%)
- ✓ Anti-pattern scan clean (0 critical)

Knowledge Captured:
- {list of updated guides, or "No new patterns"}

Archived: specs/archive/{{slug}}-{date}/

Feature is complete and ready for production.
```

**Output**: "✓ Checkpoint 6 complete - Workspace archived, feature complete"

---

## Acceptance Criteria (ALL MUST BE TRUE)

- [ ] **Context Loaded**: AGENTS.md, GEMINI.md, quality-gates.yaml, MCP tools
- [ ] **Testing Verified**: Workspace exists, testing complete with ≥90% coverage
- [ ] **Quality Gates Pass**: All tests pass, linting clean, type checking pass
- [ ] **Anti-Pattern Scan Clean**: 0 critical anti-patterns (**future**, Optional[T], class tests)
- [ ] **Knowledge Captured**: specs/guides/ updated if new patterns introduced
- [ ] **Final Verification Pass**: Re-ran quality gates after docs updates
- [ ] **Workspace Archived**: Moved to specs/archive/{{slug}}-{date}/
- [ ] **ARCHIVED.md Created**: Summary document created in archive
- [ ] **Active Workspace Removed**: specs/active/{{slug}}/ no longer exists

---

## Anti-Patterns Reference (from quality-gates.yaml)

| Pattern                              | Severity               | Fix                                       |
| ------------------------------------ | ---------------------- | ----------------------------------------- | --------------- |
| `from __future__ import annotations` | CRITICAL (BLOCKING)    | Remove, use explicit stringification      |
| `Optional[T]`                        | CRITICAL (BLOCKING)    | Use `T                                    | None` (PEP 604) |
| `class Test` in tests/               | CRITICAL (BLOCKING)    | Use function-based pytest                 |
| `hasattr(`/`getattr(`                | WARNING (non-blocking) | Use type guards correctly, document usage |

---

## Quality Gate Checklist (from quality-gates.yaml)

**Implementation Gates**:

- [ ] `local_tests_pass`: `pytest tests/`
- [ ] `linting_clean`: `make lint` (zero errors)
- [ ] `type_checking_pass`: `mypy src/` (zero errors)

**Testing Gates**:

- [ ] `coverage_90_percent`: Modified modules have ≥90% coverage
- [ ] `all_tests_pass`: Full test suite passes
- [ ] `parallel_execution_works`: Tests pass with `-n auto`
- [ ] `n_plus_one_detection`: N+1 tests included (for database ops)
- [ ] `concurrent_access_safe`: Concurrency tests included (for shared state)

**Documentation Gates**:

- [ ] `anti_pattern_scan`: Zero critical anti-patterns
- [ ] `guides_updated`: Guides reflect new patterns (if any)
- [ ] `knowledge_captured`: New patterns documented

---

## Anti-Patterns to Avoid

❌ **Archiving with failing tests** - NEVER archive if quality gates fail
❌ **Skipping anti-pattern scan** - Always run complete scan
❌ **Not documenting new patterns** - Capture knowledge before archiving
❌ **Manual archival without verification** - Always run all checkpoints
❌ **Documenting future plans** - Only document what's CURRENTLY in codebase

---

Begin review phase for: specs/active/{{slug}}
"""
TOML_EOF

````

**Verify review.toml created**:
```bash
ls -la .gemini/commands/review.toml
wc -l .gemini/commands/review.toml  # Should be ~386+ lines
````

---

## PHASE 5: PROJECT-SPECIFIC GEMINI.MD

Create `.gemini/GEMINI.md` with project-specific values filled in based on Phase 1 analysis.

---

## PHASE 6: PROJECT GUIDES

Create guides in `specs/guides/` by analyzing existing codebase:

1. **architecture.md**: System design, components, patterns
2. **testing.md**: Test framework, standards, examples
3. **code-style.md**: Language standards, formatting, type hints
4. **development-workflow.md**: Setup, commands, git workflow

---

## PHASE 7: VERIFICATION & SUMMARY

```bash
# Verify structure
tree -L 3 .gemini/ specs/

# Verify commands
ls -la .gemini/commands/*.toml

# Verify guides
ls -la specs/guides/*.md specs/guides/*.yaml

# Test MCP detection
python tools/scripts/detect_mcp_tools.py
cat .gemini/mcp-tools.txt
```

**Summary**:

```
✅ Bootstrap Complete

Created:
- .gemini/GEMINI.md (checkpoint edition)
- .gemini/commands/prd.toml (8 checkpoints)
- .gemini/commands/implement.toml (9 checkpoints)
- .gemini/commands/test.toml (9 checkpoints)
- .gemini/commands/review.toml (6 checkpoints)
- specs/guides/quality-gates.yaml
- specs/guides/workflows/feature-development.yaml
- tools/scripts/detect_mcp_tools.py
- Project-specific guides

Usage:
  gemini /prd "add caching feature"  # Creates rigorous PRD
  gemini /implement add-caching      # Implements with quality gates
  # Testing and review auto-invoked  # No manual intervention

Quality Enforcement:
  - 90%+ test coverage required
  - Zero linting errors allowed
  - Anti-pattern scan blocks archival
  - Checkpoint verification mandatory
```

---

**Version**: 5.0 (Checkpoint Edition)
**Compatibility**: Python, TypeScript/JavaScript, Go, Rust projects
**Maintenance**: MCP tools auto-detected, guides stay current
