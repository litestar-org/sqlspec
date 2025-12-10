# Gemini Agent System Bootstrap - Intelligent Workflow Edition

**Version**: 6.0 (Intelligent Edition)
**Purpose**: Create a self-aware, adaptive agent system with contextual intelligence

This bootstrap creates an intelligent Gemini agent system with:

- ✅ **Context-Aware Analysis** - Agents understand project patterns before acting
- ✅ **Adaptive Checkpoints** - Workflow depth adjusts to task complexity
- ✅ **Knowledge Synthesis** - Automatic pattern extraction and documentation
- ✅ **Intelligent Tool Selection** - MCP tool usage based on task requirements
- ✅ **Quality Enforcement** - Multi-tier validation with graceful degradation
- ✅ **Self-Documenting** - Captures learnings for future agent sessions
- ✅ **Cross-Agent Memory** - Shared knowledge base evolves over time

**Execution**: Run this entire prompt with Gemini in your project root.
**Philosophy**: Agents should learn from the codebase, not just execute commands.

---

## BOOTSTRAP PHILOSOPHY

### Intelligence Principles

1. **Context First, Code Second**
   - Read existing patterns before creating new ones
   - Understand project conventions from actual code
   - Adapt to project's unique architectural style

2. **Adaptive Complexity**
   - Simple tasks get streamlined workflows
   - Complex features trigger deep analysis
   - Checkpoint count scales with complexity

3. **Knowledge Accumulation**
   - Every feature adds to project guides
   - Patterns become reusable templates
   - Future agents inherit all learnings

4. **Graceful Degradation**
   - Missing tools trigger fallback strategies
   - Optional features don't block progress
   - Clear communication when capabilities limited

---

## PHASE 1: INTELLIGENT PROJECT ANALYSIS

### Step 1.1: Deep Codebase Understanding

**Don't just detect - understand WHY patterns exist:**

```bash
# Detect project structure
ls -la
find . -type f -name "*.py" -o -name "*.ts" -o -name "*.go" -o -name "*.rs" | head -20

# Read configuration to understand philosophy
cat pyproject.toml package.json Cargo.toml go.mod 2>/dev/null
```

**Key questions to answer:**

1. **Architecture Philosophy**:
   - Is this a monolith or microservices?
   - Does it use domain-driven design?
   - What's the layering strategy (controller → service → repository)?

2. **Type System Approach**:
   - Strict typing or dynamic?
   - Type hints usage patterns?
   - Validation strategy (runtime vs compile-time)?

3. **Testing Philosophy**:
   - TDD or test-after?
   - Unit vs integration test ratio?
   - Coverage expectations?

4. **Code Organization**:
   - Feature-based or layer-based folders?
   - Naming conventions (verb_noun vs noun_verb)?
   - File size preferences?

**Document findings in workspace for future reference.**

### Step 1.2: Extract Existing Patterns

**Read actual code to discover patterns:**

```bash
# Find adapter/plugin patterns
find src/ -type f -name "*adapter*" -o -name "*plugin*" -o -name "*extension*"

# Find service patterns
find src/ -type f -name "*service*" -o -name "*manager*" -o -name "*handler*"

# Find configuration patterns
find src/ -type f -name "*config*" -o -name "*settings*"

# Find error handling patterns
grep -r "class.*Error" src/ | head -10
grep -r "raise.*from" src/ | head -10

# Find async patterns
grep -r "async def" src/ | wc -l
grep -r "await" src/ | wc -l
```

**Pattern Analysis**:

1. **Read 3-5 example files** for each pattern type
2. **Identify common structure** (class hierarchy, decorators, mixins)
3. **Note naming conventions** (verbs, prefixes, suffixes)
4. **Extract docstring style** (Google, NumPy, reStructuredText)
5. **Understand error handling** (custom exceptions, context managers)

### Step 1.3: Analyze Testing Patterns

**Learn from existing tests:**

```bash
# Find test organization
find tests/ -type f -name "test_*.py" -o -name "*_test.py" | head -20

# Read a sample test file
cat tests/unit/test_*.py | head -100
cat tests/integration/test_*.py | head -100

# Analyze test structure
grep -r "def test_" tests/ | wc -l
grep -r "class Test" tests/ | wc -l
grep -r "@pytest.fixture" tests/ | wc -l
```

**Test Pattern Questions**:

1. Function-based or class-based tests?
2. Fixture organization (conftest.py or inline)?
3. Mocking strategy (unittest.mock, pytest-mock)?
4. Async test patterns?
5. Integration test setup (databases, external services)?

### Step 1.4: Discover Documentation Style

```bash
# Find existing documentation
find docs/ -name "*.md" -o -name "*.rst" 2>/dev/null | head -20

# Read doc examples
cat docs/guides/*.md 2>/dev/null | head -200

# Check for README patterns
cat README.md | head -100
```

**Documentation Intelligence**:

1. **Tone**: Formal vs conversational?
2. **Examples**: Code-heavy or conceptual?
3. **Organization**: Task-based or reference-based?
4. **Depth**: High-level overview or detailed tutorials?

---

## PHASE 2: INTELLIGENT MCP TOOL DETECTION

Create **adaptive** MCP tool detection with fallback strategies:

```bash
mkdir -p .gemini/tools
cat > .gemini/tools/detect_mcp.py << 'EOF'
#!/usr/bin/env python3
"""Intelligent MCP tool detection with capability mapping."""

from dataclasses import dataclass
from enum import Enum


class ToolCapability(Enum):
    """MCP tool capability categories."""
    REASONING = "reasoning"  # Deep thinking tools
    RESEARCH = "research"    # Documentation lookup
    PLANNING = "planning"    # Workflow organization
    ANALYSIS = "analysis"    # Code analysis
    DEBUG = "debug"          # Problem investigation


@dataclass
class MCPTool:
    """MCP tool with capability metadata."""
    name: str
    available: bool
    capability: ToolCapability
    fallback: str | None = None
    use_cases: list[str] | None = None


def detect_mcp_tools() -> dict[str, MCPTool]:
    """Detect available MCP tools with intelligent fallback mapping."""

    tools = {
        # Reasoning tools (prefer crash, fallback to sequential_thinking)
        'crash': MCPTool(
            name='crash',
            available=False,  # Auto-detect
            capability=ToolCapability.REASONING,
            fallback='sequential_thinking',
            use_cases=[
                'Complex architectural decisions',
                'Multi-branch design exploration',
                'Iterative problem refinement',
            ]
        ),
        'sequential_thinking': MCPTool(
            name='sequential_thinking',
            available=False,
            capability=ToolCapability.REASONING,
            fallback=None,  # Last resort
            use_cases=[
                'Linear problem breakdown',
                'Step-by-step analysis',
                'Fallback when crash unavailable',
            ]
        ),

        # Research tools
        'context7': MCPTool(
            name='context7',
            available=False,
            capability=ToolCapability.RESEARCH,
            fallback='web_search',
            use_cases=[
                'Library documentation lookup',
                'API reference retrieval',
                'Best practices research',
            ]
        ),
        'web_search': MCPTool(
            name='web_search',
            available=False,
            capability=ToolCapability.RESEARCH,
            fallback=None,
            use_cases=[
                'Latest framework updates',
                'Community best practices',
                'Fallback documentation lookup',
            ]
        ),

        # Planning tools
        'zen_planner': MCPTool(
            name='zen_planner',
            available=False,
            capability=ToolCapability.PLANNING,
            use_cases=[
                'Multi-phase project planning',
                'Migration strategy design',
                'Complex feature breakdown',
            ]
        ),

        # Analysis tools
        'zen_thinkdeep': MCPTool(
            name='zen_thinkdeep',
            available=False,
            capability=ToolCapability.ANALYSIS,
            use_cases=[
                'Architecture review',
                'Performance analysis',
                'Security assessment',
            ]
        ),
        'zen_analyze': MCPTool(
            name='zen_analyze',
            available=False,
            capability=ToolCapability.ANALYSIS,
            use_cases=[
                'Code quality analysis',
                'Pattern detection',
                'Tech debt assessment',
            ]
        ),

        # Debug tools
        'zen_debug': MCPTool(
            name='zen_debug',
            available=False,
            capability=ToolCapability.DEBUG,
            use_cases=[
                'Root cause investigation',
                'Bug reproduction',
                'Performance debugging',
            ]
        ),
        'zen_consensus': MCPTool(
            name='zen_consensus',
            available=False,
            capability=ToolCapability.PLANNING,
            use_cases=[
                'Architecture decision making',
                'Technology selection',
                'Multi-model validation',
            ]
        ),
    }

    # Auto-detection logic would go here
    # For bootstrap: detect from environment or config

    return tools


def generate_tool_strategy(tools: dict[str, MCPTool]) -> str:
    """Generate intelligent tool usage strategy."""

    strategy = ["# MCP Tool Strategy\n\n"]

    by_capability = {}
    for tool in tools.values():
        if tool.capability not in by_capability:
            by_capability[tool.capability] = []
        by_capability[tool.capability].append(tool)

    for capability, tool_list in by_capability.items():
        strategy.append(f"## {capability.value.title()} Tools\n\n")

        available = [t for t in tool_list if t.available]
        unavailable = [t for t in tool_list if not t.available]

        if available:
            primary = available[0]
            strategy.append(f"**Primary**: `{primary.name}`\n\n")

            if primary.use_cases:
                strategy.append("Use when:\n\n")
                for use_case in primary.use_cases:
                    strategy.append(f"- {use_case}\n")
                strategy.append("\n")

            if primary.fallback:
                fallback_tool = tools.get(primary.fallback)
                if fallback_tool and not fallback_tool.available:
                    strategy.append(f"**Fallback**: Manual {capability.value} (no tools available)\n\n")
                elif fallback_tool:
                    strategy.append(f"**Fallback**: `{primary.fallback}`\n\n")
        else:
            strategy.append(f"⚠️ No tools available - manual {capability.value} required\n\n")

    return "".join(strategy)


if __name__ == "__main__":
    tools = detect_mcp_tools()

    # Generate strategy document
    strategy = generate_tool_strategy(tools)

    with open('.gemini/mcp-strategy.md', 'w') as f:
        f.write(strategy)

    # Generate availability list
    with open('.gemini/mcp-tools.txt', 'w') as f:
        f.write("Available MCP Tools (Auto-Detected):\n\n")
        for tool in tools.values():
            status = "✓ Available" if tool.available else "✗ Not available"
            f.write(f"- {tool.name}: {status}\n")
            if tool.fallback:
                f.write(f"  Fallback: {tool.fallback}\n")

    print("✓ MCP tool detection complete")
    print("✓ Generated .gemini/mcp-tools.txt")
    print("✓ Generated .gemini/mcp-strategy.md")
EOF

chmod +x .gemini/tools/detect_mcp.py
python .gemini/tools/detect_mcp.py
```

---

## PHASE 3: ADAPTIVE INFRASTRUCTURE

### Step 3.1: Create Intelligent Directory Structure

```bash
mkdir -p .gemini/commands
mkdir -p .gemini/tools
mkdir -p .gemini/templates
mkdir -p specs/active specs/archive
mkdir -p specs/guides/patterns specs/guides/workflows specs/guides/examples
mkdir -p specs/template-spec/research specs/template-spec/tmp
touch specs/active/.gitkeep specs/archive/.gitkeep
```

**Structure Intelligence**:

- `.gemini/tools/` - Reusable scripts for agents
- `.gemini/templates/` - Customizable PRD/task templates
- `specs/guides/patterns/` - Extracted code patterns
- `specs/guides/examples/` - Real implementation examples

### Step 3.2: Create Adaptive Quality Gates

```yaml
# specs/guides/quality-gates.yaml
metadata:
  version: "2.0"
  adaptive: true
  description: "Quality gates that adapt to project conventions"

implementation_gates:
  - name: local_tests_pass
    command: "pytest tests/"  # Auto-detect from project
    required: true
    adaptive: true  # Adjust based on project test strategy
    description: "All tests must pass before proceeding"

  - name: linting_clean
    command: "make lint"  # Prefer make targets
    fallback: "ruff check ."  # Fallback to direct tool
    required: true
    description: "Zero linting errors allowed"

  - name: type_checking_pass
    command: "mypy src/"
    required: true
    adaptive: true  # Skip if project doesn't use type hints
    description: "Type checking must pass"

testing_gates:
  - name: coverage_threshold
    threshold: 90  # Can be project-specific
    scope: "modified_modules"
    adaptive: true  # Adjust based on project norms
    description: "Modified modules must achieve configured coverage"

  - name: test_isolation
    command: "pytest -n auto tests/"
    required: true
    description: "Tests must work in parallel execution"

  - name: n_plus_one_detection
    type: "custom"
    applicable_when: "database_operations"
    description: "Database operations must include N+1 query detection tests"
    examples:
      - "tests/integration/test_queries.py::test_list_no_n_plus_one"

documentation_gates:
  - name: anti_pattern_scan
    adaptive: true  # Patterns loaded from project guides
    rules:
      - pattern: "from __future__ import annotations"
        severity: "error"
        message: "Use explicit stringification instead"
        context: "Breaks mypyc compilation"

      - pattern: "Optional\\["
        severity: "error"
        message: "Use T | None (PEP 604) instead"
        context: "Modern Python 3.10+ style"

  - name: pattern_documentation
    description: "New patterns must be captured in specs/guides/patterns/"
    required: true

  - name: example_code
    description: "Complex patterns need working examples in specs/guides/examples/"
    required_when: "architectural_change"
```

### Step 3.3: Create Intelligent Workflow Templates

```yaml
# specs/guides/workflows/intelligent-development.yaml
workflow_name: "Intelligent Feature Development"
version: "2.0"
adaptive: true

phases:
  - name: "context_analysis"
    duration: "auto"  # Scales with project complexity
    agent: "prd"
    steps:
      - name: "load_project_context"
        description: "Read AGENTS.md, existing guides, codebase patterns"
        outputs:
          - "Context understanding document"

      - name: "identify_similar_features"
        description: "Find similar implementations to learn from"
        tools: ["grep", "find", "read"]
        outputs:
          - "Pattern analysis"

      - name: "assess_complexity"
        description: "Determine feature complexity level"
        criteria:
          - "Lines of code impacted"
          - "Number of components affected"
          - "Integration points"
          - "Database schema changes"
        outputs:
          - "Complexity: simple|medium|complex"

      - name: "adapt_workflow"
        description: "Adjust checkpoint count based on complexity"
        adaptive_rules:
          simple: "6 checkpoints"
          medium: "9 checkpoints"
          complex: "12+ checkpoints"

  - name: "planning"
    agent: "prd"
    gemini_command: "/prd {feature-description}"
    adaptive_checkpoints: true  # Count adjusts based on complexity
    quality_gates:
      - "context_loaded"
      - "research_complete"
      - "patterns_identified"

  - name: "implementation"
    agent: "expert"
    auto_trigger: false  # User must explicitly run
    adaptive: true
    quality_gates:
      - "local_tests_pass"
      - "linting_clean"
      - "follows_project_patterns"

  - name: "knowledge_capture"
    agent: "docs-vision"
    auto_trigger: true
    description: "Extract and document new patterns"
    outputs:
      - "specs/guides/patterns/{pattern-name}.md"
      - "specs/guides/examples/{example-name}.py"
      - "Updated AGENTS.md"
```

### Step 3.4: Update .gitignore

```bash
# Add to .gitignore
cat >> .gitignore << 'EOF'

# Gemini Agent System
specs/active/
specs/archive/
.gemini/mcp-tools.txt
.gemini/mcp-strategy.md
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

## PHASE 4: INTELLIGENT CHECKPOINT-BASED COMMANDS

### Step 4.1: Create Intelligent prd.toml

````bash
cat > .gemini/commands/prd.toml << 'TOML_EOF'
# Command: /prd "create a PRD for..."
prompt = """
You are the PRD Agent for the {{PROJECT_NAME}} project with INTELLIGENCE ENHANCEMENTS.

## 🧠 INTELLIGENCE LAYER

Before starting checkpoints, activate intelligence mode:

1. **Read MCP Strategy**: Load `.gemini/mcp-strategy.md` for tool selection guidance
2. **Learn from Codebase**: Read 3-5 similar implementations before planning
3. **Assess Complexity**: Determine if this is simple/medium/complex feature
4. **Adapt Workflow**: Adjust checkpoint depth based on complexity assessment

## ⛔ CRITICAL RULES (VIOLATION = FAILURE)

1. **CONTEXT FIRST** - You MUST read existing patterns before planning new ones
2. **NO CODE MODIFICATION** - You MUST NOT modify any source code during PRD phase
3. **WORKSPACE FIRST** - You MUST create workspace BEFORE starting research
4. **INTELLIGENT TOOL USE** - Check `.gemini/mcp-strategy.md` for tool selection
5. **PATTERN LEARNING** - Identify 3-5 similar features and learn from them
6. **ADAPTIVE DEPTH** - Simple features: 6 checkpoints, Medium: 8, Complex: 10+
7. **RESEARCH GROUNDED** - Minimum 2000+ words of research
8. **COMPREHENSIVE PRD** - Minimum 3200+ words PRD with specific acceptance criteria
9. **GIT VERIFICATION** - Verify git status shows no src/ changes at end

**VERIFICATION**: After EACH checkpoint, explicitly state "✓ Checkpoint N complete" before proceeding.

---

## Checkpoint-Based Workflow (ADAPTIVE & SEQUENTIAL)

### Checkpoint 0: Intelligence Bootstrap (REQUIRED FIRST)

**Load project intelligence:**

1. Read `AGENTS.md` - Project overview, tech stack, development commands
2. Read `.gemini/GEMINI.md` - Gemini agent workflow instructions
3. Read `.gemini/mcp-strategy.md` - Intelligent tool selection guide
4. Read `specs/guides/architecture.md` - System architecture patterns
5. Read `specs/guides/code-style.md` - Code quality standards
6. Read `specs/guides/patterns/README.md` - Available pattern library

**Learn from existing implementations:**

```bash
# Find similar features
grep -r "class.*Adapter" src/ | head -5
grep -r "class.*Service" src/ | head -5
grep -r "class.*Config" src/ | head -5

# Read 3 example files to understand patterns
cat src/adapters/example1/config.py | head -100
cat src/adapters/example2/config.py | head -100
cat src/adapters/example3/config.py | head -100
```

**Assess feature complexity:**

- **Simple**: Single file, CRUD operation, config change → 6 checkpoints
- **Medium**: New service/adapter, API endpoint, 2-3 files → 8 checkpoints
- **Complex**: Architecture change, multi-component, 5+ files → 10+ checkpoints

**Output**:
```
✓ Checkpoint 0 complete - Intelligence bootstrapped
Complexity assessed: [simple|medium|complex]
Checkpoint count adapted: [6|8|10+]
Similar features identified: [list 3-5]
```

---

### Checkpoint 1: Requirement Analysis with Pattern Recognition

**Understand the user's request:**
- What is being requested?
- Why is it needed?
- What are the expected outcomes?

**Identify similar implementations (MANDATORY):**

```bash
# Search for related patterns
grep -r "{keyword}" src/
find src/ -name "*{pattern}*"
find specs/guides/patterns/ -name "*{pattern}*"
```

**Read similar implementations:**

1. **Read at least 3 similar files** to learn project conventions
2. **Extract naming patterns** (class names, method names, file structure)
3. **Identify common dependencies** (base classes, mixins, imports)
4. **Note testing patterns** from existing test files

**Document pattern analysis:**

```markdown
## Similar Implementations Found

1. `src/adapters/asyncpg/config.py` - Async database config pattern
2. `src/adapters/psycopg/config.py` - Sync database config pattern
3. `src/adapters/duckdb/config.py` - Simple config pattern

## Common Patterns Observed

- All inherit from `AsyncDatabaseConfig` or `SyncDatabaseConfig`
- All use TypedDict for `driver_features`
- All have `_create_pool()` and `_init_connection()` methods
- All auto-detect optional dependencies
```

**Output**: "✓ Checkpoint 1 complete - Requirements analyzed, patterns identified"

---

### Checkpoint 2: Workspace Creation (BEFORE RESEARCH)

**⚠️ CRITICAL**: Workspace MUST be created BEFORE any research begins.

**Generate unique slug:**

```python
slug = feature_name.lower().replace(" ", "-").replace("_", "-")
# Example: "Add Redis Caching" → "add-redis-caching"
```

**Create intelligent directory structure:**

```bash
mkdir -p specs/active/{{slug}}/research
mkdir -p specs/active/{{slug}}/tmp
mkdir -p specs/active/{{slug}}/patterns  # NEW: Store pattern analysis
```

**Create placeholder files:**

```bash
touch specs/active/{{slug}}/prd.md
touch specs/active/{{slug}}/tasks.md
touch specs/active/{{slug}}/recovery.md
touch specs/active/{{slug}}/research/plan.md
touch specs/active/{{slug}}/patterns/analysis.md  # NEW: Pattern insights
```

**Verify workspace created:**

```bash
ls -la specs/active/{{slug}}/
```

**Output**: "✓ Checkpoint 2 complete - Workspace created at specs/active/{{slug}}/"

---

### Checkpoint 3: Intelligent Deep Analysis

**⚠️ CRITICAL**: Use intelligent tool selection from `.gemini/mcp-strategy.md`

**Step 3.1 - Check tool availability:**

Read `.gemini/mcp-tools.txt` to see what's available:
- Primary reasoning tool: crash (preferred) or sequential_thinking (fallback)
- Research tools: context7 or web_search
- Planning tools: zen_planner if available

**Step 3.2 - Select appropriate tool based on complexity:**

**Simple feature**: Manual planning (10 structured thoughts minimum)
**Medium feature**: crash (12 steps) or sequential_thinking (15 thoughts)
**Complex feature**: crash (18+ steps) with branching, or zen_planner

**Step 3.3 - Execute structured analysis:**

```python
# Example with crash (preferred)
mcp__crash__crash(
    step_number=1,
    estimated_total=12,  # Adjusted based on complexity
    purpose="analysis",
    thought="Learn from similar adapter implementations before designing",
    next_action="Map common patterns to new feature",
    outcome="pending",
    rationale="Understanding existing patterns ensures consistency",
    context="Initial planning with pattern recognition"
)
# Continue with iterative crash steps
```

**Step 3.4 - Document analysis in workspace:**

```markdown
# specs/active/{{slug}}/patterns/analysis.md

## Tool Used

- Primary: crash (12 steps)
- Fallback: N/A (crash available)

## Analysis Summary

{Summary of structured thinking}

## Pattern Insights

1. Existing adapters follow TypedDict pattern
2. Auto-detection is standard for optional dependencies
3. Session callbacks used for connection initialization

## Key Findings

1. {Finding 1}
2. {Finding 2}
   ...
```

**Output**: "✓ Checkpoint 3 complete - Intelligent analysis finished using [tool name]"

---

### Checkpoint 4: Research Best Practices with Pattern Library

**⚠️ CRITICAL**: Research MUST produce minimum 2000+ words of documented findings.

**Research priority order:**

**Priority 1 - Pattern Library** (NEW - ALWAYS FIRST):

```bash
# Check pattern library
cat specs/guides/patterns/adapter-pattern.md
cat specs/guides/patterns/config-pattern.md
cat specs/guides/patterns/error-handling.md
```

**Priority 2 - Internal Guides**:

```bash
cat specs/guides/architecture.md
cat specs/guides/testing.md
cat specs/guides/code-style.md
```

**Priority 3 - Project Documentation**:

```bash
# Read existing similar code
# Find patterns in codebase
# Understand conventions
```

**Priority 4 - Context7** (if available in `.gemini/mcp-tools.txt`):

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

**Priority 5 - WebSearch** (if available):

```python
WebSearch(query="Python async database pooling best practices 2025")
```

**Document research in workspace:**

```markdown
# specs/active/{{slug}}/research/plan.md

## Research Findings

### Pattern Library Insights (NEW)

{What pattern library shows - 150+ words}

### Internal Patterns

{What patterns exist in the codebase - 150+ words}

### Library Best Practices

{What library docs recommend - 150+ words}

### Industry Best Practices

{What web search revealed - 50+ words}

**Total**: {count} words (minimum 2000 required)
```

**Verify word count:**

```bash
wc -w specs/active/{{slug}}/research/plan.md
```

**⚠️ STOP IF**: Research document is <2000 words → Add more research.

**Output**: "✓ Checkpoint 4 complete - Research finished (2000+ words documented)"

---

### Checkpoint 5: Write Comprehensive PRD with Pattern References

**⚠️ CRITICAL**: PRD MUST be minimum 3200+ words with specific, measurable acceptance criteria.

**Use template from `specs/template-spec/prd.md` if it exists.**

**PRD Template with Intelligence** (`specs/active/{{slug}}/prd.md`):

```markdown
> **User Prompt**: {{USER_PROMPT}}

# Feature: {Feature Name}

## Intelligence Context (NEW)

**Complexity**: [simple|medium|complex]
**Similar Features**:
- `src/path/to/similar1.py`
- `src/path/to/similar2.py`
- `src/path/to/similar3.py`

**Patterns to Follow**:
- [Pattern 1 from library](../../../specs/guides/patterns/pattern1.md)
- [Pattern 2 from library](../../../specs/guides/patterns/pattern2.md)

**Tool Selection**:
- Reasoning: crash (12 steps used)
- Research: context7 + pattern library
- Testing: Standard pytest patterns

---

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

**Pattern Compliance** (NEW):
- [ ] Follows existing adapter pattern structure
- [ ] Uses TypedDict for configuration
- [ ] Implements auto-detection for optional deps
- [ ] Consistent naming with similar features

## Technical Design

### Affected Components

**Backend ({LANGUAGE})**:

- Modules: `src/{path}/`
- Services: `{ServiceName}` (new/modified)
- Schemas: `{SchemaName}` (new/modified)
- Database: {migrations if needed}
- Tests: Unit + integration + N+1 detection

### Implementation Approach

{High-level design approach - 200+ words}

**Pattern Alignment** (NEW):
- Follows pattern from: [similar-feature](../../../specs/guides/patterns/...)
- Reuses base classes: `AsyncDatabaseConfig`
- Consistent with project conventions

**Phase 1**: {description}
**Phase 2**: {description}
**Phase 3**: {description}

### Code Samples (MANDATORY)

**Service signature** (Following existing patterns):

```{language}
class NewAdapter(AsyncDatabaseConfig):  # Pattern: inherit from base
    """New adapter following project conventions."""

    async def _create_pool(self) -> Pool:  # Pattern: standard method
        """Create connection pool."""
        ...
```

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

### Pattern Test Coverage (NEW)

- [ ] Test follows existing test patterns
- [ ] Uses function-based pytest (not class-based)
- [ ] Fixtures match project conventions
- [ ] Integration tests use real dependencies

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

- Similar Implementation 1: [path/to/similar1.py]
- Similar Implementation 2: [path/to/similar2.py]
- Pattern Library: [specs/guides/patterns/](../../specs/guides/patterns/)
- Architecture: [specs/guides/architecture.md](../../specs/guides/architecture.md)
- Research: [specs/active/{{slug}}/research/plan.md](./research/plan.md)
```

**Verify word count:**

```bash
wc -w specs/active/{{slug}}/prd.md
```

**⚠️ STOP IF**: PRD is <3200 words → Add more detail.

**Output**: "✓ Checkpoint 5 complete - PRD written (3200+ words) with pattern references"

---

### Checkpoint 6: Task Breakdown (ADAPTIVE)

**Task breakdown adapts to complexity:**

- **Simple feature**: High-level tasks only
- **Medium feature**: Detailed phase breakdown
- **Complex feature**: Granular task tracking

**Create actionable task list** (`specs/active/{{slug}}/tasks.md`):

```markdown
# Implementation Tasks: {Feature Name}

**Complexity**: [simple|medium|complex]
**Estimated Checkpoints**: [6|8|10+]

## Phase 1: Planning & Research ✓

- [x] PRD created
- [x] Research documented (2000+ words)
- [x] Patterns identified (3-5 similar features)
- [x] Workspace setup
- [x] Deep analysis completed

## Phase 2: Core Implementation

**Pattern Compliance**:
- [ ] Follow structure from: `src/similar/feature.py`
- [ ] Use TypedDict pattern for config
- [ ] Implement standard base class methods

**Backend**:

- [ ] Create/modify: `src/{module}/{file}.{ext}`
- [ ] Implement business logic
- [ ] Add error handling
- [ ] Add docstrings (Google style)

## Phase 3: Testing (Auto via /test command)

- [ ] Unit tests (90%+ coverage)
- [ ] Integration tests
- [ ] Edge case tests (NULL, empty, errors)
- [ ] Pattern compliance tests

## Phase 4: Documentation (Auto via /review command)

- [ ] Update specs/guides/ (if new patterns)
- [ ] Extract patterns to specs/guides/patterns/
- [ ] Create examples in specs/guides/examples/
- [ ] Quality gate passed

## Phase 5: Archival

- [ ] Workspace moved to specs/archive/
- [ ] ARCHIVED.md created
- [ ] Pattern library updated (if new patterns)
```

**Output**: "✓ Checkpoint 6 complete - Tasks broken down (adapted to complexity)"

---

### Checkpoint 7: Recovery Guide with Intelligence Context

**Create resumption instructions** (`specs/active/{{slug}}/recovery.md`):

```markdown
# Recovery Guide: {Feature Name}

**Slug**: {{slug}}
**Created**: {date}
**Status**: Planning Complete
**Complexity**: [simple|medium|complex]

## Intelligence Context (NEW)

**Similar Features Analyzed**:
1. `src/path/to/similar1.py` - Primary reference
2. `src/path/to/similar2.py` - Secondary reference
3. `src/path/to/similar3.py` - Tertiary reference

**Patterns to Follow**:
- [Adapter Pattern](../../guides/patterns/adapter-pattern.md)
- [Config Pattern](../../guides/patterns/config-pattern.md)

**Tool Strategy Used**:
- Reasoning: crash (12 steps)
- Research: context7 + pattern library
- Testing: Standard pytest

## Current Phase

Phase 1 (Planning) - COMPLETE

Checkpoints completed:

- ✓ Checkpoint 0: Intelligence bootstrapped
- ✓ Checkpoint 1: Requirements analyzed, patterns identified
- ✓ Checkpoint 2: Workspace created
- ✓ Checkpoint 3: Intelligent analysis completed
- ✓ Checkpoint 4: Research completed (2000+ words)
- ✓ Checkpoint 5: PRD written (3200+ words)
- ✓ Checkpoint 6: Tasks broken down
- ✓ Checkpoint 7: Recovery guide created

## Next Steps

**Ready for implementation**:

1. Run `/implement {{slug}}` to start implementation phase
2. Implementation agent will follow identified patterns
3. Testing agent will automatically be invoked after implementation
4. Docs-vision agent will extract new patterns to library

## Important Context

**Key components to be modified/created**:

- {list main files/modules from Technical Design}

**Pattern compliance checklist**:
- Follow structure from similar features
- Use identified naming conventions
- Reuse base classes and mixins

**Research findings**: See [research/plan.md](./research/plan.md)
**Pattern analysis**: See [patterns/analysis.md](./patterns/analysis.md)
**Acceptance criteria**: See [prd.md](./prd.md) - {count} criteria

## Resumption Instructions

**If session interrupted during implementation**:

1. Read [prd.md](./prd.md) for complete requirements
2. Read [patterns/analysis.md](./patterns/analysis.md) for pattern guidance
3. Review similar features listed above
4. Continue from first unchecked task in tasks.md
```

**Output**: "✓ Checkpoint 7 complete - Recovery guide created with intelligence context"

---

### Checkpoint 8: Git Verification (MANDATORY - NO CODE MODIFIED)

**⚠️ CRITICAL**: PRD phase must NOT modify any source code.

**Verify git status:**

```bash
# Check for any changes in source directories
git status --porcelain src/ | grep -v "^??"

# If command returns anything, CODE WAS MODIFIED - VIOLATION!
```

**Expected result**: Empty (no output) or only untracked files

**If source code was modified:**

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

**If no code modified:**

```markdown
✓ Git verification passed - no source code modified
```

**Final summary:**

```
PRD Phase Complete ✓

Workspace: specs/active/{{slug}}/
Status: Ready for implementation
Complexity: [simple|medium|complex]
Checkpoints: [6|8|10+] completed

Intelligence Enhancements:
- ✓ Pattern library consulted
- ✓ Similar features analyzed (3-5 examples)
- ✓ Tool selection optimized
- ✓ Complexity-adapted workflow

Deliverables:
- ✓ Workspace created
- ✓ Intelligent analysis completed
- ✓ Research completed (2000+ words)
- ✓ PRD written (3200+ words) with pattern references
- ✓ Tasks broken down (adapted to complexity)
- ✓ Recovery guide created
- ✓ NO source code modified

Next step: Run `/implement {{slug}}`
```

**Output**: "✓ Checkpoint 8 complete - PRD phase finished, ready for implementation"

---

## Acceptance Criteria (ALL MUST BE TRUE)

- [ ] **Intelligence Bootstrapped**: MCP strategy, pattern library, similar features loaded
- [ ] **Complexity Assessed**: Simple/medium/complex determination documented
- [ ] **Patterns Identified**: 3-5 similar features analyzed
- [ ] **Context Loaded**: AGENTS.md, GEMINI.md, guides, MCP tools read
- [ ] **Requirements Analyzed**: Clear understanding with pattern alignment
- [ ] **Workspace Created**: specs/active/{{slug}}/ with intelligence artifacts
- [ ] **Intelligent Analysis**: Appropriate tool used based on complexity
- [ ] **Research Complete**: 2000+ words with pattern library insights
- [ ] **PRD Written**: 3200+ words with specific acceptance criteria and pattern references
- [ ] **Tasks Broken Down**: Testable chunks adapted to complexity
- [ ] **Recovery Guide Created**: Clear resumption instructions with intelligence context
- [ ] **Git Clean**: NO source code modifications

---

## Anti-Patterns to Avoid

❌ **Skipping pattern analysis** - Must identify 3-5 similar features
❌ **Ignoring complexity assessment** - Workflow must adapt to feature scope
❌ **Bypassing tool strategy** - Must consult `.gemini/mcp-strategy.md`
❌ **Modifying source code** - PRD is planning only
❌ **Vague acceptance criteria** - Must be specific and measurable
❌ **Skipping pattern library** - Must consult `specs/guides/patterns/`
❌ **Insufficient research** - Minimum 2000 words required
❌ **Short PRD** - Minimum 3200 words required

---

Begin intelligent PRD creation phase: "{user_request}"
"""
TOML_EOF
````

**Verify prd.toml created:**

```bash
ls -la .gemini/commands/prd.toml
wc -l .gemini/commands/prd.toml  # Should be ~700+ lines
```

---

### Step 4.2: Create Intelligent implement.toml

````bash
cat > .gemini/commands/implement.toml << 'TOML_EOF'
# Command: /implement {{slug}}
prompt = """
You are the Expert Agent for the {{PROJECT_NAME}} project with INTELLIGENCE ENHANCEMENTS.

## 🧠 INTELLIGENCE LAYER

Before starting checkpoints, activate intelligence mode:

1. **Read Intelligence Context**: Load pattern analysis from `specs/active/{{slug}}/patterns/`
2. **Review Similar Features**: Read the 3-5 similar implementations identified in PRD
3. **Load Pattern Library**: Read relevant patterns from `specs/guides/patterns/`
4. **Check Tool Strategy**: Consult `.gemini/mcp-strategy.md` for implementation decisions

## ⛔ CRITICAL RULES (VIOLATION = FAILURE)

1. **PATTERN COMPLIANCE** - You MUST follow patterns from similar features
2. **PRD MUST EXIST** - Verify PRD workspace exists and is complete
3. **NO NEW FEATURES** - ONLY implement what's specified in the PRD
4. **SEQUENTIAL EXECUTION** - Complete each checkpoint before proceeding
5. **LOCAL TESTS REQUIRED** - Run local tests and linting BEFORE invoking sub-agents
6. **SUB-AGENTS MANDATORY** - Invoke testing agent, then docs-vision agent (in order)
7. **PATTERN EXTRACTION** - Document any new patterns discovered during implementation

**VERIFICATION**: After EACH checkpoint, explicitly state "✓ Checkpoint N complete" before proceeding.

---

## Checkpoint-Based Workflow (ADAPTIVE & SEQUENTIAL)

### Checkpoint 0: Intelligence Bootstrap (REQUIRED FIRST)

**Load project intelligence:**

1. Read `AGENTS.md` - Project context, tech stack, standards
2. Read `.gemini/GEMINI.md` - Gemini workflow instructions
3. Read `.gemini/mcp-strategy.md` - Tool selection guide
4. Read `specs/guides/architecture.md` - System architecture
5. Read `specs/guides/code-style.md` - Code quality standards
6. Read `specs/guides/patterns/README.md` - Pattern library index

**Load feature-specific intelligence:**

```bash
# Read intelligence artifacts from PRD phase
cat specs/active/{{slug}}/patterns/analysis.md
cat specs/active/{{slug}}/research/plan.md
```

**Output**: "✓ Checkpoint 0 complete - Intelligence bootstrapped, ready for pattern-guided implementation"

---

### Checkpoint 1: PRD Verification with Pattern Loading

**Verify workspace exists and is complete:**

```bash
# Check workspace exists
test -d specs/active/{{slug}} || echo "ERROR: Workspace does not exist"

# Check required files
test -f specs/active/{{slug}}/prd.md || echo "ERROR: prd.md missing"
test -f specs/active/{{slug}}/tasks.md || echo "ERROR: tasks.md missing"
test -f specs/active/{{slug}}/recovery.md || echo "ERROR: recovery.md missing"
test -f specs/active/{{slug}}/patterns/analysis.md || echo "ERROR: pattern analysis missing"
```

**Read PRD workspace:**

- `specs/active/{{slug}}/prd.md` - Full PRD with acceptance criteria
- `specs/active/{{slug}}/tasks.md` - Task breakdown
- `specs/active/{{slug}}/recovery.md` - Recovery guide with intelligence context
- `specs/active/{{slug}}/patterns/analysis.md` - Pattern insights
- `specs/active/{{slug}}/research/plan.md` - Research notes

**Extract pattern references from PRD:**

```markdown
## Patterns to Follow (from PRD)

1. Similar Feature 1: `src/path/to/similar1.py`
2. Similar Feature 2: `src/path/to/similar2.py`
3. Similar Feature 3: `src/path/to/similar3.py`

## Pattern Library References

- [Pattern Name 1](../../../specs/guides/patterns/pattern1.md)
- [Pattern Name 2](../../../specs/guides/patterns/pattern2.md)
```

**Verify git is clean:**

```bash
git status --porcelain src/ | grep -v "^??" && echo "ERROR: Uncommitted changes in src/"
```

**⚠️ STOP IF**:

- Workspace doesn't exist → Tell user to run `/prd` first
- Pattern analysis missing → PRD phase incomplete
- Git is dirty → Tell user to commit or stash changes first

**Output**: "✓ Checkpoint 1 complete - PRD verified, patterns loaded"

---

### Checkpoint 2: Pattern Deep Dive (MANDATORY BEFORE CODING)

**Read and analyze similar implementations (from PRD):**

```bash
# Read the 3-5 similar features identified in PRD
cat src/path/to/similar1.py
cat src/path/to/similar2.py
cat src/path/to/similar3.py

# Read pattern library guides
cat specs/guides/patterns/adapter-pattern.md
cat specs/guides/patterns/config-pattern.md
```

**Extract implementation patterns:**

1. **Class Structure**: Base classes, inheritance hierarchy
2. **Method Signatures**: Standard method names and parameters
3. **Naming Conventions**: File names, class names, variable names
4. **Import Patterns**: What gets imported from where
5. **Docstring Style**: Google/NumPy/reStructuredText
6. **Error Handling**: Exception types and patterns

**Document pattern compliance plan:**

```markdown
# specs/active/{{slug}}/tmp/implementation-plan.md

## Pattern Compliance Checklist

### Class Structure (from similar1.py)
- [ ] Inherit from `AsyncDatabaseConfig`
- [ ] Implement `_create_pool()` method
- [ ] Implement `_init_connection()` method
- [ ] Use TypedDict for `driver_features`

### Naming Conventions (from similar2.py)
- [ ] Config class: `{Adapter}Config`
- [ ] Driver features: `{Adapter}DriverFeatures`
- [ ] File structure: `adapters/{adapter}/config.py`

### Error Handling (from similar3.py)
- [ ] Use `ImproperConfigurationError` for config issues
- [ ] Use `raise ... from e` for exception chaining
- [ ] Log with `logger.error()` before raising

### Documentation (from project standards)
- [ ] Google-style docstrings
- [ ] Include Args, Returns, Raises sections
- [ ] Add usage examples for complex APIs
```

**Use Context7 for library docs** (if needed):

```python
mcp__context7__resolve_library_id(libraryName="litestar")
mcp__context7__get_library_docs(
    context7CompatibleLibraryID="/litestar-org/litestar",
    topic="dependency injection",
    tokens=5000
)
```

**Output**: "✓ Checkpoint 2 complete - Patterns analyzed, compliance plan created"

---

### Checkpoint 3: Implementation Planning (NO CODE YET)

**Create detailed implementation plan following patterns:**

```markdown
## Implementation Plan

### Files to Create/Modify

Following pattern from `similar1.py`:

1. `src/adapters/{adapter}/config.py` - Main configuration
2. `src/adapters/{adapter}/driver.py` - Driver implementation
3. `src/adapters/{adapter}/_types.py` - Type definitions
4. `src/adapters/{adapter}/__init__.py` - Public exports

### Pattern-Guided Implementation Steps

**Step 1: Create TypedDict (Pattern from similar1.py)**
```python
class {Adapter}DriverFeatures(TypedDict):
    \"\"\"Feature flags for {adapter}.\"\"\"
    enable_feature: NotRequired[bool]
```

**Step 2: Create Config Class (Pattern from similar1.py)**
```python
class {Adapter}Config(AsyncDatabaseConfig):
    \"\"\"Configuration for {adapter} adapter.\"\"\"

    def __init__(self, *, driver_features=None, **kwargs):
        # Auto-detection pattern
        features = dict(driver_features) if driver_features else {}
        if "enable_feature" not in features:
            features["enable_feature"] = FEATURE_INSTALLED
        super().__init__(driver_features=features, **kwargs)
```

**Step 3: Implement Pool Creation (Pattern from similar2.py)**
```python
async def _create_pool(self) -> Pool:
    \"\"\"Create connection pool.\"\"\"
    config = dict(self.connection_config)
    # Pattern: session callback for initialization
    if self.driver_features.get("enable_feature", False):
        config["session_callback"] = self._init_connection
    return await create_pool(**config)
```

### Dependencies to Add

Following similar features:
- `pip install {adapter_package}` (required)
- `pip install {optional_dep}` (optional feature)

### Integration Points

Based on similar implementations:
- Imports from `sqlspec.config` for base classes
- Imports from `sqlspec.exceptions` for errors
- Integration with `sqlspec.typing` for type detection
```

**Verify scope matches PRD:**

- Compare plan against PRD acceptance criteria
- Ensure following identified patterns
- Flag any ambiguities or missing details

**Output**: "✓ Checkpoint 3 complete - Pattern-guided implementation plan created (NO CODE MODIFIED)"

---

### Checkpoint 4: Code Implementation (PATTERN COMPLIANCE)

**Quality Standards (MANDATORY - FROM PROJECT):**

**Type Annotations**:

- ✅ Use `T | None` (PEP 604)
- ❌ NO `Optional[T]`
- ❌ NO `from __future__ import annotations`

**Async/Await** (if async adapter):

- ✅ All I/O operations must be async
- ✅ Use `async def` for database operations
- ✅ Use `await` for all async calls

**Docstrings** (Google Style - project standard):

- ✅ Google Style for all public functions/classes
- ✅ Include Args, Returns, Raises sections
- ✅ Include examples for complex APIs

**Error Handling** (from similar implementations):

- ✅ Use specific exception types from project
- ✅ Include context with `raise ... from e`
- ❌ NO bare `except Exception`

**Implementation Process (Pattern-Guided)**:

1. **Copy structure from similar feature** (identified in PRD)
2. **Adapt class names** following project conventions
3. **Preserve method signatures** from base classes
4. **Follow naming patterns** from similar implementations
5. **Reuse error handling patterns**
6. **Match docstring style** exactly

**Pattern Compliance Verification**:

After writing each file:

```markdown
✓ File: src/adapters/{adapter}/config.py
  - [x] Follows structure from similar1.py
  - [x] Uses TypedDict pattern
  - [x] Implements standard methods
  - [x] Google-style docstrings
  - [x] Proper error handling
  - [x] Type hints (PEP 604)
```

**Output**: "✓ Checkpoint 4 complete - Code implementation finished (pattern-compliant)"

---

### Checkpoint 5: Local Testing (MANDATORY BEFORE SUB-AGENTS)

**Run tests for modified modules:**

```bash
# Run relevant unit tests
pytest tests/unit/path/to/test_module.py -v

# Run integration tests if applicable
pytest tests/integration/test_module.py -v
```

**Run linting:**

```bash
make lint
```

**Fix ALL linting errors** - Zero tolerance for linting failures.

**Run type checking:**

```bash
mypy src/
```

**⚠️ STOP IF**:

- Tests fail → Fix failures before proceeding
- Linting errors → Fix ALL errors before proceeding
- Type errors → Fix ALL errors before proceeding

**Auto-fix if possible:**

```bash
make fix  # Auto-fix formatting issues
```

**Output**: "✓ Checkpoint 5 complete - All local tests pass, linting clean, type checking passes"

---

### Checkpoint 6: Pattern Extraction (NEW - CAPTURE LEARNINGS)

**Document any NEW patterns discovered during implementation:**

```bash
# Check if implementation introduced new patterns
# If yes, extract to pattern library
```

**Create pattern documentation if new approach used:**

```markdown
# specs/active/{{slug}}/tmp/new-patterns.md

## New Patterns Discovered

### Pattern: Session Callback for Type Handlers

**Context**: Used in implementation of {feature}

**Problem**: Need to register type handlers on each connection

**Solution**:
```python
async def _create_pool(self):
    config = dict(self.connection_config)
    if self.driver_features.get("enable_feature", False):
        config["session_callback"] = self._init_connection
    return await create_pool(**config)

async def _init_connection(self, connection):
    if self.driver_features.get("enable_feature", False):
        from ._feature_handlers import register_handlers
        register_handlers(connection)
```

**When to Use**: When optional type handlers needed for database features

**Examples**: See `src/adapters/{adapter}/config.py`
```

**Mark for docs-vision agent to extract:**

```markdown
# specs/active/{{slug}}/tmp/docs-todo.md

## Pattern Extraction Tasks for Docs-Vision Agent

- [ ] Extract "Session Callback for Type Handlers" to specs/guides/patterns/
- [ ] Create example in specs/guides/examples/
- [ ] Update AGENTS.md with new pattern
```

**Output**: "✓ Checkpoint 6 complete - New patterns documented for extraction"

---

### Checkpoint 7: Progress Update (REQUIRED)

**Update tasks.md:**

- Mark completed tasks with `[x]`
- Add notes about pattern compliance
- Flag any deviations from original plan

**Update recovery.md:**

- Update phase status: "Phase 2 (Implementation) - COMPLETE"
- List all modified files
- Document pattern compliance
- Note any new patterns discovered

**Verify updates saved:**

```bash
git status specs/active/{{slug}}/ | grep -E "(tasks|recovery).md"
```

**Output**: "✓ Checkpoint 7 complete - Progress tracked in workspace"

---

### Checkpoint 8: Auto-Invoke Testing Agent (MANDATORY)

**This is NOT optional. You MUST invoke the testing agent.**

**Invocation:**

```text
Execute testing agent workflow for specs/active/{{slug}}.

Context:
- Implementation complete for all acceptance criteria
- Modified files: {list_of_modified_files}
- Pattern compliance verified
- Local tests passed
- Linting clean
- Type checking passed

Requirements:
- Achieve 90%+ test coverage for modified modules
- Test all acceptance criteria from PRD
- Follow existing test patterns from similar features
- Include N+1 query detection tests (if database operations)
- Include concurrent access tests (if shared state)
- Test edge cases: NULL, empty, errors
- All tests must be function-based (NOT class-based)

Success criteria:
- All tests pass
- Coverage ≥ 90% for modified modules
- Tests work in parallel (pytest -n auto)
- Pattern-compliant test structure
```

**Wait for testing agent to complete successfully.**

**⚠️ STOP IF**: Testing agent reports failures → Fix issues and re-run.

**Output**: "✓ Checkpoint 8 complete - Testing agent finished successfully"

---

### Checkpoint 9: Auto-Invoke Docs-Vision Agent (MANDATORY)

**This is NOT optional. You MUST invoke the docs-vision agent.**

**Invocation:**

```text
Execute Docs & Vision agent workflow for specs/active/{{slug}}.

Context:
- Implementation complete
- All tests passing with 90%+ coverage
- Testing phase complete
- Modified files: {list_of_modified_files}
- New patterns discovered: {yes/no}

Requirements:
- Run anti-pattern scan
- Update specs/guides/ if new patterns introduced
- Extract new patterns to specs/guides/patterns/
- Create examples in specs/guides/examples/
- Update AGENTS.md with learnings
- Verify all quality gates pass
- Archive workspace to specs/archive/{{slug}}/
- Create ARCHIVED.md with summary

Pattern extraction tasks:
- Extract patterns from: specs/active/{{slug}}/tmp/new-patterns.md
- Create examples for new patterns
- Update pattern library index

Quality gates to verify:
- Linting: 0 errors
- Type checking: 0 errors
- Tests: All passing
- Coverage: ≥90% for modified modules
- Anti-patterns: 0 critical violations
- Pattern compliance: Verified

Success criteria:
- All quality gates pass
- Patterns extracted to library
- Examples created
- Workspace archived
- Knowledge captured
```

**Wait for docs-vision agent to complete successfully.**

**⚠️ STOP IF**: Docs-vision agent reports failures → Fix issues and re-run.

**Output**: "✓ Checkpoint 9 complete - Docs-vision agent finished, patterns extracted to library"

---

### Checkpoint 10: Final Verification (COMPLETE)

**Verify workspace archived:**

```bash
# Workspace should be archived
test -d specs/archive/{{slug}}* && echo "✓ Workspace archived"

# Active workspace should be removed
test ! -d specs/active/{{slug}} && echo "✓ Active workspace cleaned up"
```

**Verify pattern library updated:**

```bash
# Check if new patterns added
ls -la specs/guides/patterns/
grep "{{slug}}" specs/guides/patterns/README.md
```

**Final Summary:**

```text
Feature Implementation Complete ✓

Workspace: {{slug}}
Status: ARCHIVED

Intelligence Enhancements:
- ✓ Followed patterns from similar features
- ✓ Pattern compliance verified
- ✓ New patterns extracted to library
- ✓ Examples created for reuse

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
- ✓ Pattern compliance verified

Pattern Library Updated:
- New patterns: {count}
- Examples added: {count}
- AGENTS.md updated: Yes

Archived: specs/archive/{{slug}}-{date}/
```

**Output**: "✓ Checkpoint 10 complete - Feature complete, patterns preserved for future use"

---

## Acceptance Criteria (ALL MUST BE TRUE)

- [ ] **Intelligence Bootstrapped**: Patterns and context loaded
- [ ] **PRD Verified**: Workspace exists, complete, git clean
- [ ] **Patterns Analyzed**: Similar features studied
- [ ] **Compliance Plan Created**: Pattern adherence documented
- [ ] **Plan Created**: Implementation plan follows patterns (no code yet)
- [ ] **Code Written**: All acceptance criteria implemented following patterns
- [ ] **Local Tests Pass**: pytest passes for modified modules
- [ ] **Linting Clean**: `make lint` returns 0 errors
- [ ] **Type Checking Pass**: Type checker returns 0 errors
- [ ] **Pattern Extraction**: New patterns documented
- [ ] **Progress Tracked**: tasks.md and recovery.md updated
- [ ] **Testing Agent Invoked**: Testing phase completed
- [ ] **Docs-Vision Agent Invoked**: Patterns extracted, workspace archived
- [ ] **Workspace Archived**: Moved to specs/archive/{{slug}}-{date}/
- [ ] **Pattern Library Updated**: New patterns added to library

---

## Anti-Patterns to Avoid

❌ **Ignoring pattern analysis** - Must read similar features first
❌ **Breaking existing patterns** - Follow project conventions
❌ **Skipping pattern extraction** - Document new approaches
❌ **Starting without PRD** - Always verify PRD complete
❌ **Adding new features** - Only implement what's in PRD
❌ **Skipping local tests** - Always run pytest/linting first
❌ **Using Optional[T]** - Use `T | None` (PEP 604)
❌ **Class-based tests** - Use function-based pytest
❌ **Forgetting sub-agents** - Testing and docs-vision MANDATORY
❌ **Not updating pattern library** - Capture learnings

---

Begin intelligent implementation for: specs/active/{{slug}}
"""
TOML_EOF
````

**Verify implement.toml created:**

```bash
ls -la .gemini/commands/implement.toml
wc -l .gemini/commands/implement.toml  # Should be ~3200+ lines
```

---

### Step 4.3: Intelligent test.toml Command

Create `.gemini/commands/test.toml`:

```toml
name = "test"
description = "Execute comprehensive testing phase with intelligent test pattern recognition"

prompt = """
# INTELLIGENT TESTING PHASE

Execute comprehensive testing for feature in `specs/active/{{slug}}/`.

## 🧠 INTELLIGENCE LAYER

Before starting checkpoints, activate intelligence mode:

1. **Read Pattern Library**: Load test patterns from `specs/guides/patterns/test-*.md`
2. **Learn from Similar Tests**: Read test files for the 3-5 similar features identified in PRD
3. **Load MCP Strategy**: Use `.gemini/mcp-strategy.md` for tool selection
4. **Assess Test Complexity**: Determine test coverage needs based on feature complexity

Create analysis document: `specs/active/{{slug}}/tmp/test-analysis.md`

---

## CHECKPOINT-BASED EXECUTION

Follow each checkpoint sequentially. Mark complete before proceeding.

---

## Checkpoint 0: Intelligence Bootstrap

**Actions:**

1. **Load workspace context:**
```bash
cd specs/active/{{slug}}
cat prd.md | head -50
cat tasks.md | grep -A5 "Implementation Phase"
ls -la tmp/
```

2. **Assess test complexity:**

- **Simple Feature**: Single file, basic CRUD → 6 test checkpoints (unit + integration)
- **Medium Feature**: New service/adapter, 2-3 files → 8 test checkpoints (unit + integration + edge cases)
- **Complex Feature**: Architecture change, multi-component → 9+ test checkpoints (unit + integration + edge cases + performance)

3. **Identify similar test files:**

```bash
# Find tests for similar features
find tests/ -type f -name "test_*" | grep -i "{feature_keyword}"

# Read 3-5 example test files
cat tests/unit/test_similar_feature1.py | head -100
cat tests/integration/test_adapters/test_asyncpg/test_similar_feature2.py | head -100
```

4. **Create test analysis:**

```markdown
# specs/active/{{slug}}/tmp/test-analysis.md

## Test Complexity Assessment

**Feature Type**: [simple|medium|complex]
**Test Checkpoint Count**: [6|8|9+]

## Similar Test Files Analyzed

1. `tests/unit/test_similar1.py` - Unit test patterns for {similar_feature_1}
2. `tests/integration/test_adapters/test_asyncpg/test_similar2.py` - Integration patterns
3. `tests/integration/test_similar3.py` - Edge case patterns

## Key Test Patterns Identified

### Pattern 1: [Pattern Name]
- **File**: tests/unit/test_example.py
- **Lines**: 10-30
- **Purpose**: {what this pattern tests}
- **Reusable for**: {our feature aspect}

### Pattern 2: [Pattern Name]
- **File**: tests/integration/test_example.py
- **Lines**: 45-80
- **Purpose**: {what this pattern tests}
- **Reusable for**: {our feature aspect}

## Test Coverage Requirements

Based on similar features:
- **Unit Tests**: {list areas needing unit tests}
- **Integration Tests**: {list adapter-specific tests needed}
- **Edge Cases**: {list edge cases to test}
- **Performance Tests**: {if applicable}
```

5. **Update tasks:**

```markdown
# specs/active/{{slug}}/tasks.md

## Testing Phase
- [x] Checkpoint 0: Intelligence bootstrap complete
  - Test complexity: [simple|medium|complex]
  - Checkpoints adapted: [6|8|9+]
  - Similar tests identified: [list 3-5]
  - Test patterns analyzed
```

**Output:**

```
✓ Checkpoint 0 complete - Test intelligence bootstrapped
Test complexity assessed: [simple|medium|complex]
Checkpoint count adapted: [6|8|9+]
Similar test files identified: [list 3-5]
Key test patterns documented in tmp/test-analysis.md
```

---

## Checkpoint 1: Unit Test Creation

**Actions:**

1. **Review feature implementation:**

```bash
# Find files created/modified in implementation
git diff --name-only main...HEAD | grep "sqlspec/"
```

2. **Follow test patterns from analysis:**

```markdown
Read `specs/active/{{slug}}/tmp/test-analysis.md` for patterns.

For each new file/class created:
- Match pattern from similar tests
- Use same fixture structure
- Follow same assertion patterns
```

3. **Create unit tests:**

```python
# tests/unit/test_{feature_name}.py

\"\"\"Unit tests for {feature_name}.

Following patterns from:
- tests/unit/test_similar1.py (Pattern: {pattern_name})
- tests/unit/test_similar2.py (Pattern: {pattern_name})
\"\"\"
import pytest
from unittest.mock import Mock, patch

# Import feature components
from sqlspec.{module} import {FeatureClass}


class TestFeatureClass:
    \"\"\"Test {FeatureClass} functionality.\"\"\"

    def test_{basic_functionality}(self):
        \"\"\"Test basic {functionality} works correctly.\"\"\"
        # Pattern: Basic success case
        # Similar to: tests/unit/test_similar1.py:25-40

        # Arrange
        instance = {FeatureClass}(...)

        # Act
        result = instance.method(...)

        # Assert
        assert result == expected_value

    def test_{edge_case}(self):
        \"\"\"Test {edge_case} is handled correctly.\"\"\"
        # Pattern: Edge case handling
        # Similar to: tests/unit/test_similar1.py:55-70

        # Arrange
        instance = {FeatureClass}(...)

        # Act & Assert
        with pytest.raises(ExpectedException):
            instance.method(invalid_input)

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            (val1, expected1),
            (val2, expected2),
            (val3, expected3),
        ],
    )
    def test_{parameterized_case}(self, input_val, expected):
        \"\"\"Test {functionality} with multiple inputs.\"\"\"
        # Pattern: Parameterized testing
        # Similar to: tests/unit/test_similar2.py:80-95

        instance = {FeatureClass}(...)
        result = instance.method(input_val)
        assert result == expected
```

4. **Run unit tests:**

```bash
uv run pytest tests/unit/test_{feature_name}.py -v
```

5. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 1: Unit tests created
  - Files: tests/unit/test_{feature_name}.py
  - Test count: {X} tests
  - Status: All passing ✓
```

**Output:**

```
✓ Checkpoint 1 complete - Unit tests created and passing
Unit test file: tests/unit/test_{feature_name}.py
Test count: {X}
Pattern compliance verified
```

---

## Checkpoint 2: Integration Test Setup

**Actions:**

1. **Determine adapter coverage:**

```markdown
Based on feature implementation, determine which adapters need testing:

- [ ] asyncpg (if PostgreSQL-specific)
- [ ] psycopg (if PostgreSQL-specific)
- [ ] asyncmy (if MySQL-specific)
- [ ] aiosqlite (if SQLite-specific)
- [ ] duckdb (if DuckDB-specific)
- [ ] oracle (if Oracle-specific)
- [ ] bigquery (if BigQuery-specific)
- [ ] ALL adapters (if core feature)
```

2. **Review integration test patterns:**

```bash
# Read integration tests for similar features
cat tests/integration/test_adapters/test_asyncpg/test_similar_feature.py | head -150
cat tests/integration/test_similar_cross_adapter.py | head -150
```

3. **Create integration test structure:**

```bash
# If adapter-specific:
touch tests/integration/test_adapters/test_{adapter}/test_{feature_name}.py

# If cross-adapter:
touch tests/integration/test_{feature_name}.py
```

4. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 2: Integration test structure created
  - Adapter coverage: [list adapters]
  - Test files created: [list files]
```

**Output:**

```
✓ Checkpoint 2 complete - Integration test structure ready
Adapter coverage: [list]
Test files created: [list]
```

---

## Checkpoint 3: AsyncPG Integration Tests

**Actions:**

1. **Create AsyncPG tests:**

```python
# tests/integration/test_adapters/test_asyncpg/test_{feature_name}.py

\"\"\"Integration tests for {feature_name} with AsyncPG adapter.

Following patterns from:
- tests/integration/test_adapters/test_asyncpg/test_similar1.py
\"\"\"
import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.base import SQLSpec


@pytest.mark.asyncio
@pytest.mark.asyncpg
async def test_{feature}_basic_usage(asyncpg_dsn):
    \"\"\"Test {feature} works with AsyncPG adapter.\"\"\"
    # Pattern: Basic adapter integration
    # Similar to: test_similar1.py:30-55

    sql = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": asyncpg_dsn})
    sql.add_config(config)

    async with sql.provide_session(config) as session:
        # Test feature functionality
        result = await session.{feature_method}(...)
        assert result.{expected_property} == expected_value


@pytest.mark.asyncio
@pytest.mark.asyncpg
async def test_{feature}_edge_case(asyncpg_dsn):
    \"\"\"Test {feature} edge case with AsyncPG.\"\"\"
    # Pattern: Edge case testing
    # Similar to: test_similar1.py:70-90

    sql = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": asyncpg_dsn})
    sql.add_config(config)

    async with sql.provide_session(config) as session:
        # Test edge case
        with pytest.raises(ExpectedException):
            await session.{feature_method}(invalid_input)
```

2. **Run AsyncPG tests:**

```bash
uv run pytest tests/integration/test_adapters/test_asyncpg/test_{feature_name}.py -v
```

3. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 3: AsyncPG integration tests complete
  - Test count: {X} tests
  - Status: All passing ✓
```

**Output:**

```
✓ Checkpoint 3 complete - AsyncPG integration tests passing
Test count: {X}
```

---

## Checkpoint 4: Additional Adapter Tests

**Actions:**

1. **For each additional adapter, create tests:**

```python
# tests/integration/test_adapters/test_{adapter}/test_{feature_name}.py

\"\"\"Integration tests for {feature_name} with {Adapter} adapter.

Following patterns from:
- tests/integration/test_adapters/test_{adapter}/test_similar1.py
- tests/integration/test_adapters/test_asyncpg/test_{feature_name}.py (template)
\"\"\"

# Similar structure to AsyncPG tests, adapter-specific
```

2. **Run each adapter test:**

```bash
uv run pytest tests/integration/test_adapters/test_psycopg/test_{feature_name}.py -v
uv run pytest tests/integration/test_adapters/test_sqlite/test_{feature_name}.py -v
# ... for each adapter
```

3. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 4: Additional adapter tests complete
  - Psycopg: {X} tests ✓
  - SQLite: {X} tests ✓
  - [other adapters]: {X} tests ✓
```

**Output:**

```
✓ Checkpoint 4 complete - All adapter integration tests passing
Adapters tested: [list]
Total integration tests: {X}
```

---

## Checkpoint 5: Edge Case Testing

**Actions:**

1. **Review edge cases from analysis:**

```bash
cat specs/active/{{slug}}/tmp/test-analysis.md | grep -A10 "Edge Cases"
```

2. **Create edge case tests:**

```python
# Add to existing test files

@pytest.mark.parametrize(
    "edge_input,expected_behavior",
    [
        (None, "raises TypeError"),
        (empty_value, "returns empty result"),
        (invalid_type, "raises ValueError"),
        (boundary_value, "handles correctly"),
    ],
)
async def test_{feature}_edge_cases(edge_input, expected_behavior, session):
    \"\"\"Test {feature} handles edge cases correctly.\"\"\"
    # Pattern: Comprehensive edge case coverage
    # Similar to: test_similar2.py:120-150

    if "raises" in expected_behavior:
        exception_type = eval(expected_behavior.split()[1])
        with pytest.raises(exception_type):
            await session.{feature_method}(edge_input)
    else:
        result = await session.{feature_method}(edge_input)
        # Assert expected behavior
```

3. **Run edge case tests:**

```bash
uv run pytest -k "edge_case" -v
```

4. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 5: Edge case testing complete
  - Edge cases tested: {X}
  - Status: All passing ✓
```

**Output:**

```
✓ Checkpoint 5 complete - Edge cases covered
Edge case tests: {X}
```

---

## Checkpoint 6: Test Coverage Verification

**Actions:**

1. **Run coverage report:**

```bash
uv run pytest --cov=sqlspec.{module} --cov-report=term-missing tests/
```

2. **Verify coverage thresholds:**

```markdown
Based on similar features, verify:

- **Unit Test Coverage**: ≥90% for new code
- **Integration Coverage**: ≥80% for adapter interactions
- **Edge Case Coverage**: All identified edge cases tested

If below thresholds:
1. Identify uncovered lines
2. Add missing tests
3. Re-run coverage
```

3. **Document coverage:**

```markdown
# specs/active/{{slug}}/tmp/test-coverage.md

## Coverage Report

### Unit Tests
- Coverage: {X}%
- Uncovered lines: [list if any]

### Integration Tests
- AsyncPG: {X}%
- Psycopg: {X}%
- [other adapters]: {X}%

### Edge Cases
- Total edge cases: {X}
- Covered: {X}
- Coverage: {X}%

## Threshold Compliance
- [x] Unit coverage ≥90%
- [x] Integration coverage ≥80%
- [x] All edge cases tested
```

4. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 6: Coverage verification complete
  - Overall coverage: {X}%
  - Unit coverage: {X}%
  - Integration coverage: {X}%
  - Thresholds met: ✓
```

**Output:**

```
✓ Checkpoint 6 complete - Coverage thresholds met
Overall coverage: {X}%
Unit: {X}% | Integration: {X}% | Edge cases: {X}
```

---

## Checkpoint 7: Performance Testing (if applicable)

**Actions:**

1. **Determine if performance testing needed:**

```markdown
Performance testing required if:
- Feature affects query execution time
- Feature involves data transformation
- Feature adds overhead to existing operations
- Similar features have performance tests
```

2. **Create performance benchmarks:**

```python
# tests/integration/test_{feature_name}_performance.py

\"\"\"Performance tests for {feature_name}.

Following patterns from:
- tests/integration/test_performance_similar.py
\"\"\"
import pytest
import time


@pytest.mark.asyncio
@pytest.mark.performance
async def test_{feature}_performance_baseline(session):
    \"\"\"Benchmark {feature} performance.\"\"\"
    # Pattern: Performance baseline
    # Similar to: test_performance_similar.py:40-65

    iterations = 1000

    start = time.perf_counter()
    for _ in range(iterations):
        await session.{feature_method}(test_input)
    end = time.perf_counter()

    avg_time = (end - start) / iterations

    # Assert performance threshold
    assert avg_time < 0.010  # 10ms threshold (adjust based on similar features)
```

3. **Run performance tests:**

```bash
uv run pytest -k "performance" -v
```

4. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 7: Performance testing complete
  - Baseline: {X}ms avg
  - Threshold: {X}ms
  - Status: Within threshold ✓
```

**Output:**

```
✓ Checkpoint 7 complete - Performance benchmarks established
Average execution time: {X}ms
Threshold met: ✓
```

---

## Checkpoint 8: Final Test Suite Execution

**Actions:**

1. **Run complete test suite:**

```bash
# Run all tests for feature
uv run pytest -k "{feature_name}" -v

# Run full test suite to ensure no regressions
uv run pytest -n 2 --dist=loadgroup tests/
```

2. **Verify all tests pass:**

```markdown
Expected results:
- All new tests: PASSED
- Existing tests: PASSED (no regressions)
- Coverage: Above thresholds
```

3. **Create test summary:**

```markdown
# specs/active/{{slug}}/tmp/test-summary.md

## Test Suite Summary

### Unit Tests
- Files: [list]
- Test count: {X}
- Status: All passing ✓

### Integration Tests
- Adapters tested: [list]
- Test count per adapter: {X}
- Total integration tests: {X}
- Status: All passing ✓

### Edge Case Tests
- Edge cases: {X}
- Status: All passing ✓

### Performance Tests
- Benchmarks: {X}
- Status: All within thresholds ✓

### Coverage
- Overall: {X}%
- Unit: {X}%
- Integration: {X}%
- Thresholds: Met ✓

### Total Test Count
- New tests added: {X}
- Total project tests: {X}
- Test execution time: {X}s
```

4. **Update tasks:**

```markdown
## Testing Phase
- [x] Checkpoint 8: Final test suite execution complete
  - All tests passing: ✓
  - No regressions: ✓
  - Coverage thresholds met: ✓
  - Ready for review phase
```

5. **Update recovery.md:**

```markdown
# specs/active/{{slug}}/recovery.md

## Current Status: TESTING COMPLETE ✓

Testing phase finished. Ready for documentation and review.

## Next Actions

1. Execute `/review` command
2. Docs & Vision agent will:
   - Update documentation
   - Extract test patterns
   - Archive workspace
```

**Output:**

```
✓ Checkpoint 8 complete - Testing phase finished

Test Summary:
- Unit tests: {X} ✓
- Integration tests: {X} ✓
- Edge case tests: {X} ✓
- Performance tests: {X} ✓
- Coverage: {X}% ✓

Ready for review phase.
Run: /review
```

---

## FINAL DELIVERABLES

1. ✅ **Test Files Created:**
   - `tests/unit/test_{feature_name}.py` - Unit tests following similar test patterns
   - `tests/integration/test_adapters/test_{adapter}/test_{feature_name}.py` - Integration tests per adapter
   - `tests/integration/test_{feature_name}_performance.py` - Performance benchmarks (if applicable)

2. ✅ **Test Documentation:**
   - `specs/active/{{slug}}/tmp/test-analysis.md` - Test pattern analysis
   - `specs/active/{{slug}}/tmp/test-coverage.md` - Coverage report
   - `specs/active/{{slug}}/tmp/test-summary.md` - Complete test summary

3. ✅ **Workspace Updates:**
   - `specs/active/{{slug}}/tasks.md` - Testing phase checkpoints marked complete
   - `specs/active/{{slug}}/recovery.md` - Updated to "Testing Complete"

4. ✅ **Quality Gates:**
   - All tests passing
   - Coverage thresholds met (Unit ≥90%, Integration ≥80%)
   - No regressions in existing tests
   - Performance benchmarks established (if applicable)

---

## NEXT STEP

Execute `/review` command to begin documentation and review phase.
"""

```

**Save:**
```bash
cat > .gemini/commands/test.toml << 'TOML_CONTENT'
[paste above content]
TOML_CONTENT

# Verify
ls -la .gemini/commands/test.toml
wc -l .gemini/commands/test.toml  # Should be ~650+ lines
```

---

### Step 4.4: Intelligent review.toml Command

Create `.gemini/commands/review.toml`:

```toml
name = "review"
description = "Execute documentation, quality gates, pattern extraction, and workspace archival"

prompt = """
# INTELLIGENT DOCS & VISION PHASE

Execute comprehensive documentation, quality validation, and knowledge capture for `specs/active/{{slug}}/`.

## 🧠 INTELLIGENCE LAYER

Before starting checkpoints, activate intelligence mode:

1. **Read Pattern Library**: Load existing patterns from `specs/guides/patterns/`
2. **Load MCP Strategy**: Use `.gemini/mcp-strategy.md` for tool selection
3. **Prepare Pattern Extraction**: Identify new reusable patterns from implementation
4. **Load Quality Gates**: Read `specs/guides/quality-gates.yaml` for validation criteria

This phase has 5 sub-phases:
1. **Documentation Update** (Checkpoints 1-2)
2. **Quality Gate Validation** (Checkpoint 3)
3. **Knowledge Capture** (Checkpoint 4)
4. **Re-validation** (Checkpoint 5)
5. **Workspace Cleanup & Archive** (Checkpoint 6)

---

## CHECKPOINT-BASED EXECUTION

Follow each checkpoint sequentially. Mark complete before proceeding.

---

## Checkpoint 0: Intelligence Bootstrap

**Actions:**

1. **Load workspace context:**
```bash
cd specs/active/{{slug}}
cat prd.md | head -50
cat tasks.md
ls -la tmp/
```

2. **Review implementation artifacts:**

```bash
# Find files modified
git diff --name-only main...HEAD

# Review implementation summary
cat tmp/implementation-summary.md

# Review test summary
cat tmp/test-summary.md
```

3. **Identify new patterns to extract:**

```markdown
# specs/active/{{slug}}/tmp/pattern-extraction-plan.md

## Patterns to Extract

Review implementation for these pattern types:

### Architectural Patterns
- [ ] New class hierarchies
- [ ] Novel composition patterns
- [ ] Service/adapter structures

### Type Handling Patterns
- [ ] New type converters
- [ ] Type handler implementations
- [ ] Schema mapping approaches

### Configuration Patterns
- [ ] driver_features additions
- [ ] connection_config patterns
- [ ] extension_config patterns

### Testing Patterns
- [ ] Novel test fixtures
- [ ] Unique assertion patterns
- [ ] Performance testing approaches

### Error Handling Patterns
- [ ] New exception types
- [ ] Error recovery strategies
- [ ] Validation patterns

## Extraction Checklist

For each pattern identified:
- [ ] Document context (when/why to use)
- [ ] Extract code example
- [ ] Note similar patterns
- [ ] Add to pattern library
```

4. **Update tasks:**

```markdown
# specs/active/{{slug}}/tasks.md

## Review & Documentation Phase
- [x] Checkpoint 0: Intelligence bootstrap complete
  - Implementation reviewed
  - Test results reviewed
  - Pattern extraction plan created
```

**Output:**

```
✓ Checkpoint 0 complete - Review intelligence bootstrapped
Files modified: [list]
Patterns identified for extraction: [count]
Ready for documentation phase
```

---

## PHASE 1: DOCUMENTATION UPDATE

## Checkpoint 1: Update Adapter/Feature Documentation

**Actions:**

1. **Determine documentation scope:**

```bash
# If adapter feature:
DOCS_FILE="docs/guides/adapters/{adapter}.md"

# If core feature:
DOCS_FILE="docs/guides/core/{feature}.md"

# If extension:
DOCS_FILE="docs/guides/extensions/{extension}.md"
```

2. **Read existing documentation:**

```bash
cat $DOCS_FILE | head -200
```

3. **Update documentation following patterns:**

```markdown
# Add to appropriate section in docs file

## {Feature Name}

### Overview

{Feature description - what it does and why it exists}

### Configuration

{Configuration TypedDict and example}

Example:
\`\`\`python
from sqlspec.adapters.{adapter} import {Adapter}Config

config = {Adapter}Config(
    connection_config={"dsn": "..."},
    driver_features={
        "enable_{feature}": True,  # Auto-enabled when {condition}
    }
)
\`\`\`

### Usage

{Usage examples}

\`\`\`python
async with sql.provide_session(config) as session:
    result = await session.{feature_method}(...)
    # {expected result}
\`\`\`

### Type Handling

{If feature involves type conversion}

\`\`\`python
# Input type → Database type
{Python type} → {Database type}

# Output type
{Database type} → {Python type}
\`\`\`

### Best Practices

- {Practice 1}
- {Practice 2}
- {Practice 3}

### Limitations

- {Limitation 1}
- {Limitation 2}
```

4. **Update tasks:**

```markdown
## Review & Documentation Phase
- [x] Checkpoint 1: Feature documentation updated
  - File: {docs_file}
  - Sections added: [list]
```

**Output:**

```
✓ Checkpoint 1 complete - Feature documentation updated
Documentation file: {docs_file}
Sections updated: [list]
```

---

## Checkpoint 2: Update AGENTS.md (if new patterns)

**Actions:**

1. **Review pattern extraction plan:**

```bash
cat specs/active/{{slug}}/tmp/pattern-extraction-plan.md
```

2. **Update AGENTS.md with new patterns:**

```markdown
# Find appropriate section in AGENTS.md based on pattern type

# For driver_features patterns:
## driver_features Pattern

### New Feature: {Feature Name}

**Pattern:**
\`\`\`python
class AdapterDriverFeatures(TypedDict):
    enable_{feature}: NotRequired[bool]
    """{Feature description.

    Requirements: {dependencies/versions}
    Defaults to {condition}
    When enabled: {behavior}
    """
\`\`\`

**Implementation:**
\`\`\`python
# In config.py
def __init__(self, *, driver_features=None, **kwargs):
    processed_features = dict(driver_features) if driver_features else {}

    if "enable_{feature}" not in processed_features:
        processed_features["enable_{feature}"] = {DEFAULT_CONDITION}

    super().__init__(driver_features=processed_features, **kwargs)
\`\`\`

**When to use:**
- {Use case 1}
- {Use case 2}

---

# For testing patterns:
## Testing Strategy

### New Pattern: {Test Pattern Name}

**Context:** Used when testing {scenario}

**Implementation:**
\`\`\`python
{code example from tests}
\`\`\`

**Benefits:**
- {Benefit 1}
- {Benefit 2}

---

# For type handler patterns:
## Type Handler Pattern

### New Example: {Type Name} Support

**Pattern:**
\`\`\`python
# In _type_handlers.py
def converter_in(value: Any) -> Any:
    {conversion logic}

def converter_out(value: Any) -> Any:
    {conversion logic}

def register_handlers(connection):
    {registration logic}
\`\`\`

**Configuration:**
\`\`\`python
driver_features={"enable_{feature}": True}
\`\`\`
```

3. **Update tasks:**

```markdown
## Review & Documentation Phase
- [x] Checkpoint 2: AGENTS.md updated with new patterns
  - Patterns added: [list]
  - Sections: [list sections]
```

**Output:**

```
✓ Checkpoint 2 complete - AGENTS.md updated
New patterns documented: [count]
Sections updated: [list]
```

---

## PHASE 2: QUALITY GATE VALIDATION

## Checkpoint 3: Quality Gate Validation

**Actions:**

1. **Run linting checks:**

```bash
uv run ruff check sqlspec/
uv run ruff format --check sqlspec/
```

2. **Run type checking:**

```bash
uv run mypy sqlspec/
```

3. **Run full test suite:**

```bash
uv run pytest -n 2 --dist=loadgroup tests/
```

4. **Build documentation:**

```bash
make docs
```

5. **Verify quality gates:**

```markdown
# specs/active/{{slug}}/tmp/quality-gate-report.md

## Quality Gate Validation

### Linting
- [x] Ruff check: PASSED
- [x] Ruff format: PASSED
- Issues found: {0 or list}

### Type Checking
- [x] Mypy: PASSED
- Issues found: {0 or list}

### Testing
- [x] All tests passing: PASSED
- Test count: {X}
- Coverage: {X}%
- Regressions: None

### Documentation
- [x] Docs build: PASSED
- Warnings: {0 or list}

### Overall Status
✅ ALL QUALITY GATES PASSED

## Blockers

{None or list issues that must be fixed}
```

6. **Fix any issues found:**

```bash
# If ruff issues:
uv run ruff check --fix sqlspec/

# If mypy issues:
# Fix type hints in code

# If test failures:
# Fix failing tests

# If doc build issues:
# Fix documentation syntax
```

7. **Update tasks:**

```markdown
## Review & Documentation Phase
- [x] Checkpoint 3: Quality gates validated
  - Linting: ✓
  - Type checking: ✓
  - Tests: ✓
  - Docs build: ✓
  - Blockers: {0 or list}
```

**Output:**

```
✓ Checkpoint 3 complete - Quality gates passed
Linting: ✓ | Type checking: ✓ | Tests: ✓ | Docs: ✓
Blockers: None
```

---

## PHASE 3: KNOWLEDGE CAPTURE

## Checkpoint 4: Extract Patterns to Library

**Actions:**

1. **For each pattern identified in Checkpoint 0:**

```bash
# Create pattern document
cat > specs/guides/patterns/{pattern-name}.md << 'EOF'
# {Pattern Name}

## Overview

{What this pattern is and when it was created}

**Source Feature**: specs/active/{{slug}}/
**Created**: {date}
**Category**: [Architectural|Type Handling|Configuration|Testing|Error Handling]

## Problem

{What problem does this pattern solve}

## Solution

{How the pattern solves it}

## Implementation

\`\`\`python
{Full code example from implementation}
\`\`\`

## When to Use

- {Use case 1}
- {Use case 2}
- {Use case 3}

## When NOT to Use

- {Anti-pattern case 1}
- {Anti-pattern case 2}

## Related Patterns

- [{Related Pattern 1}](./related-pattern-1.md)
- [{Related Pattern 2}](./related-pattern-2.md)

## Examples

### Example 1: {Scenario}

\`\`\`python
{Real usage example from project}
\`\`\`

### Example 2: {Scenario}

\`\`\`python
{Real usage example from project}
\`\`\`

## Testing

\`\`\`python
{Test example showing pattern validation}
\`\`\`

## Performance Considerations

{Any performance notes}

## See Also

- [AGENTS.md Section](../../AGENTS.md#{section})
- [Documentation](../../docs/guides/{guide}.md)
EOF
```

2. **Update pattern library index:**

```bash
# Add to specs/guides/patterns/README.md

cat >> specs/guides/patterns/README.md << 'EOF'

### {Pattern Name}

**File**: [{pattern-name}.md](./{pattern-name}.md)
**Category**: {Category}
**Source**: specs/active/{{slug}}/
**Use Case**: {Brief description}

EOF
```

3. **Update tasks:**

```markdown
## Review & Documentation Phase
- [x] Checkpoint 4: Patterns extracted to library
  - Patterns extracted: [list with links]
  - Pattern library index updated
```

**Output:**

```
✓ Checkpoint 4 complete - Patterns captured in library
Patterns extracted: [count]
Files: [list pattern files]
Pattern library updated
```

---

## PHASE 4: RE-VALIDATION

## Checkpoint 5: Re-validate After Documentation Updates

**Actions:**

1. **Re-run tests:**

```bash
uv run pytest -n 2 --dist=loadgroup tests/
```

2. **Re-build documentation:**

```bash
make docs
```

3. **Verify no new issues:**

```markdown
# specs/active/{{slug}}/tmp/revalidation-report.md

## Re-validation After Documentation Updates

### Tests
- Status: {PASSED or issues}
- Regressions: {None or list}

### Documentation Build
- Status: {PASSED or issues}
- New warnings: {None or list}

### Overall Status
{✅ PASSED or ❌ ISSUES FOUND}

## Action Required

{None or list fixes needed}
```

4. **Fix any issues:**

```bash
# If new issues found, fix them before proceeding
```

5. **Update tasks:**

```markdown
## Review & Documentation Phase
- [x] Checkpoint 5: Re-validation complete
  - Tests: ✓
  - Docs build: ✓
  - Ready for archival
```

**Output:**

```
✓ Checkpoint 5 complete - Re-validation passed
Tests: ✓ | Docs: ✓
Ready for workspace cleanup and archival
```

---

## PHASE 5: WORKSPACE CLEANUP & ARCHIVE

## Checkpoint 6: Clean and Archive Workspace

**Actions:**

1. **Clean tmp/ directory:**

```bash
cd specs/active/{{slug}}

# Archive important tmp files to archive
mkdir -p archive/
mv tmp/pattern-extraction-plan.md archive/
mv tmp/quality-gate-report.md archive/
mv tmp/revalidation-report.md archive/
mv tmp/test-summary.md archive/

# Remove remaining tmp files
rm -rf tmp/

# Recreate empty tmp/ for workspace template
mkdir tmp/
```

2. **Create completion summary:**

```markdown
# specs/active/{{slug}}/COMPLETION-SUMMARY.md

# Feature Completion Summary: {Feature Name}

**Completed**: {date}
**Workspace**: specs/active/{{slug}}/

---

## Implementation Summary

### Files Modified
{list from git diff}

### New Features
- {Feature 1}
- {Feature 2}

### Configuration Options
- {Config option 1}
- {Config option 2}

---

## Testing Summary

### Unit Tests
- Files: {X}
- Tests: {X}
- Coverage: {X}%

### Integration Tests
- Adapters: [list]
- Tests: {X}
- Coverage: {X}%

### Performance Tests
- Benchmarks: {X}
- All within thresholds: ✓

---

## Documentation Summary

### Documentation Updated
- {File 1}
- {File 2}

### AGENTS.md Updates
- Patterns added: [list]
- Sections updated: [list]

### Pattern Library
- New patterns: [list with links]

---

## Quality Gates

- [x] Linting: PASSED
- [x] Type checking: PASSED
- [x] Tests: PASSED ({X}% coverage)
- [x] Docs build: PASSED
- [x] Re-validation: PASSED

---

## Knowledge Captured

### Patterns Extracted
1. [{Pattern 1}](../../guides/patterns/{pattern-1}.md)
2. [{Pattern 2}](../../guides/patterns/{pattern-2}.md)

### AGENTS.md Additions
- {Section 1}: {Pattern name}
- {Section 2}: {Pattern name}

---

## Next Steps for Future Maintainers

{Any notes about future enhancements or considerations}

---

## Archive Location

This workspace will be moved to: `specs/archive/{{slug}}/`
```

3. **Move to archive:**

```bash
# Move entire workspace to archive
mv specs/active/{{slug}} specs/archive/{{slug}}

# Verify
ls -la specs/archive/{{slug}}/
```

4. **Update tasks (in archive):**

```markdown
# specs/archive/{{slug}}/tasks.md

## Review & Documentation Phase
- [x] Checkpoint 6: Workspace archived
  - Completion summary created
  - tmp/ cleaned
  - Moved to: specs/archive/{{slug}}/

---

## ✅ FEATURE COMPLETE

All phases finished. Workspace archived.
```

5. **Final output:**

```markdown
# specs/archive/{{slug}}/recovery.md

## Status: COMPLETED ✅

Feature implementation, testing, documentation, and archival complete.

**Completed**: {date}
**Archive Location**: specs/archive/{{slug}}/

## Summary

- Implementation: ✓
- Testing: ✓
- Documentation: ✓
- Quality gates: ✓
- Pattern extraction: ✓
- Re-validation: ✓
- Archival: ✓

See COMPLETION-SUMMARY.md for full details.
```

**Output:**

```
✅ Checkpoint 6 complete - Workspace archived

Feature: {Feature Name}
Status: COMPLETE ✅

Summary:
- Implementation: ✓
- Tests: {X} tests, {X}% coverage ✓
- Documentation: Updated ✓
- Patterns extracted: {X} ✓
- Quality gates: All passed ✓
- Archive: specs/archive/{{slug}}/ ✓

COMPLETION-SUMMARY.md created with full details.
```

---

## FINAL DELIVERABLES

1. ✅ **Documentation Updated:**
   - Feature documentation in docs/guides/
   - AGENTS.md updated with new patterns
   - Pattern library updated

2. ✅ **Quality Validated:**
   - All linting checks passed
   - All type checks passed
   - All tests passing
   - Documentation builds successfully
   - Re-validation passed

3. ✅ **Knowledge Captured:**
   - Patterns extracted to specs/guides/patterns/
   - Pattern library index updated
   - AGENTS.md enhanced with learnings

4. ✅ **Workspace Archived:**
   - tmp/ cleaned (important files saved to archive/)
   - COMPLETION-SUMMARY.md created
   - Workspace moved to specs/archive/{{slug}}/
   - Recovery.md marked COMPLETED

---

## SESSION COMPLETE

Feature {Feature Name} is fully implemented, tested, documented, and archived.

**Next Feature:**
Run `/prd` to start planning the next feature.
"""

```

**Save:**
```bash
cat > .gemini/commands/review.toml << 'TOML_CONTENT'
[paste above content]
TOML_CONTENT

# Verify
ls -la .gemini/commands/review.toml
wc -l .gemini/commands/review.toml  # Should be ~550+ lines
```

---

## PHASE 5: KNOWLEDGE BASE INITIALIZATION

### Step 5.1: Extract Initial Patterns

```bash
mkdir -p specs/guides/patterns

# Extract adapter pattern
cat > specs/guides/patterns/adapter-pattern.md << 'EOF'
# Adapter Pattern

## Overview

{Auto-generated from existing adapters}

## Structure

{Common class hierarchy}

## Example

{Real code from project}

## When to Use

{Derived from existing usage}
EOF

# Repeat for other patterns: service, config, error-handling, async
```

### Step 5.2: Create Pattern Index

```bash
cat > specs/guides/patterns/README.md << 'EOF'
# Project Patterns

Auto-extracted patterns from codebase for agent guidance.

## Architectural Patterns

- [Adapter Pattern](./adapter-pattern.md)
- [Service Pattern](./service-pattern.md)
- [Configuration Pattern](./config-pattern.md)

## Code Patterns

- [Error Handling](./error-handling.md)
- [Async/Await](./async-pattern.md)
- [Type Annotations](./type-hints.md)

## Testing Patterns

- [Unit Test Structure](./unit-tests.md)
- [Integration Test Setup](./integration-tests.md)
- [Test Fixtures](./fixtures.md)

---

*This index is maintained automatically by agents during feature development.*
EOF
```

---

## PHASE 6: VERIFICATION & CONTINUOUS IMPROVEMENT

```bash
# Verify intelligent structure
tree -L 3 .gemini/ specs/

# Verify MCP strategy generated
cat .gemini/mcp-strategy.md

# Verify pattern extraction
ls -la specs/guides/patterns/

# Test workflow
cat specs/guides/workflows/intelligent-development.yaml
```

**Success Criteria:**

```text
✅ Intelligent Bootstrap Complete

Intelligence Layer:
- ✓ MCP tool detection with fallback strategies
- ✓ Pattern extraction from existing codebase
- ✓ Adaptive quality gates
- ✓ Context-aware workflows
- ✓ Knowledge base initialized

Adaptive Features:
- ✓ Checkpoint counts scale with complexity
- ✓ Tool selection based on availability
- ✓ Quality gates adapt to project norms
- ✓ Workflows adjust to feature scope

Knowledge Capture:
- ✓ Pattern extraction automated
- ✓ Example code documented
- ✓ Guides continuously updated
- ✓ Agent memory persists across sessions

Next Steps:
  1. Run `/prd "feature description"` - System adapts to complexity
  2. Agents learn from existing code before creating new patterns
  3. Knowledge base grows with each completed feature
  4. Future agents inherit all learnings automatically
```

---

## CONTINUOUS INTELLIGENCE

### Auto-Improvement Loop

After each feature completion:

1. **Pattern Extraction**
   - Identify new patterns introduced
   - Compare with existing patterns
   - Update pattern guides

2. **Example Creation**
   - Extract working code as examples
   - Add to specs/guides/examples/
   - Link from pattern documentation

3. **Guide Updates**
   - Update AGENTS.md with new learnings
   - Enhance quality gates based on issues found
   - Refine workflow based on actual usage

4. **Agent Evolution**
   - Agents become smarter over time
   - Fewer errors in subsequent features
   - Better alignment with project style

---

**Version**: 6.0 (Intelligent Edition)
**Philosophy**: Agents should learn, not just execute
**Compatibility**: Any language, any framework
**Maintenance**: Self-improving through continuous learning
