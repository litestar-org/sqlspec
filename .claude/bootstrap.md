# Claude Agent System Bootstrap

**Version**: 1.0
**Purpose**: Autonomous setup of complete Claude multi-agent system for any project

This is a **single-file, self-contained bootstrap** that will:

1. Analyze your project structure, languages, frameworks, and tools
2. Create all necessary folders and agent configuration files
3. Generate project-specific AGENTS.md and guides
4. Set up agent definitions tailored to your project
5. Configure .gitignore appropriately
6. Create workspace templates for feature development

**Usage**: Run this with Claude Code in your project root.

---

## PHASE 0: MCP TOOL DISCOVERY

### Step 0.1: Detect Available MCP Servers

**Objective**: Discover which MCP tools are available for research and planning.

```python
# Check for MCP tool availability
available_tools = {}

# Try crash
try:
    available_tools['crash'] = True
except:
    available_tools['crash'] = False

# Try sequential-thinking
try:
    available_tools['sequential_thinking'] = True
except:
    available_tools['sequential_thinking'] = False

# Try zen MCP tools
try:
    available_tools['zen_planner'] = True
    available_tools['zen_consensus'] = True
    available_tools['zen_thinkdeep'] = True
    available_tools['zen_analyze'] = True
    available_tools['zen_debug'] = True
except:
    available_tools['zen_planner'] = False
    available_tools['zen_consensus'] = False
    available_tools['zen_thinkdeep'] = False
    available_tools['zen_analyze'] = False
    available_tools['zen_debug'] = False

# Try context7
try:
    available_tools['context7'] = True
except:
    available_tools['context7'] = False

# Try web search
try:
    available_tools['web_search'] = True
except:
    available_tools['web_search'] = False

print("\n=== MCP TOOL AVAILABILITY ===\n")
print(f"Crash: {'✓' if available_tools.get('crash') else '✗'}")
print(f"Sequential Thinking: {'✓' if available_tools['sequential_thinking'] else '✗'}")
print(f"Zen Planner: {'✓' if available_tools['zen_planner'] else '✗'}")
print(f"Zen Consensus: {'✓' if available_tools['zen_consensus'] else '✗'}")
print(f"Zen ThinkDeep: {'✓' if available_tools['zen_thinkdeep'] else '✗'}")
print(f"Zen Analyze: {'✓' if available_tools['zen_analyze'] else '✗'}")
print(f"Zen Debug: {'✓' if available_tools['zen_debug'] else '✗'}")
print(f"Context7: {'✓' if available_tools['context7'] else '✗'}")
print(f"WebSearch: {'✓' if available_tools['web_search'] else '✗'}")
```

---

## PHASE 1: PROJECT ANALYSIS & DISCOVERY

### Step 1.1: Discover Project Structure

```python
# Discover project root files
Bash("ls -la")

# Find all source code files by language
Glob(pattern="**/*.py")   # Python
Glob(pattern="**/*.js")   # JavaScript
Glob(pattern="**/*.ts")   # TypeScript
Glob(pattern="**/*.go")   # Go
Glob(pattern="**/*.rs")   # Rust

# Find configuration files
Read("pyproject.toml")    # Python
Read("package.json")      # Node.js
Read("Cargo.toml")        # Rust
Read("go.mod")            # Go
Read("Makefile")          # Make-based projects

# Find existing documentation
Glob(pattern="**/*.md")
Glob(pattern="docs/**/*")

# Find test directories
Glob(pattern="tests/**/*")
Glob(pattern="test/**/*")
```

### Step 1.2: Identify Primary Language & Framework

**Python Detection:**

- Has `pyproject.toml` OR `setup.py`
- Look for frameworks: Django, Flask, FastAPI, Litestar
- Look for test framework: pytest, unittest
- Look for tools: ruff, mypy, black

**Node.js Detection:**

- Has `package.json`
- Look for frameworks: React, Express, Next.js
- Look for test framework: Jest, Mocha
- Look for tools: ESLint, Prettier

**Go Detection:**

- Has `go.mod`
- Look for frameworks: Gin, Echo
- Uses `go test`

**Rust Detection:**

- Has `Cargo.toml`
- Look for frameworks: Actix, Rocket
- Uses `cargo test`

### Step 1.3: Detect Build & Development Tools

```python
# Check for build systems
has_make = exists("Makefile")
has_npm = exists("package.json")
has_cargo = exists("Cargo.toml")
has_poetry = exists("poetry.lock")
has_uv = exists("uv.lock")

# Read and extract commands
if has_make:
    makefile = Read("Makefile")
    # Extract targets: test, lint, build, docs, install
```

### Step 1.4: Detect Architecture & Patterns

```python
# Python architecture detection
Grep(pattern=r'class.*Service', path='src/')
Grep(pattern=r'class.*Repository', path='src/')
Grep(pattern=r'async def', path='src/')
Grep(pattern=r'await ', path='src/')

# Check for type hints
Grep(pattern=r'from __future__ import annotations', output_mode='count')
Grep(pattern=r'->.*:', output_mode='count')
Grep(pattern=r'\| None', output_mode='count')
```

### Step 1.5: Detect Domain-Specific Patterns (CRITICAL)

**Objective**: Discover if project has multi-variant patterns that need strategy matrices.

**Multi-Adapter/Multi-Driver Pattern Detection**:

```python
# Look for adapter or driver patterns
adapters_found = Grep(pattern=r'class.*Adapter', path='.', output_mode='files_with_matches')
drivers_found = Grep(pattern=r'class.*Driver', path='.', output_mode='files_with_matches')

has_adapter_pattern = len(adapters_found) > 2

# Common adapter directory patterns
has_adapters_dir = exists("*/adapters/") or exists("*/drivers/") or exists("*/backends/")

# Detect what types of adapters
adapter_types = []
if Grep(pattern=r'database|db|sql', path='*/adapters/', output_mode='count') > 0:
    adapter_types.append("database")
if Grep(pattern=r'api|client|http', path='*/adapters/', output_mode='count') > 0:
    adapter_types.append("api_client")
if Grep(pattern=r'storage|s3|blob', path='*/adapters/', output_mode='count') > 0:
    adapter_types.append("storage")
if Grep(pattern=r'cache|redis|memcache', path='*/adapters/', output_mode='count') > 0:
    adapter_types.append("cache")

print(f"\n=== DOMAIN PATTERN DETECTION ===\n")
print(f"Multi-Adapter Pattern: {has_adapter_pattern}")
if has_adapter_pattern:
    print(f"Adapter Types: {', '.join(adapter_types)}")
```

**Multi-Service Pattern Detection**:

```python
# Look for service layer patterns
services_found = Grep(pattern=r'class.*Service', path='.', output_mode='count')
has_service_layer = services_found > 3

# Look for microservices indicators
has_microservices = (
    exists("services/*/") or
    exists("apps/*/") or
    (Grep(pattern=r'grpc|protobuf', path='.', output_mode='count') > 0)
)

print(f"Service Layer Pattern: {has_service_layer}")
print(f"Microservices Pattern: {has_microservices}")
```

**Repository Pattern Detection**:

```python
repositories_found = Grep(pattern=r'class.*Repository', path='.', output_mode='count')
has_repository_pattern = repositories_found > 2

print(f"Repository Pattern: {has_repository_pattern}")
```

**API/Endpoint Pattern Detection**:

```python
has_rest_api = (
    Grep(pattern=r'@app.route|@router\.|@api_route', path='.', output_mode='count') > 5
)

has_graphql = Grep(pattern=r'graphql|@strawberry|@ariadne', path='.', output_mode='count') > 0

print(f"REST API Pattern: {has_rest_api}")
print(f"GraphQL Pattern: {has_graphql}")
```

**Store Detected Patterns**:

```python
domain_patterns = {
    'multi_adapter': has_adapter_pattern,
    'adapter_types': adapter_types,
    'service_layer': has_service_layer,
    'microservices': has_microservices,
    'repository': has_repository_pattern,
    'rest_api': has_rest_api,
    'graphql': has_graphql,
}

print(f"\nDetected patterns will influence PRD template structure.")
```

**Language-Specific Framework Detection (Deep Dive)**:

```python
# Python-specific patterns
has_django = exists("manage.py") or Grep(pattern=r'from django', path='.', output_mode='count') > 0
has_django_orm = Grep(pattern=r'from django.db import models|models\.Model', path='.', output_mode='count') > 0

has_sqlalchemy = Grep(pattern=r'from sqlalchemy|import sqlalchemy', path='.', output_mode='count') > 0
has_sqlalchemy_orm = Grep(pattern=r'declarative_base|Base = declarative_base', path='.', output_mode='count') > 0

has_pydantic = Grep(pattern=r'from pydantic import BaseModel', path='.', output_mode='count') > 0
has_msgspec = Grep(pattern=r'import msgspec|msgspec\.Struct', path='.', output_mode='count') > 0

print(f"\n=== PYTHON FRAMEWORK DETECTION ===\n")
print(f"Django: {has_django}")
print(f"Django ORM: {has_django_orm}")
print(f"SQLAlchemy: {has_sqlalchemy}")
print(f"SQLAlchemy ORM: {has_sqlalchemy_orm}")
print(f"Pydantic Models: {has_pydantic}")
print(f"msgspec Structs: {has_msgspec}")

# Go-specific patterns
has_grpc = Grep(pattern=r'google\.golang\.org/grpc|import.*grpc', path='.', output_mode='count') > 0
has_protobuf = Grep(pattern=r'google\.golang\.org/protobuf|\.proto', path='.', output_mode='count') > 0

if has_grpc or has_protobuf:
    print(f"\n=== GO FRAMEWORK DETECTION ===\n")
    print(f"gRPC: {has_grpc}")
    print(f"Protocol Buffers: {has_protobuf}")

# Rust-specific patterns
has_tokio = Grep(pattern=r'tokio::|use tokio', path='.', output_mode='count') > 0
has_async_std = Grep(pattern=r'async_std::|use async_std', path='.', output_mode='count') > 0
has_serde = Grep(pattern=r'serde::|use serde', path='.', output_mode='count') > 0

if has_tokio or has_async_std or has_serde:
    print(f"\n=== RUST FRAMEWORK DETECTION ===\n")
    print(f"Tokio: {has_tokio}")
    print(f"async-std: {has_async_std}")
    print(f"Serde: {has_serde}")

# Store language-specific patterns
language_frameworks = {
    'django': has_django,
    'django_orm': has_django_orm,
    'sqlalchemy': has_sqlalchemy,
    'sqlalchemy_orm': has_sqlalchemy_orm,
    'pydantic': has_pydantic,
    'msgspec': has_msgspec,
    'grpc': has_grpc,
    'protobuf': has_protobuf,
    'tokio': has_tokio,
    'async_std': has_async_std,
    'serde': has_serde,
}
```

**CI/CD Integration Detection**:

```python
# Detect CI/CD systems
has_github_actions = exists(".github/workflows/")
has_gitlab_ci = exists(".gitlab-ci.yml")
has_circleci = exists(".circleci/config.yml")
has_jenkins = exists("Jenkinsfile")
has_travis = exists(".travis.yml")

print(f"\n=== CI/CD DETECTION ===\n")
print(f"GitHub Actions: {'✓' if has_github_actions else '✗'}")
print(f"GitLab CI: {'✓' if has_gitlab_ci else '✗'}")
print(f"CircleCI: {'✓' if has_circleci else '✗'}")
print(f"Jenkins: {'✓' if has_jenkins else '✗'}")
print(f"Travis CI: {'✓' if has_travis else '✗'}")

# Store CI/CD patterns
ci_patterns = {
    'github_actions': has_github_actions,
    'gitlab_ci': has_gitlab_ci,
    'circleci': has_circleci,
    'jenkins': has_jenkins,
    'travis': has_travis,
}
```

**Testing Framework Deep Detection**:

```python
# Python testing frameworks
has_pytest = exists("pytest.ini") or exists("pyproject.toml")
pytest_plugins = []

if has_pytest:
    # Detect pytest plugins
    pytest_asyncio = Grep(pattern=r'pytest-asyncio|pytest_asyncio', path='.', output_mode='count') > 0
    pytest_cov = Grep(pattern=r'pytest-cov|pytest_cov', path='.', output_mode='count') > 0
    pytest_xdist = Grep(pattern=r'pytest-xdist|pytest_xdist', path='.', output_mode='count') > 0
    pytest_mock = Grep(pattern=r'pytest-mock|pytest_mock', path='.', output_mode='count') > 0

    if pytest_asyncio:
        pytest_plugins.append("pytest-asyncio")
    if pytest_cov:
        pytest_plugins.append("pytest-cov")
    if pytest_xdist:
        pytest_plugins.append("pytest-xdist")
    if pytest_mock:
        pytest_plugins.append("pytest-mock")

# Fixture patterns
has_conftest = exists("tests/conftest.py") or exists("conftest.py")
fixture_count = Grep(pattern=r'@pytest\.fixture', path='tests/', output_mode='count') if exists("tests/") else 0

# Test organization
test_structure = "function-based"
class_based_tests = Grep(pattern=r'class Test', path='tests/', output_mode='count') if exists("tests/") else 0
if class_based_tests > 5:
    test_structure = "class-based"

print(f"\n=== TESTING FRAMEWORK DETECTION ===\n")
print(f"pytest: {'✓' if has_pytest else '✗'}")
if pytest_plugins:
    print(f"pytest plugins: {', '.join(pytest_plugins)}")
print(f"Fixtures (conftest.py): {'✓' if has_conftest else '✗'}")
print(f"Fixture count: {fixture_count}")
print(f"Test structure: {test_structure}")

# Go testing
if exists("**/*_test.go"):
    has_testify = Grep(pattern=r'github\.com/stretchr/testify', path='.', output_mode='count') > 0
    print(f"\n=== GO TESTING ===\n")
    print(f"Testify: {'✓' if has_testify else '✗'}")

# Rust testing
if exists("tests/"):
    has_proptest = Grep(pattern=r'use proptest', path='.', output_mode='count') > 0
    print(f"\n=== RUST TESTING ===\n")
    print(f"Proptest: {'✓' if has_proptest else '✗'}")

testing_framework = {
    'pytest': has_pytest,
    'pytest_plugins': pytest_plugins,
    'has_conftest': has_conftest,
    'fixture_count': fixture_count,
    'test_structure': test_structure,
}
```

### Step 1.6: Detect Documentation System

```python
# Sphinx (Python)
has_sphinx = exists("docs/conf.py")

# MkDocs
has_mkdocs = exists("mkdocs.yml")

# Docusaurus (Node.js)
has_docusaurus = exists("docusaurus.config.js")

if has_sphinx:
    Read("docs/conf.py")
    Glob(pattern="docs/**/*.rst")
```

---

## PHASE 2: FOLDER STRUCTURE CREATION

### Step 2.1: Check for Existing Configuration

```python
has_claude_dir = exists(".claude/")
has_claude_agents = exists(".claude/agents/")
has_agents_md = exists("AGENTS.md")

has_specs_dir = exists("specs/")
has_specs_guides = exists("specs/guides/")
```

### Step 2.2: Create Directory Structure

```bash
mkdir -p .claude/agents
mkdir -p specs/active
mkdir -p specs/archive
mkdir -p specs/template-spec/research
mkdir -p specs/template-spec/tmp
mkdir -p specs/guides

touch specs/active/.gitkeep
touch specs/archive/.gitkeep
```

### Step 2.3: Update .gitignore

```python
gitignore_exists = exists(".gitignore")
current_gitignore = Read(".gitignore") if gitignore_exists else ""

claude_ignores = [
    "",
    "# Claude Agent System",
    "specs/active/",
    "specs/archive/",
    "!specs/active/.gitkeep",
    "!specs/archive/.gitkeep",
]

# Check which entries are missing and append
# (Implementation similar to Gemini bootstrap)
```

---

## PHASE 3: AGENTS.MD CREATION

### Step 3.1: Generate AGENTS.md Content

```python
from datetime import datetime

agents_md_content = f'''# Claude Agent System: {project_name}

**Version**: 1.0
**Last Updated**: {datetime.now().strftime("%A, %B %d, %Y")}

This document is the **single source of truth** for the Claude Code multi-agent workflow in this project.

## Philosophy

This system is built on **"Continuous Knowledge Capture"** - ensuring documentation evolves with code.

## Agent Architecture

Claude uses a **multi-agent system** where specialized agents handle specific phases:

| Agent | File | Mission |
|-------|------|---------|
| **PRD** | `.claude/agents/prd.md` | Requirements analysis, PRD creation, task breakdown |
| **Expert** | `.claude/agents/expert.md` | Implementation with deep technical knowledge |
| **Testing** | `.claude/agents/testing.md` | Comprehensive test creation (90%+ coverage) |
| **Docs & Vision** | `.claude/agents/docs-vision.md` | Documentation, quality gate, knowledge capture |
| **Sync Guides** | `.claude/agents/sync-guides.md` | Documentation synchronization |

## Workflow

### Sequential Development Phases

1. **Phase 1: PRD** - Agent creates workspace in `specs/active/{{slug}}/`
2. **Phase 2: Expert Research** - Research patterns, libraries, best practices
3. **Phase 3: Implementation** - Expert writes production code
4. **Phase 4: Testing** - Testing agent creates comprehensive tests (auto-invoked by Expert)
5. **Phase 5: Documentation** - Docs & Vision updates guides (auto-invoked after Testing)
6. **Phase 6: Quality Gate** - Full validation and knowledge capture
7. **Phase 7: Archive** - Workspace moved to `specs/archive/`

## Workspace Structure

```sh

specs/active/{{slug}}/
├── prd.md          # Product Requirements Document
├── tasks.md        # Implementation checklist
├── recovery.md     # Session resume instructions
├── research/       # Research findings
└── tmp/            # Temporary files

```

## Project Context

**Language**: {primary_language}
**Framework**: {detected_framework}
**Architecture**: {detected_architecture_pattern}
**Test Framework**: {test_framework}
**Build Tool**: {build_tool}
**Documentation**: {docs_system}

## Development Commands

### Build

```bash
{build_commands}
```

### Test

```bash
{test_commands}
```

### Lint

```bash
{linting_commands}
```

### Documentation

```bash
{docs_commands}
```

## Code Quality Standards

### Type Hints

- Style: {type_hint_style}
- All functions must have type hints
- Use `from __future__ import annotations` for modern syntax

### Formatting

- Tool: {formatter_tool}
- Style: {code_style}
- Auto-format before committing

### Documentation

- Docstring style: {docstring_style}
- All public APIs must be documented
- Include usage examples

### Testing

- Framework: {test_framework}
- Coverage target: **90%+ for all modified modules**
- Test types required:
  1. Unit tests (isolated components)
  2. Integration tests (real dependencies)
  3. Edge cases (NULL, empty, errors)
  4. Performance tests (N+1 query detection for database ops)
  5. Concurrent access tests (race conditions, deadlocks)

### Async Patterns

{async_patterns}

## Project Structure

```
{project_structure_tree}
```

## Key Architectural Patterns

{detected_patterns}

## MCP Tools Available

### Context7

- **Purpose**: Up-to-date library documentation
- **Usage**: Research external libraries during planning/implementation
- **Commands**:

  ```python
  mcp__context7__resolve-library-id(libraryName="fastapi")
  mcp__context7__get-library-docs(
      context7CompatibleLibraryID="/tiangolo/fastapi",
      topic="dependency injection",
      tokens=5000
  )
  ```

### Zen MCP

- **zen.planner**: Multi-step planning with revision capabilities
- **zen.chat**: Collaborative thinking and brainstorming
- **zen.thinkdeep**: Deep architectural analysis
- **zen.analyze**: Code quality and performance analysis
- **zen.debug**: Systematic debugging workflow
- **zen.consensus**: Multi-model consensus for decisions

### WebSearch

- **Purpose**: Modern best practices and industry standards
- **Usage**: Research novel problems and architectural decisions

## Anti-Patterns (Avoid These)

{detected_anti_patterns}

## Dependencies

### Core

{core_dependencies}

### Development

{dev_dependencies}

## Knowledge Capture Protocol

After every significant feature:

1. Update `specs/guides/` with new patterns
2. Ensure all public APIs documented
3. Add working code examples
4. Update this AGENTS.md if workflow improves

## Version Control

{version_control_guidelines}

---

**To start a new feature:**

1. Use PRD agent to create requirements
2. Expert agent implements and orchestrates testing
3. Docs & Vision agent handles quality gate and archival

**To sync documentation:**

Use Sync Guides agent to ensure specs/guides/ matches codebase.
'''

Write(file_path="AGENTS.md", content=agents_md_content)
print("✓ Created AGENTS.md")

```

---

## PHASE 4: AGENT FILES CREATION

### Step 4.1: Create PRD Agent

```python
prd_agent = '''---
name: prd
description: {project_name} PRD specialist - requirement analysis, PRD creation, task breakdown
tools: mcp__context7__resolve-library-id, mcp__context7__get-library-docs, WebSearch, mcp__zen__planner, mcp__zen__chat, Read, Write, Glob, Grep, Task
model: sonnet
---

# PRD Agent

Strategic planning specialist for {project_name}. Creates comprehensive PRDs, task breakdowns, and requirement structures.

## Core Responsibilities

1. **Requirement Analysis** - Understand user needs
2. **PRD Creation** - Write detailed requirements
3. **Task Breakdown** - Create actionable tasks
4. **Research Coordination** - Identify what Expert needs
5. **Workspace Setup** - Create specs/active/{{slug}}/

## Project Context

**Language**: {primary_language}
**Framework**: {detected_framework}
**Test Framework**: {test_framework}
**Build Tool**: {build_tool}

**Available MCP Tools**:
{mcp_tools_summary}

**Detected Domain Patterns**:
{domain_patterns_summary}

## Planning Workflow (Adaptive based on available tools)

### Step 1: Understand Requirement

**Gather Context:**
```python
Read("AGENTS.md")
Read("specs/guides/architecture.md")
Grep(pattern="class.*Service", path="{source_dir}")
```

### Step 2: Deep Analysis (MANDATORY - Use Best Available Tool)

**TIER 1 (Preferred): Crash**

```python
# Use crash for structured, revisable deep analysis
mcp__crash__crash(
    step_number=1,
    estimated_total=12,
    purpose="analysis",
    thought="Step 1: Analyze feature scope and affected components",
    next_action="Map dependencies",
    outcome="pending",
    rationale="Crash enables branching and revisions",
    context="Initial planning"
)
# Continue through comprehensive crash steps (≥12 for non-trivial work)
```

**TIER 2 (If Crash Unavailable but Sequential Thinking Installed): Sequential Thinking**

```python
# Use sequential thinking for deep analysis
# Minimum 12-15 thoughts for any non-trivial feature
mcp__sequential-thinking__sequentialthinking(
    thought="Step 1: Analyze feature scope and affected components",
    thought_number=1,
    total_thoughts=15,
    next_thought_needed=True
)
# Continue through comprehensive analysis...
```

**TIER 3 (If Neither Crash nor Sequential Thinking Available): Zen Planner**

```python
# Use zen.planner for structured breakdown
mcp__zen__planner(
    step="Analyze feature scope: Identify affected modules, dependencies, and integration points",
    step_number=1,
    total_steps=8,
    next_step_required=True
)
```

**TIER 4 (If No MCP Tools): Internal Planning**

- Manually break down into phases
- Document analysis in research/plan.md
- Be extra thorough - you don't have AI assistance

### Step 3: Research Best Practices (Adaptive)

**Priority Order:**

1. **Internal Guides (Always First)**: Read `specs/guides/` first
2. **Project Documentation**: Read `docs/` or `README.md`
3. **Context7 (If Available)**: External library docs (5000+ tokens)
4. **WebSearch (If Available)**: Best practices (e.g. "{framework} {feature-type} best practices 2025")
5. **Manual Research (Fallback)**: Read library documentation directly

**Context7 Usage (If Available):**

```python
mcp__context7__resolve-library-id(libraryName="sqlalchemy")
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/sqlalchemy/sqlalchemy",
    topic="async session management",
    tokens=5000
)
```

**WebSearch Usage (If Available):**

```python
WebSearch(query="async database connection pooling best practices 2025")
```

### Step 4: Get Consensus on Architecture (Complex Features)

**If zen.consensus Available (PREFERRED)**:

```python
mcp__zen__consensus(
    step="Evaluate architectural approaches for {{feature}}",
    models=[
        {{"model": "gemini-2.5-pro", "stance": "for"}},
        {{"model": "openai/gpt-5-pro", "stance": "against"}},
        {{"model": "openai/gpt-5", "stance": "neutral"}}
    ],
    relevant_files=["path/to/relevant/file.py"]
)
```

**If Not Available**:

- Document trade-offs manually
- Research architectural patterns thoroughly
- Get human review for major decisions

### Step 5: Create Workspace

```python
slug = feature_name.lower().replace(" ", "-")
Write(file_path=f"specs/active/{{slug}}/prd.md", content=...)
Write(file_path=f"specs/active/{{slug}}/tasks.md", content=...)
Write(file_path=f"specs/active/{{slug}}/recovery.md", content=...)
Write(file_path=f"specs/active/{{slug}}/research/plan.md", content=...)
```

### Step 6: Adapt PRD Template Based on Domain Patterns

**If Multi-Adapter Pattern Detected**:

- Add "Per-Adapter Strategy" section with matrix
- Include code examples per adapter
- Specify performance targets per adapter type

**If Service Layer Detected**:

- Add "Service Integration Strategy" section
- Document inter-service dependencies

**If REST API Detected**:

- Add "Endpoint Strategy" section
- Document API design decisions

**If GraphQL Detected**:

- Add "Schema Changes" section
- Document resolver strategy

### Step 7: Write Comprehensive PRD

Use adaptive template from `specs/template-spec/prd.md` (see Quality Gate section for completeness checklist)

### Step 8: Task List

Break down into phases:

- Phase 1: Planning & Research ✓
- Phase 2: Expert Research
- Phase 3: Core Implementation
- Phase 4: Integration
- Phase 5: Testing (auto via Expert)
- Phase 6: Documentation (auto via Expert)
- Phase 7: Quality Gate & Archive

## Success Criteria

✅ PRD is comprehensive
✅ Tasks are actionable
✅ Recovery guide complete
✅ Research questions clear
✅ Follows {project_name} patterns
'''

Write(file_path=".claude/agents/prd.md", content=prd_agent.format(
    project_name=project_name,
    primary_language=primary_language,
    detected_framework=detected_framework,
    test_framework=test_framework,
    build_tool=build_tool,
    source_dir=source_directory
))
print("✓ Created .claude/agents/prd.md")

```

### Step 4.2: Create Expert Agent

```python
expert_agent = '''---
name: expert
description: {project_name} implementation expert with deep knowledge of {framework} and {language}
tools: mcp__context7__resolve-library-id, mcp__context7__get-library-docs, WebSearch, mcp__zen__analyze, mcp__zen__thinkdeep, mcp__zen__debug, mcp__zen__chat, Read, Edit, Write, Bash, Glob, Grep, Task
model: sonnet
---

# Expert Agent

Implementation specialist for {project_name}. Deep expertise in {framework}, {language}, and project architecture.

## Core Responsibilities

1. **Implementation** - Write production-quality code
2. **Research** - Use Context7, WebSearch for libraries/patterns
3. **Architecture** - Use zen.thinkdeep for complex decisions
4. **Debugging** - Use zen.debug for systematic troubleshooting
5. **Orchestration** - Auto-invoke Testing and Docs & Vision agents
6. **Quality** - Ensure all code meets AGENTS.md standards

## Workflow

### Step 1: Understand Plan

```python
Read("specs/active/{{slug}}/prd.md")
Read("specs/active/{{slug}}/tasks.md")
Read("specs/active/{{slug}}/recovery.md")
```

### Step 2: Research

**Read project guides:**

```python
Read("AGENTS.md")
Read("specs/guides/architecture.md")
Read("specs/guides/testing.md")
```

**Find similar patterns:**

```python
Glob(pattern="{source_dir}/**/*service*.py")
Grep(pattern="class.*Service", path="{source_dir}")
```

**Research external libraries:**

```python
mcp__context7__resolve-library-id(libraryName="httpx")
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/encode/httpx",
    topic="async client usage",
    tokens=5000
)
```

### Step 3: Implement

Write production code following AGENTS.md:

- {type_hint_requirements}
- {docstring_requirements}
- {architecture_requirements}
- Pass `{linting_command}` with zero errors

### Step 4: Use Advanced Tools

**For debugging:**

```python
mcp__zen__debug(
    step="Investigating async transaction issue",
    step_number=1,
    total_steps=3,
    next_step_required=True,
    findings="Context manager scope unclear",
    hypothesis="Missing await in transaction block",
    confidence="medium",
    relevant_files=["/path/to/file.py"]
)
```

**For architectural decisions:**

```python
mcp__zen__thinkdeep(
    step="Evaluating caching strategy",
    step_number=1,
    total_steps=4,
    next_step_required=True,
    findings="Current implementation makes 100 DB calls per request",
    hypothesis="Redis cache with 5min TTL would reduce load",
    confidence="high",
    focus_areas=["performance", "scalability"]
)
```

**For code analysis:**

```python
mcp__zen__analyze(
    step="Analyzing service layer for N+1 queries",
    step_number=1,
    total_steps=3,
    next_step_required=True,
    findings="Multiple services load relationships without joinedload",
    analysis_type="performance",
    relevant_files=["/path/to/service.py"]
)
```

### Step 5: Local Testing

```bash
{test_command}
{linting_command}
```

### Step 6: Update Progress

```python
Edit(file_path="specs/active/{{slug}}/tasks.md", ...)
Edit(file_path="specs/active/{{slug}}/recovery.md", ...)
```

### Step 7: Auto-Invoke Sub-Agents (MANDATORY)

**After implementation complete:**

```python
Task(
    description="Run comprehensive testing phase",
    prompt='''Execute testing agent workflow for specs/active/{{slug}}.

    Context:
    - Implementation complete for all acceptance criteria
    - Modified files: [list]
    - Local tests passed

    Requirements:
    - Achieve 90%+ test coverage
    - Test all acceptance criteria
    - Include N+1 query detection
    - Test concurrent access
    ''',
    subagent_type="testing",
    model="sonnet"
)
```

**After testing complete:**

```python
Task(
    description="Run docs, quality gate, and archival",
    prompt='''Execute Docs & Vision 5-phase workflow for specs/active/{{slug}}.

    Context:
    - Implementation complete
    - All tests passing with 90%+ coverage

    Requirements:
    - Update documentation
    - Run full quality gate
    - Scan for anti-patterns
    - Capture knowledge in AGENTS.md
    - Archive workspace
    ''',
    subagent_type="docs-vision",
    model="sonnet"
)
```

## Success Criteria

✅ Code meets AGENTS.md standards
✅ Local tests pass
✅ Linting clean
✅ Sub-agents invoked and succeed
✅ Workspace archived
'''

Write(file_path=".claude/agents/expert.md", content=expert_agent.format(
    project_name=project_name,
    framework=detected_framework,
    language=primary_language,
    source_dir=source_directory,
    type_hint_requirements=type_hint_requirements,
    docstring_requirements=docstring_requirements,
    architecture_requirements=architecture_requirements,
    linting_command=linting_command,
    test_command=test_command
))
print("✓ Created .claude/agents/expert.md")

```

### Step 4.3: Create Testing Agent

```python
testing_agent = '''---
name: testing
description: {project_name} testing specialist - comprehensive test creation using pytest
tools: mcp__context7__resolve-library-id, mcp__context7__get-library-docs, WebSearch, mcp__zen__debug, mcp__zen__chat, Read, Edit, Write, Bash, Glob, Grep, Task
model: sonnet
---

# Testing Agent

Test creation specialist for {project_name}. Creates comprehensive test suites with 90%+ coverage.

## Core Responsibilities

1. **Test Planning** - Develop comprehensive test strategies
2. **Unit Tests** - Test components in isolation
3. **Integration Tests** - Test with real dependencies
4. **Edge Cases** - NULL, empty, error conditions
5. **Performance Tests** - N+1 query detection (MANDATORY)
6. **Concurrent Tests** - Race conditions, deadlocks
7. **Coverage Verification** - Ensure 90%+ coverage

## Workflow

### Step 1: Understand Requirements

```python
Read("specs/active/{{slug}}/prd.md")
Read("specs/active/{{slug}}/tasks.md")
# Read implemented code to understand what to test
```

### Step 2: Research Test Patterns

```python
Read("specs/guides/testing.md")
Read("AGENTS.md")  # Section on testing standards

# Find similar test patterns
Glob(pattern="tests/**/*.py")
Grep(pattern="@pytest.fixture", path="tests/")
```

**Research test framework docs if needed:**

```python
mcp__context7__resolve-library-id(libraryName="pytest")
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/pytest-dev/pytest",
    topic="async fixtures and pytest-asyncio",
    tokens=3000
)
```

### Step 3: Develop Test Plan

Cover:

- All acceptance criteria
- Unit tests for components
- Integration tests with dependencies
- Edge cases: NULL, empty, errors
- **Performance: N+1 query detection**
- **Concurrent access: race conditions**

### Step 4: Implement Tests

**Standards:**

- Function-based tests (not class-based)
- pytest with pytest-asyncio for async
- 90%+ coverage target (strictly enforced)
- Parallelizable tests (pytest-xdist)

**Examples:**

**Async test:**

```python
@pytest.mark.asyncio
async def test_fetch_data():
    result = await fetch_data(id=123)
    assert result is not None
```

**N+1 query detection:**

```python
from sqlalchemy import event
from sqlalchemy.engine import Engine

query_count = 0

@event.listens_for(Engine, "before_cursor_execute")
def count_queries(conn, cursor, statement, params, context, executemany):
    global query_count
    query_count += 1

@pytest.mark.asyncio
async def test_no_n_plus_one():
    global query_count
    query_count = 0

    items = await get_items_with_relationships(limit=10)

    # Should be 1-2 queries max (with joinedload)
    assert query_count <= 2, f"N+1 detected: {{query_count}} queries"
```

**Concurrent access:**

```python
import asyncio

@pytest.mark.asyncio
async def test_concurrent_updates():
    tasks = [
        update_resource(id=123, status="active")
        for _ in range(10)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(not isinstance(r, Exception) for r in results)
```

### Step 5: Execute & Verify

```bash
{test_command}
{coverage_command}
```

**Coverage must be 90%+ for modified modules.**

### Step 6: Update Progress

```python
Edit(file_path="specs/active/{{slug}}/tasks.md", ...)
Edit(file_path="specs/active/{{slug}}/recovery.md", ...)
```

## Success Criteria

✅ All acceptance criteria tested
✅ Unit tests created
✅ Integration tests created
✅ Edge cases covered
✅ **N+1 query detection tests**
✅ **Concurrent access tests**
✅ **90%+ coverage achieved**
✅ All tests pass
✅ Parallel execution works

'''

Write(file_path=".claude/agents/testing.md", content=testing_agent.format(
    project_name=project_name,
    test_command=test_command,
    coverage_command=coverage_command
))
print("✓ Created .claude/agents/testing.md")

```

### Step 4.4: Docs & Vision Agent (Already exists, verify it has anti-pattern detection)

```python
# This agent was already created in Phase 4.3 of the alignment
# Verify it exists and has anti-pattern detection
if not exists(".claude/agents/docs-vision.md"):
    # Create it (code similar to existing one but project-customized)
    pass
```

### Step 4.5: Sync Guides Agent (Already exists from Phase 4.1)

```python
# This agent was already created
# Verify it exists
if not exists(".claude/agents/sync-guides.md"):
    # Create it (code from earlier in alignment)
    pass
```

---

## PHASE 4.6: SQLSPEC SKILLS CREATION (If SQLSpec Project)

**Objective**: Auto-create comprehensive SQLSpec usage skills for database-focused projects.

### Step 4.6.1: Detect if SQLSpec Project

```python
is_sqlspec_project = (
    Grep(pattern=r'from sqlspec|import sqlspec', path='.', output_mode='count') > 5 or
    exists("sqlspec/") or
    (exists("pyproject.toml") and "sqlspec" in Read("pyproject.toml"))
)

if not is_sqlspec_project:
    print("Not a SQLSpec project - skipping SQLSpec skills creation")
    # Skip to Phase 5
```

### Step 4.6.2: Create SQLSpec Skills Directory

```bash
mkdir -p .claude/skills/sqlspec-usage/patterns
mkdir -p .claude/skills/sqlspec-usage/examples
mkdir -p .claude/skills/sqlspec-adapters
```

### Step 4.6.3: Generate Main SQLSpec Skill

```python
sqlspec_skill = '''# SQLSpec Usage Expert Skill

**Version:** 1.0.0
**Category:** Database, Python, SQLSpec
**Status:** Active

## Description

Comprehensive guidance on using SQLSpec - a type-safe SQL query mapper for Python.
Covers configuration, query execution, framework integration, migrations, testing,
and performance optimization across all supported database adapters.

## Activation Triggers

- SQLSpec configuration or setup questions
- Database connection management
- Query execution patterns
- Framework integration (Litestar, FastAPI, Starlette, Flask)
- Migration management
- Testing with SQLSpec
- Performance optimization
- Multi-database setups

## Quick Reference

See pattern guides for detailed information:
- [Configuration Patterns](patterns/configuration.md)
- [Query Execution Patterns](patterns/queries.md)
- [Framework Integration](patterns/frameworks.md)
- [Migration Patterns](patterns/migrations.md)
- [Testing Best Practices](patterns/testing.md)
- [Performance Optimization](patterns/performance.md)
- [Troubleshooting Guide](patterns/troubleshooting.md)

Working examples in `examples/` directory.
'''

Write(file_path=".claude/skills/sqlspec-usage/skill.md", content=sqlspec_skill)
print("✓ Created .claude/skills/sqlspec-usage/skill.md")
```

### Step 4.6.4: Generate Pattern Guides

Create comprehensive pattern guides:

```python
# Configuration patterns
Write(file_path=".claude/skills/sqlspec-usage/patterns/configuration.md", content=config_patterns)

# Query patterns
Write(file_path=".claude/skills/sqlspec-usage/patterns/queries.md", content=query_patterns)

# Framework integration
Write(file_path=".claude/skills/sqlspec-usage/patterns/frameworks.md", content=framework_patterns)

# Migration patterns
Write(file_path=".claude/skills/sqlspec-usage/patterns/migrations.md", content=migration_patterns)

# Testing patterns
Write(file_path=".claude/skills/sqlspec-usage/patterns/testing.md", content=testing_patterns)

# Performance patterns
Write(file_path=".claude/skills/sqlspec-usage/patterns/performance.md", content=performance_patterns)

# Troubleshooting
Write(file_path=".claude/skills/sqlspec-usage/patterns/troubleshooting.md", content=troubleshooting_guide)

print("✓ Created all SQLSpec pattern guides")
```

### Step 4.6.5: Generate Working Examples

```python
# Litestar integration example
Write(file_path=".claude/skills/sqlspec-usage/examples/litestar-integration.py", content=litestar_example)

# FastAPI integration example
Write(file_path=".claude/skills/sqlspec-usage/examples/fastapi-integration.py", content=fastapi_example)

# Multi-database example
Write(file_path=".claude/skills/sqlspec-usage/examples/multi-database.py", content=multi_db_example)

# Testing patterns example
Write(file_path=".claude/skills/sqlspec-usage/examples/testing-patterns.py", content=testing_example)

# Migration workflow shell script
Write(file_path=".claude/skills/sqlspec-usage/examples/migration-workflow.sh", content=migration_script)

print("✓ Created all SQLSpec example files")
```

### Step 4.6.6: Detect Project Adapters and Create Adapter Skills

```python
# Detect which adapters are used in this project
adapters_used = []

if Grep(pattern=r'from sqlspec.adapters.asyncpg', path='.', output_mode='count') > 0:
    adapters_used.append("asyncpg")
if Grep(pattern=r'from sqlspec.adapters.psycopg', path='.', output_mode='count') > 0:
    adapters_used.append("psycopg")
if Grep(pattern=r'from sqlspec.adapters.duckdb', path='.', output_mode='count') > 0:
    adapters_used.append("duckdb")
if Grep(pattern=r'from sqlspec.adapters.sqlite', path='.', output_mode='count') > 0:
    adapters_used.append("sqlite")
if Grep(pattern=r'from sqlspec.adapters.aiosqlite', path='.', output_mode='count') > 0:
    adapters_used.append("aiosqlite")
if Grep(pattern=r'from sqlspec.adapters.oracledb', path='.', output_mode='count') > 0:
    adapters_used.append("oracledb")

print(f"Detected adapters: {', '.join(adapters_used)}")

# Create adapter-specific skills
for adapter in adapters_used:
    adapter_skill_content = generate_adapter_skill(adapter)
    Write(file_path=f".claude/skills/sqlspec-adapters/{adapter}.md", content=adapter_skill_content)
    print(f"✓ Created .claude/skills/sqlspec-adapters/{adapter}.md")

# Create adapters README
adapters_readme = '''# SQLSpec Adapter Skills

Adapter-specific guidance for each database adapter used in this project.

## Detected Adapters

''' + '\n'.join([f'- [{adapter}.md]({adapter}.md)' for adapter in adapters_used]) + '''

## Adapter Selection Guide

See main skill documentation for complete adapter comparison.
'''

Write(file_path=".claude/skills/sqlspec-adapters/README.md", content=adapters_readme)
print("✓ Created .claude/skills/sqlspec-adapters/README.md")
```

### Step 4.6.7: Update Agent Files to Reference Skills

```python
# Add skills reference to expert.md
expert_skills_section = '''
**Use SQLSpec skills for guidance:**

```python
# Main SQLSpec usage skill
Read(".claude/skills/sqlspec-usage/skill.md")

# Pattern guides
Read(".claude/skills/sqlspec-usage/patterns/configuration.md")
Read(".claude/skills/sqlspec-usage/patterns/queries.md")
Read(".claude/skills/sqlspec-usage/patterns/frameworks.md")
Read(".claude/skills/sqlspec-usage/patterns/testing.md")

# Adapter-specific skills
Read(f".claude/skills/sqlspec-adapters/{adapter}.md")

# Working examples
Read(".claude/skills/sqlspec-usage/examples/litestar-integration.py")
```
'''

# Insert into expert.md after guides section
# (Implementation details...)

# Add skills reference to testing.md
testing_skills_section = '''
```python
Read(".claude/skills/sqlspec-usage/patterns/testing.md")
Read(".claude/skills/sqlspec-usage/examples/testing-patterns.py")
```
'''

# Insert into testing.md
# (Implementation details...)

print("✓ Updated agent files to reference SQLSpec skills")
```

### Step 4.6.8: Summary

```python
print("\n=== SQLSPEC SKILLS CREATED ===\n")
print("Main skill: .claude/skills/sqlspec-usage/skill.md")
print("Pattern guides: .claude/skills/sqlspec-usage/patterns/")
print("Examples: .claude/skills/sqlspec-usage/examples/")
print(f"Adapter skills: {len(adapters_used)} adapters detected")
print("\nAgents updated to reference skills automatically.")
```

---

## PHASE 5: PROJECT GUIDES CREATION

### Step 5.1: Create Architecture Guide

```python
architecture_guide = f'''# Architecture Guide: {project_name}

**Last Updated**: {datetime.now().strftime("%Y-%m-%d")}

## Overview

{project_name} uses **{detected_architecture_pattern}**.

## Project Structure

```

{project_structure_tree}

```

## Core Components

{discovered_components}

## Design Patterns

{detected_patterns}

## Data Flow

{data_flow}

## Key Conventions

{project_conventions}
'''

Write(file_path="specs/guides/architecture.md", content=architecture_guide)
print("✓ Created specs/guides/architecture.md")
```

### Step 5.2: Create Testing Guide

```python
testing_guide = f'''# Testing Guide: {project_name}

## Framework

{test_framework}

## Running Tests

```bash
{test_commands}
```

## Standards

- **Coverage**: 90%+ for modified modules
- **Style**: Function-based pytest
- **N+1 Detection**: Mandatory for database ops
- **Async**: Use pytest-asyncio
- **Parallel**: Tests must work with -n auto

## Examples

{test_examples}
'''

Write(file_path="specs/guides/testing.md", content=testing_guide)
print("✓ Created specs/guides/testing.md")

```

### Step 5.3: Create Code Style Guide

```python
style_guide = f'''# Code Style Guide: {project_name}

## Language

**{primary_language}** {language_version}

## Type Hints

{type_hint_standards}

## Formatting

{formatting_standards}

## Documentation

{docstring_standards}

## Linting

```bash
{linting_commands}
```

'''

Write(file_path="specs/guides/code-style.md", content=style_guide)
print("✓ Created specs/guides/code-style.md")

```

---

## PHASE 6: TEMPLATE STRUCTURE

### Step 6.1: Create Template Files

```python
# Create prd.md, tasks.md, recovery.md templates
# (Similar to Gemini bootstrap but adapted for Claude)

Write(file_path="specs/template-spec/prd.md", content=template_prd)
Write(file_path="specs/template-spec/tasks.md", content=template_tasks)
Write(file_path="specs/template-spec/recovery.md", content=template_recovery)
Write(file_path="specs/template-spec/research/plan.md", content=template_research)
```

---

## PHASE 7: VERIFICATION & SUMMARY

### Step 7.1: Verify All Files Created

```python
required_files = [
    "AGENTS.md",
    ".claude/agents/prd.md",
    ".claude/agents/expert.md",
    ".claude/agents/testing.md",
    ".claude/agents/docs-vision.md",
    ".claude/agents/sync-guides.md",
    "specs/guides/architecture.md",
    "specs/guides/testing.md",
    "specs/guides/code-style.md",
    "specs/template-spec/prd.md",
    "specs/template-spec/tasks.md",
    "specs/template-spec/recovery.md",
]

for file in required_files:
    if exists(file):
        print(f"✓ {file}")
    else:
        print(f"✗ {file} - MISSING")
```

### Step 7.2: Generate Summary

```python
summary = f'''
# 🎉 Claude Agent System Bootstrap Complete

**Project**: {project_name}
**Date**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Created Files

### Configuration
- ✓ AGENTS.md - Project standards and workflow
- ✓ .claude/agents/prd.md - PRD creation agent
- ✓ .claude/agents/expert.md - Implementation agent
- ✓ .claude/agents/testing.md - Testing agent (90%+ coverage)
- ✓ .claude/agents/docs-vision.md - Documentation & quality gate agent
- ✓ .claude/agents/sync-guides.md - Documentation sync agent

### Guides
- ✓ specs/guides/architecture.md
- ✓ specs/guides/testing.md
- ✓ specs/guides/code-style.md

### Workspace
- ✓ specs/active/ (gitignored)
- ✓ specs/archive/ (gitignored)
- ✓ specs/template-spec/

## Project Analysis

**Language**: {primary_language}
**Framework**: {detected_framework}
**Test Framework**: {test_framework}
**Coverage Target**: 90%+
**MCP Tools**: Context7, Zen (planner, chat, thinkdeep, analyze, debug)

## Usage

Start a new feature:
```bash
# PRD agent creates workspace
Task(
    description="Create PRD for new feature",
    prompt="Create PRD for: [feature description]",
    subagent_type="prd"
)

# Expert agent implements (auto-invokes testing and docs)
Task(
    description="Implement feature",
    prompt="Implement feature from specs/active/[slug]",
    subagent_type="expert"
)

# Sync documentation
Task(
    description="Sync documentation",
    prompt="Ensure specs/guides/ matches codebase",
    subagent_type="sync-guides"
)
```

## Next Steps

1. Review AGENTS.md for accuracy
2. Review specs/guides/ content
3. Test agent invocation
4. Add project-specific patterns to guides

---

🚀 **Claude Agent System is ready!**
'''

Write(file_path="CLAUDE_BOOTSTRAP_SUMMARY.md", content=summary)
print(summary)

```

---

## COMPLETE BOOTSTRAP EXECUTION

**Execute this bootstrap by running it with Claude Code in your project root.**

The bootstrap will:
1. ✓ Analyze project structure
2. ✓ Detect language, framework, tools
3. ✓ Create .claude/agents/ with specialized agents
4. ✓ Generate AGENTS.md tailored to project
5. ✓ Create specs/guides/
6. ✓ Set up workspace templates
7. ✓ Update .gitignore

**Setup Time**: ~5-10 minutes

**Result**: Complete Claude multi-agent system configured for your project.

---

**Version**: 1.0
**Adapted from**: Gemini Bootstrap 4.0
**License**: Use freely in any project
