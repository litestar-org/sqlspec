Implement the feature from the active workspace.

Invoke the Expert agent to:

1. **Read Plan** - Load prd.md, tasks.md, research/plan.md from .agents/{requirement}/
2. **Research** - Consult guides and library docs
3. **Implement** - Write clean, type-safe, performant code following CLAUDE.md standards
4. **Test** - Run relevant tests to verify implementation
5. **Update** - Mark tasks complete in tasks.md, update recovery.md

The expert should:
- Follow CLAUDE.md code quality standards (NO hasattr, NO workaround naming, etc.)
- Reference docs/guides/ for patterns
- Use zen.debug for complex bugs
- Use zen.thinkdeep for architectural decisions
- Use zen.analyze for code analysis
- Update workspace progress continuously

After implementation, hand off to testing.

Next step: Run `/test` to create comprehensive tests.
