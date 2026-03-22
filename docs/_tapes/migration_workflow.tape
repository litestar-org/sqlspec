# SQLSpec Migration Workflow Demo
# Demonstrates: CLI migration commands with SQLite

Output docs/_static/demos/migration_workflow.gif

Set Shell "bash"
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 50ms

Type "# SQLSpec Migrations - lightweight, code-first workflow"
Enter
Sleep 1s

Hide
Type "source .venv/bin/activate"
Enter
Sleep 500ms
Type "cd $(mktemp -d)"
Enter
Sleep 500ms
Show

Type "# Initialize the migration environment"
Enter
Sleep 500ms

Type "sqlspec init"
Enter
Sleep 3s

Type "# Create a new migration"
Enter
Sleep 500ms

Type 'sqlspec create-migration -m "add users table"'
Enter
Sleep 3s

Type "# Apply the migration"
Enter
Sleep 500ms

Type "sqlspec upgrade"
Enter
Sleep 3s

Type "# Check current revision"
Enter
Sleep 500ms

Type "sqlspec show-current-revision"
Enter
Sleep 3s

Type "# Clean, simple, no ORM required!"
Enter
Sleep 3s
