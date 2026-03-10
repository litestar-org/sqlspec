# SQLSpec Query Builder Demo
# Demonstrates: Fluent SQL builder for SELECT, INSERT, UPDATE

Output docs/_static/demos/query_builder.gif

Set Shell "bash"
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 50ms

Type "# SQLSpec Query Builder - fluent SQL construction"
Enter
Sleep 1s

Hide
Type "source .venv/bin/activate"
Enter
Sleep 500ms
Show

Type "python3"
Enter
Sleep 1s

Type "from sqlspec import sql"
Enter
Sleep 500ms

Type "# Build a SELECT with joins"
Enter
Sleep 500ms

Type "query = sql.select('users.name', 'orders.total').from_('users').join('orders', on='users.id = orders.user_id').where('orders.total > :min_total').order_by('orders.total DESC').limit(10)"
Enter
Sleep 1s

Type "print(query.build())"
Enter
Sleep 3s

Type "# Build an INSERT"
Enter
Sleep 500ms

Type "insert = sql.insert('users').columns('name', 'email').values(':name', ':email')"
Enter
Sleep 500ms

Type "print(insert.build())"
Enter
Sleep 3s

Type "# Build an UPDATE"
Enter
Sleep 500ms

Type "update = sql.update('users').set(email=':email').where('id = :id')"
Enter
Sleep 500ms

Type "print(update.build())"
Enter
Sleep 3s

Type "# Type-safe, composable, driver-agnostic"
Enter
Sleep 500ms

Type "exit()"
Enter
Sleep 2s
