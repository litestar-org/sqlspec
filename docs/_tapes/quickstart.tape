# SQLSpec Quickstart Demo
# Demonstrates: Connect to SQLite, create a table, insert and query data

Output docs/_static/demos/quickstart.gif

Set Shell "bash"
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 50ms

Type "# SQLSpec Quickstart - zero-config SQLite in seconds"
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

Type "from sqlspec.adapters.sqlite import SqliteConfig"
Enter
Sleep 500ms

Type "config = SqliteConfig(database=':memory:')"
Enter
Sleep 500ms

Type "# Provide a session from the config"
Enter
Sleep 500ms

Type "with config.provide_session() as driver:"
Enter
Type "    driver.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')"
Enter
Sleep 500ms

Type@50ms `    driver.execute("INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')")`
Enter
Sleep 500ms

Type@50ms `    driver.execute("INSERT INTO users (name, email) VALUES ('bob', 'bob@example.com')")`
Enter
Sleep 500ms

Type "    rows = driver.select('SELECT * FROM users')"
Enter
Type "    for row in rows:"
Enter
Type "        print(row)"
Enter
Enter
Sleep 3s

Type "# That's it - no pool setup, no boilerplate!"
Enter
Sleep 500ms

Type "exit()"
Enter
Sleep 2s
