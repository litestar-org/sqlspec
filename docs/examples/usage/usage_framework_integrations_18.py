# start-example
def get_db():
    if 'db' not in g:
        g.db = spec.provide_session(db).__enter__()
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop('db', None)
    if db is not None:
        db.__exit__(None, None, None)

# Use in routes
@app.route('/users/<int:user_id>')
def get_user(user_id):
    db = get_db()
    result = db.execute("SELECT * FROM users WHERE id = ?", user_id)
    return result.one()
# end-example

def test_stub():
    assert True
