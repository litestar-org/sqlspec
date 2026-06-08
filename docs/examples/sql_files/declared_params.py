from pathlib import Path

__all__ = ("test_declared_params",)


def test_declared_params(tmp_path: "Path") -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.exceptions import SQLSpecError

    sql_file = tmp_path / "teams.sql"
    sql_file.write_text(
        "-- name: get_team_by_name\n"
        "-- param: name str  The team name to look up\n"
        "select id, name from teams where name = :name\n"
    )

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    spec.load_sql_files(sql_file)

    # Introspect declared parameters without executing.
    declarations = spec.get_query_parameters("get_team_by_name")
    assert declarations[0].name == "name"
    assert declarations[0].type_str == "str"
    assert declarations[0].description == "The team name to look up"

    # The declarations also ride on the SQL object returned by get_sql().
    query = spec.get_sql("get_team_by_name")
    assert query.declared_parameters == declarations

    with spec.provide_session(config) as session:
        session.execute("create table teams (id integer primary key, name text)")
        session.execute("insert into teams (name) values ('Litestar'), ('SQLSpec')")

        # A declared query validates supplied parameters automatically.
        row = session.execute(query, {"name": "SQLSpec"}).one()

        # Omitting a declared parameter raises before the query reaches the driver.
        try:
            session.execute(spec.get_sql("get_team_by_name"), {})
        except SQLSpecError as exc:
            missing_error = str(exc)
    # end-example

    assert row["name"] == "SQLSpec"
    assert "name" in missing_error
