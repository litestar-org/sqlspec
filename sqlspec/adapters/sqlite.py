from sqlspec.adapters.dbapi import GenericAdapter


class SQLite3Adapter(GenericAdapter):
    # overwrites some methods using sqlite3-specific non-standard methods

    def insert_returning(self, conn, statement_name, sql, parameters):
        cur = self._cursor(conn)
        try:
            cur.execute(sql, parameters)
            results = cur.lastrowid
        finally:
            cur.close()
        return results

    def execute_script(self, conn, sql) -> str:
        conn.executescript(sql)
        return "DONE"
