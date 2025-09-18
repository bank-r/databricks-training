import pyodbc

def pick_sql_driver():
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    installed = set(pyodbc.drivers())
    for d in preferred:
        if d in installed:
            return d
    raise RuntimeError(
        f"No suitable SQL Server ODBC driver found. Installed: {sorted(installed)}"
    )