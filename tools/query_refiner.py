import re

SQL_KEYWORDS = {
    # Data Query Language
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "WITH",

    # Data Manipulation Language
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "MERGE",

    # Data Definition Language
    "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "TABLE", "VIEW", "INDEX", "SEQUENCE", "DATABASE",

    # Transaction Control Language
    "COMMIT", "ROLLBACK", "SAVEPOINT",

    # Data Control Language
    "GRANT", "REVOKE",

    # Join Keywords
    "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON", "USING",

    # Operators and Conditions
    "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "IS", "NULL", "EXISTS", "ALL", "ANY", "UNION", "INTERSECT", "EXCEPT",

    # Functions / Aliasing / Distinctness
    "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",

    # Case Expressions
    "CASE", "WHEN", "THEN", "ELSE", "END",

    # Constraints
    "CONSTRAINT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",

    # Ordering
    "ASC", "DESC", "NULLS", "FIRST", "LAST",
}

def query_refiner(query: str, db_type: str) -> str:
    def repl(m):
        ident = m.group(1)
        if ident.upper() in SQL_KEYWORDS:
            return ident  # leave keywords untouched
        return f'`{ident}`' if db_type in ("mysql", "sqlite") else f'"{ident}"'
    
    pattern = r'(?<![`"\w\.])([a-zA-Z_][a-zA-Z0-9_]*)'
    return re.sub(pattern, repl, query)
