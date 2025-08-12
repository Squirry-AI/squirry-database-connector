import re
from typing import Dict, Match

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

def query_refiner(query: str, db_type: str) -> Dict[str, str]:
    """
    Refines a SQL query by quoting identifiers based on the database type,
    while ignoring SQL keywords and string literals.

    Args:
        query: The SQL query string.
        db_type: The type of the database ('mysql', 'sqlite', 'postgres', etc.).

    Returns:
        A dictionary containing the refined SQL query.
    """
    def repl(m: Match[str]) -> str:
        """Replacement function for re.sub that handles identifiers and literals."""
        # The pattern has two capturing groups: one for literals, one for identifiers.
        literal, identifier = m.group(1), m.group(2)
        
        if literal:
            # This is a string literal (e.g., '%Batman%'), return it unchanged.
            return literal

        if identifier:
            if identifier.upper() in SQL_KEYWORDS:
                # This is a SQL keyword, return it unchanged.
                return identifier

            # This is an identifier, quote it based on the DB type.
            return f'`{identifier}`' if db_type in ("mysql", "sqlite") else f'"{identifier}"'

        return m.group(0) # Should not be reached with the current regex

    # This regex uses an OR '|' to match either a single-quoted string literal
    # or an unquoted identifier.
    # Group 1: ((?:'[^']*'|\"[^\"]*\"))
    # - Captures string literals, which can be enclosed in either single (') or double (") quotes.
    # - The inner (?:...) is a non-capturing group to handle the OR condition for the two quote types.
    # Group 2: (?<![`\"\w\.])([a-zA-Z_][a-zA-Z0-9_]*)
    # - Captures valid identifiers that are not already quoted or part of a qualified name (e.g. table.column).
    pattern = r"((?:'[^']*'|\"[^\"]*\"))|(?<![`\"\w\.])([a-zA-Z_][a-zA-Z0-9_]*)"
    refined_query = re.sub(pattern, repl, query)
    return {"refined_query": refined_query}
