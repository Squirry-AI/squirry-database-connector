import re

SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT",
    "ON", "GROUP", "BY", "ORDER", "INSERT", "INTO", "VALUES",
    "UPDATE", "SET", "DELETE", "AND", "OR", "LIMIT", "OFFSET",
    # Add more as needed...
}

def query_refiner(query: str, db_type: str) -> str:
    def repl(m):
        ident = m.group(1)
        if ident.upper() in SQL_KEYWORDS:
            return ident  # leave keywords untouched
        return f'`{ident}`' if db_type in ("mysql", "sqlite") else f'"{ident}"'
    
    pattern = r'(?<![`"\w\.])([a-zA-Z_][a-zA-Z0-9_]*)'
    return re.sub(pattern, repl, query)
