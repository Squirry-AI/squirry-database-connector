# utils/db/constants.py

POSTGRES = "postgres"
MYSQL = "mysql"
SQLITE = "sqlite"

POSTGRES_URL_PREFIX = "postgresql://"
MYSQL_URL_PREFIX = "mysql://"
LEGACY_POSTGRES_URL_PREFIX = "postgres://"

ParameterTypes = {
    "sqlite": "templateParameters",
    "mysql": "parameters",
    "postgres": "parameters"
}