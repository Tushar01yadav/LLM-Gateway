"""Small SQLite-compatible database adapter for PostgreSQL deployments."""

import os
import re
import sqlite3 as _sqlite3

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
_TABLES = (
    "async_jobs_new", "async_jobs", "audit_logs", "budget_accounts", "budget_reservations",
    "cost_records", "gateway_api_keys",
    "messages", "oauth_states", "oauth_tokens", "otp_verifications", "request_logs",
    "sessions", "user_sessions", "users",
)

if DATABASE_URL:
    import psycopg

    _INTEGRITY_ERROR = psycopg.IntegrityError
    _OPERATIONAL_ERROR = psycopg.OperationalError
else:
    _INTEGRITY_ERROR = _sqlite3.IntegrityError
    _OPERATIONAL_ERROR = _sqlite3.OperationalError


def _translate_sql(sql):
    if not DATABASE_URL:
        return sql

    statement = sql.strip()
    if statement.upper().startswith("PRAGMA "):
        return None

    statement = re.sub(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", statement, flags=re.I)
    if statement != sql.strip() and not re.search(r"\bON\s+CONFLICT\b", statement, flags=re.I):
        statement += " ON CONFLICT DO NOTHING"
    statement = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "BIGSERIAL PRIMARY KEY", statement, flags=re.I)
    statement = re.sub(r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(?!IF\s+NOT\s+EXISTS)(\w+)", r"ALTER TABLE \1 ADD COLUMN IF NOT EXISTS \2", statement, flags=re.I)
    statement = re.sub(r"datetime\s*\(\s*'now'\s*,\s*\?\s*\)", "(CURRENT_TIMESTAMP + ?::interval)", statement, flags=re.I)
    statement = re.sub(r"datetime\s*\(\s*timestamp\s*\)", "timestamp", statement, flags=re.I)
    statement = statement.replace("BEGIN IMMEDIATE", "BEGIN")
    for table in _TABLES:
        statement = re.sub(
            rf"\b(FROM|JOIN|INTO|UPDATE|TABLE|REFERENCES|ON)\s+((?:IF\s+NOT\s+EXISTS\s+)?)({table})\b",
            rf"\1 \2llm_gateway.\3",
            statement,
            flags=re.I,
        )
    return statement.replace("?", "%s")


class _Cursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, parameters=()):
        translated = _translate_sql(sql)
        if translated is not None:
            if parameters:
                self._cursor.execute(translated, parameters)
            else:
                self._cursor.execute(translated)
        else:
            self._cursor.execute("SELECT 1 WHERE FALSE")
        return self

    def executemany(self, sql, seq_of_parameters):
        translated = _translate_sql(sql)
        if translated is not None:
            self._cursor.executemany(translated, seq_of_parameters)
        return self

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _Connection:
    def __init__(self, dsn, **kwargs):
        import psycopg

        self._connection = psycopg.connect(DATABASE_URL, prepare_threshold=None)
        self._connection.execute("CREATE SCHEMA IF NOT EXISTS llm_gateway")

    def cursor(self, *args, **kwargs):
        return _Cursor(self._connection.cursor(*args, **kwargs))

    def execute(self, sql, parameters=()):
        return self.cursor().execute(sql, parameters)

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def close(self):
        return self._connection.close()

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _SQLiteCompat:
    IntegrityError = _INTEGRITY_ERROR
    OperationalError = _OPERATIONAL_ERROR

    @staticmethod
    def connect(database=None, *args, **kwargs):
        if not DATABASE_URL:
            return _sqlite3.connect(database, *args, **kwargs)
        return _Connection(DATABASE_URL)


sqlite3 = _SQLiteCompat()
