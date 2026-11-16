"""One-time, transactional transfer of gateway.db into Supabase PostgreSQL."""

import os
import sqlite3
import sys
import traceback
from pathlib import Path

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.db import _translate_sql


source_path = os.getenv("SQLITE_MIGRATION_PATH", "/migration/gateway.db")
database_url = os.environ["DATABASE_URL"]
source = sqlite3.connect(f"file:{source_path}?mode=ro&immutable=1", uri=True)
target = psycopg.connect(database_url, prepare_threshold=None)

try:
    with target.transaction():
        target.execute("CREATE SCHEMA IF NOT EXISTS llm_gateway")
        objects = source.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
            "AND type IN ('table', 'index') ORDER BY type DESC, name"
        ).fetchall()
        tables = [row for row in objects if row[0] == "table"]
        tables.sort(key=lambda row: (row[1] not in ("users", "sessions"), row[1]))

        for _, _, ddl in tables:
            target.execute(_translate_sql(ddl))
        for kind, _, ddl in objects:
            if kind == "index":
                target.execute(_translate_sql(ddl))

        counts = []
        for _, table, _ in tables:
            columns = [row[1] for row in source.execute(f'PRAGMA table_info("{table}")')]
            rows = source.execute(f'SELECT * FROM "{table}"').fetchall()
            if rows:
                insert = sql.SQL("INSERT INTO llm_gateway.{} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
                target.cursor().executemany(insert, rows)
            counts.append((table, len(rows)))

        sequences = target.execute(
            "SELECT table_name FROM information_schema.columns "
            "WHERE table_schema = 'llm_gateway' AND column_name = 'id' "
            "AND column_default LIKE 'nextval(%'"
        ).fetchall()
        for (table,) in sequences:
            target.execute(
                sql.SQL(
                    "SELECT setval(pg_get_serial_sequence(%s, 'id'), "
                    "COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM llm_gateway.{}"
                ).format(sql.Identifier(table)),
                (f"llm_gateway.{table}",),
            )

    for table, count in counts:
        print(f"{table}: {count} rows migrated")
except Exception as exc:
    state = getattr(exc, "sqlstate", None)
    message = getattr(getattr(exc, "diag", None), "message_primary", "")
    safe_message = str(exc) if isinstance(exc, sqlite3.Error) else message
    print(f"Migration failed; transaction rolled back ({type(exc).__name__}, SQLSTATE={state}, {safe_message}).", file=sys.stderr)
    traceback.print_tb(exc.__traceback__, file=sys.stderr)
    raise SystemExit(1) from None
finally:
    source.close()
    target.close()
