import logging
import os
import time
from typing import Iterable, Sequence

import pymssql
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)


def get_connection():
    server = os.getenv("serverenv", "localhost")
    database = os.getenv("databaseenv", "MSSQL_DW")
    user = os.getenv("usernameenv")
    password = os.getenv("passwordenv")

    parts = server.replace(",", ":").split(":")
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 1433
    if host == "localhost":
        host = "sqlserver-dw"
        port = 1433

    return pymssql.connect(
        server=host,
        port=port,
        user=user,
        password=password,
        database=database,
        timeout=300,  # 5 minutos timeout para queries pesadas
        as_dict=False,
    )


def wait_for_db(retries: int = 30, delay: float = 2.0):
    for i in range(retries):
        try:
            with get_connection():
                LOGGER.info("DB lista (%s/%s)", i + 1, retries)
                return True
        except Exception as e:
            LOGGER.info("DB no disponible (%s/%s): %s", i + 1, retries, e)
            time.sleep(delay)
    return False


def executemany(table: str, columns: Sequence[str], rows: Iterable[Sequence]):
    rows = list(rows)
    if not rows:
        LOGGER.info("Sin filas para insertar en %s", table)
        return 0
    placeholders = ", ".join(["%s"] * len(columns))
    cols_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"
    if not wait_for_db():
        LOGGER.error("DB no disponible para insertar en %s", table)
        return 0
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        LOGGER.info("Insertadas %s filas en %s", cur.rowcount, table)
        return cur.rowcount


def executemany_chunks(table: str, columns: Sequence[str], rows: Iterable[Sequence], chunk_size: int = 5000):
    rows = list(rows)
    if not rows:
        LOGGER.info("Sin filas para insertar en %s", table)
        return 0
    total = 0
    placeholders = ", ".join(["%s"] * len(columns))
    cols_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"
    if not wait_for_db():
        LOGGER.error("DB no disponible para insertar en %s", table)
        return 0
    with get_connection() as conn:
        cur = conn.cursor()
        for i in range(0, len(rows), chunk_size):
            batch = rows[i : i + chunk_size]
            cur.executemany(sql, batch)
            total += cur.rowcount
        conn.commit()
        LOGGER.info("Insertadas %s filas en %s (chunks de %s)", total, table, chunk_size)
        return total


def execute_sp(sp_name: str):
    if not wait_for_db():
        LOGGER.error("DB no disponible para ejecutar %s", sp_name)
        return
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"EXEC {sp_name};")
        conn.commit()
        LOGGER.info("Ejecutado %s", sp_name)


def clear_table(table: str):
    if not wait_for_db():
        LOGGER.error("DB no disponible para truncar %s", table)
        return
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"TRUNCATE TABLE {table};")
        except Exception:
            cur.execute(f"DELETE FROM {table};")
        conn.commit()
        LOGGER.info("Limpieza completa de %s", table)
