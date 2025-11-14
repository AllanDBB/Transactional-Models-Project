#!/usr/bin/env python3
"""Verificar estructura de la BD transaccional MSSQL"""

import pyodbc
from config import DatabaseConfig

conn = pyodbc.connect(DatabaseConfig.get_source_connection_string())
cursor = conn.cursor()

print("=" * 80)
print("[VERIFICANDO BASE DE DATOS TRANSACCIONAL]")
print("=" * 80)

# 1. Listar esquemas
print("\n[ESQUEMAS DISPONIBLES]")
cursor.execute("""
    SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema', 'pg_catalog')
    ORDER BY SCHEMA_NAME
""")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

# 2. Listar tablas en cada esquema
print("\n[TABLAS POR ESQUEMA]")
cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
""")
current_schema = None
for schema, table in cursor.fetchall():
    if schema != current_schema:
        print(f"\n  Esquema: {schema}")
        current_schema = schema
    print(f"    - {table}")

# 3. Intentar leer cada tabla
print("\n" + "=" * 80)
print("[INTENTANDO LEER TABLAS]")
print("=" * 80)

# Buscar todas las tablas
cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
""")
tables = cursor.fetchall()

for schema, table in tables:
    full_name = f"[{schema}].[{table}]"
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
        count = cursor.fetchone()[0]
        print(f"\n  {full_name}: {count} registros")
        
        # Si tiene registros, mostrar columnas
        if count > 0 or count == 0:
            cursor.execute(f"""
                SELECT TOP 1 * FROM {full_name}
            """)
            columns = [desc[0] for desc in cursor.description]
            print(f"    Columnas: {', '.join(columns)}")
    except Exception as e:
        print(f"\n  {full_name}: ERROR - {str(e)}")

conn.close()
print("\n" + "=" * 80)
