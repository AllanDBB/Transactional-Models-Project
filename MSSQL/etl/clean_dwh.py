#!/usr/bin/env python3
"""Script para limpiar las tablas del DWH manualmente"""

import pyodbc
from config import DatabaseConfig

conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
cursor = conn.cursor()

print("=" * 80)
print("[LIMPIANDO TABLAS DEL DWH]")
print("=" * 80)

# Orden importante: primero FactSales, luego dimensiones
tables_order = ['FactSales', 'DimCustomer', 'DimProduct', 'DimChannel', 'DimCategory', 'DimTime']

for table in tables_order:
    try:
        cursor.execute(f"TRUNCATE TABLE {table}")
        print(f"  [OK] {table} truncada")
    except Exception as e:
        print(f"  [WARN] {table}: {str(e)}")
        # Intentar DELETE
        try:
            cursor.execute(f"DELETE FROM {table}")
            print(f"       Limpiada con DELETE")
        except Exception as e2:
            print(f"       [ERROR] No se pudo limpiar: {str(e2)}")

conn.commit()

# Verificar
print("\n" + "=" * 80)
print("[VERIFICACION POST-LIMPIEZA]")
print("=" * 80)

for table in tables_order:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} registros")
    except Exception as e:
        print(f"  {table}: ERROR - {str(e)}")

cursor.close()
conn.close()
print("\n" + "=" * 80)
print("[OK] Limpieza completada")
print("=" * 80)
