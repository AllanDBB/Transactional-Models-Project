#!/usr/bin/env python3
"""Eliminar duplicados y limpiar BD completamente"""

import pyodbc
from config import DatabaseConfig

print("=" * 80)
print("[LIMPIEZA COMPLETA DE BASES DE DATOS]")
print("=" * 80)

# 1. Limpiar BD transaccional
print("\n[1] Limpiando BD transaccional...")
conn = pyodbc.connect(DatabaseConfig.get_source_connection_string())
cursor = conn.cursor()

try:
    cursor.execute("DELETE FROM sales_ms.OrdenDetalle")
    cursor.execute("DELETE FROM sales_ms.Orden")
    cursor.execute("DELETE FROM sales_ms.Producto")
    cursor.execute("DELETE FROM sales_ms.Cliente")
    conn.commit()
    print("  [OK] BD transaccional limpiada")
except Exception as e:
    print(f"  [ERROR] {str(e)}")

cursor.close()
conn.close()

# 2. Limpiar DWH completamente
print("\n[2] Limpiando DWH...")
conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
cursor = conn.cursor()

tables = [
    'FactSales',
    'DimCustomer', 
    'DimProduct',
    'DimChannel',
    'DimCategory',
    'DimTime',
    'staging_map_producto',
    'staging_tipo_cambio',
    'staging_source_tracking'
]

for table in tables:
    try:
        cursor.execute(f"DELETE FROM {table}")
        print(f"  [OK] {table} limpiada")
    except Exception as e:
        print(f"  [WARN] {table}: {str(e)}")

conn.commit()

# Verificar
print("\n[3] Verificando estado...")
for table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} registros")
    except:
        pass

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("[OK] Limpieza completada")
print("=" * 80)
