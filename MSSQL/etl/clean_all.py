#!/usr/bin/env python3
"""Limpiar completamente todas las bases de datos (transaccional y DWH)"""

import pyodbc
from config import DatabaseConfig

print("=" * 80)
print("[LIMPIEZA TOTAL DE BASES DE DATOS]")
print("=" * 80)

# 1. Limpiar DWH
print("\n[DWH] Limpiando tablas...")
try:
    conn_dw = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
    cursor_dw = conn_dw.cursor()
    
    tables_order = ['FactSales', 'DimCustomer', 'DimProduct', 'DimChannel', 'DimCategory', 'DimTime']
    
    for table in tables_order:
        try:
            cursor_dw.execute(f"DELETE FROM {table}")
            print(f"  [OK] {table} limpia")
        except Exception as e:
            print(f"  [WARN] {table}: {str(e)[:60]}")
    
    conn_dw.commit()
    cursor_dw.close()
    conn_dw.close()
    print("  [OK] DWH limpio")
except Exception as e:
    print(f"  [ERROR] No se pudo conectar a DWH: {str(e)}")

# 2. Limpiar BD transaccional
print("\n[TRANSACCIONAL] Limpiando tablas...")
try:
    conn_src = pyodbc.connect(DatabaseConfig.get_source_connection_string())
    cursor_src = conn_src.cursor()
    
    # Orden importante: primero detalles, luego órdenes, luego productos y clientes
    cursor_src.execute("DELETE FROM sales_ms.OrdenDetalle")
    print(f"  [OK] OrdenDetalle limpia")
    
    cursor_src.execute("DELETE FROM sales_ms.Orden")
    print(f"  [OK] Orden limpia")
    
    cursor_src.execute("DELETE FROM sales_ms.Producto")
    print(f"  [OK] Producto limpia")
    
    cursor_src.execute("DELETE FROM sales_ms.Cliente")
    print(f"  [OK] Cliente limpia")
    
    conn_src.commit()
    cursor_src.close()
    conn_src.close()
    print("  [OK] Transaccional limpia")
except Exception as e:
    print(f"  [ERROR] No se pudo conectar a Transaccional: {str(e)}")

print("\n" + "=" * 80)
print("[OK] Limpieza completada")
print("=" * 80)
