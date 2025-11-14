#!/usr/bin/env python3
"""Limpiar TODA la BD transaccional"""

import pyodbc
from config import DatabaseConfig

conn = pyodbc.connect(DatabaseConfig.get_source_connection_string())
cursor = conn.cursor()

print("Limpiando BD transaccional...")

# Orden: detalles, órdenes, productos, clientes
cursor.execute("DELETE FROM sales_ms.OrdenDetalle")
cursor.execute("DELETE FROM sales_ms.Orden")
cursor.execute("DELETE FROM sales_ms.Producto")
cursor.execute("DELETE FROM sales_ms.Cliente")

conn.commit()

cursor.execute("SELECT COUNT(*) FROM sales_ms.Cliente")
print(f"  Clientes: {cursor.fetchone()[0]}")
cursor.execute("SELECT COUNT(*) FROM sales_ms.Producto")
print(f"  Productos: {cursor.fetchone()[0]}")
cursor.execute("SELECT COUNT(*) FROM sales_ms.Orden")
print(f"  Órdenes: {cursor.fetchone()[0]}")
cursor.execute("SELECT COUNT(*) FROM sales_ms.OrdenDetalle")
print(f"  Detalles: {cursor.fetchone()[0]}")

cursor.close()
conn.close()
print("[OK] BD transaccional limpiada")
