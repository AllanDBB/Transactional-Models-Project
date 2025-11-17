#!/usr/bin/env python3
"""Limpiar BD Transaccional MSSQL usando stored procedure"""

import pyodbc
from config import DatabaseConfig

print("=" * 80)
print("[LIMPIEZA BD TRANSACCIONAL MSSQL]")
print("=" * 80)

try:
    # Conectar a BD transaccional
    conn = pyodbc.connect(DatabaseConfig.get_source_connection_string())
    cursor = conn.cursor()
    
    # Ejecutar stored procedure
    print("\nEjecutando sales_ms.sp_limpiar_bd...")
    cursor.execute("EXEC sales_ms.sp_limpiar_bd")
    
    # Obtener mensajes del servidor
    while cursor.nextset():
        pass
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("[OK] BD Transaccional limpiada exitosamente")
    print("=" * 80)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    exit(1)
