#!/usr/bin/env python3
"""Limpiar DWH usando stored procedure"""

import pyodbc
from config import DatabaseConfig

print("=" * 80)
print("[LIMPIEZA DWH]")
print("=" * 80)

try:
    # Conectar a DWH
    conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
    cursor = conn.cursor()
    
    # Ejecutar stored procedure
    print("\nEjecutando dbo.sp_limpiar_dwh...")
    cursor.execute("EXEC dbo.sp_limpiar_dwh")
    
    # Obtener mensajes del servidor
    while cursor.nextset():
        pass
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("[OK] DWH limpiado exitosamente")
    print("=" * 80)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    exit(1)
