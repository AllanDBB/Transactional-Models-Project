#!/usr/bin/env python3
"""Generar datos de prueba en BD Transaccional usando stored procedure"""

import pyodbc
from config import DatabaseConfig

print("=" * 80)
print("[GENERACIÓN DE DATOS DE PRUEBA - BD TRANSACCIONAL MSSQL]")
print("=" * 80)
print("\nGenerando:")
print("  - 600 clientes (42 nombres reales)")
print("  - 5,000 productos")
print("  - 5,000 órdenes")
print("  - 17,500 detalles (promedio 3.5 items/orden)")
print()

try:
    # Conectar a BD transaccional
    conn = pyodbc.connect(DatabaseConfig.get_source_connection_string())
    cursor = conn.cursor()
    
    # Ejecutar stored procedure
    print("Ejecutando sales_ms.sp_generar_datos...")
    print("(Este proceso puede tardar 10-15 segundos)\n")
    
    cursor.execute("EXEC sales_ms.sp_generar_datos")
    
    # Obtener mensajes del servidor
    while cursor.nextset():
        pass
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("[OK] Datos generados exitosamente")
    print("=" * 80)
    print("\nPróximo paso:")
    print("  python run_etl.py")
    print()
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    exit(1)
