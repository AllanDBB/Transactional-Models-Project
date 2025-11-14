#!/usr/bin/env python3
"""
Ejecutor maestro del ETL PASO 3
Limpia todo y ejecuta el pipeline completo de forma secuencial
"""
import subprocess
import sys
import os

os.chdir(r"c:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\MSSQL\etl")

venv_python = r"c:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\.venv\Scripts\python.exe"

print("=" * 80)
print("[PASO 3 ETL - MAESTRO]")
print("=" * 80)

# Paso 1: Limpiar BD transaccional
print("\n[1/4] Limpiando BD transaccional...")
result = subprocess.run([venv_python, "clean_transactional.py"])
if result.returncode != 0:
    print("[ERROR] Falló al limpiar transaccional")
    sys.exit(1)

# Paso 2: Cargar datos de prueba
print("\n[2/4] Cargando datos de prueba...")
result = subprocess.run([venv_python, "load_test_data.py"])
if result.returncode != 0:
    print("[ERROR] Falló al cargar datos de prueba")
    sys.exit(1)

# Paso 3: Limpiar DWH
print("\n[3/4] Limpiando DWH...")
result = subprocess.run([venv_python, "force_delete.py"])
if result.returncode != 0:
    print("[ERROR] Falló al limpiar DWH")
    sys.exit(1)

# Paso 4: Ejecutar ETL
print("\n[4/4] Ejecutando ETL completo...")
result = subprocess.run([venv_python, "run_etl.py"])
if result.returncode != 0:
    print("[ERROR] Falló ETL")
    sys.exit(1)

print("\n" + "=" * 80)
print("[OK] PASO 3 COMPLETADO EXITOSAMENTE")
print("=" * 80)
