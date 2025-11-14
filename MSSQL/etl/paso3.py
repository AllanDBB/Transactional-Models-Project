#!/usr/bin/env python3
"""
PASO 3 MAESTRO: Cargar datos y ejecutar ETL completo
"""
import subprocess
import sys
import os

os.chdir(r"c:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\MSSQL\etl")

venv_python = r"c:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\.venv\Scripts\python.exe"

print("\n" + "=" * 80)
print("PASO 3: EJECUTAR ETL COMPLETO")
print("=" * 80)

# Paso 1: Cargar datos de prueba
print("\n[PASO 1/2] Cargando datos de prueba en BD transaccional...")
result = subprocess.run([venv_python, "load_test_data.py"])
if result.returncode != 0:
    print("\n[ERROR] Falló al cargar datos de prueba")
    sys.exit(1)

# Paso 2: Ejecutar ETL
print("\n\n[PASO 2/2] Ejecutando ETL completo (Extract -> Transform -> Load)...")
result = subprocess.run([venv_python, "run_etl.py"])
if result.returncode != 0:
    print("\n[ERROR] Falló ETL")
    sys.exit(1)

print("\n" + "=" * 80)
print("[OK] PASO 3 COMPLETADO EXITOSAMENTE")
print("=" * 80)
