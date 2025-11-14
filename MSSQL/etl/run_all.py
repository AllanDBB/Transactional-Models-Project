#!/usr/bin/env python3
"""Script maestro: Limpiar, cargar datos y ejecutar ETL"""

import subprocess
import sys

venv_python = r"c:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\.venv\Scripts\python.exe"

scripts = [
    ("clean_transactional.py", "Limpiar BD transaccional..."),
    ("load_test_data.py", "Cargar datos de prueba..."),
    ("force_delete.py", "Limpiar DWH..."),
    ("run_etl.py", "Ejecutar ETL..."),
]

for script, desc in scripts:
    print(f"\n{'=' * 80}")
    print(f"{desc}")
    print(f"{'=' * 80}")
    result = subprocess.run([venv_python, script], cwd=".")
    if result.returncode != 0:
        print(f"\n[ERROR] {script} falló con código {result.returncode}")
        sys.exit(1)

print(f"\n{'=' * 80}")
print("[OK] TODOS LOS PASOS COMPLETADOS")
print(f"{'=' * 80}")
