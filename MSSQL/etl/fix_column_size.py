#!/usr/bin/env python3
"""Ampliar columna gender en DimCustomer"""

import pyodbc
from config import DatabaseConfig

conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
cursor = conn.cursor()

print("Ampliando columna gender...")

# Remover constraint de CHECK
cursor.execute("""
    ALTER TABLE DimCustomer DROP CONSTRAINT chk_gender
""")

# Cambiar el tipo de dato
cursor.execute("""
    ALTER TABLE DimCustomer ALTER COLUMN gender VARCHAR(20)
""")

conn.commit()
cursor.close()
conn.close()

print("[OK] Columna gender ampliada de VARCHAR(1) a VARCHAR(20)")
