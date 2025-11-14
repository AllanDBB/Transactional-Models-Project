#!/usr/bin/env python3
"""Force delete from DimCategory and DimChannel"""

import pyodbc
from config import DatabaseConfig

conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
cursor = conn.cursor()

print("Forzando DELETE...")
cursor.execute("DELETE FROM DimCategory")
cursor.execute("DELETE FROM DimChannel")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM DimCategory")
print(f"DimCategory: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM DimChannel")
print(f"DimChannel: {cursor.fetchone()[0]}")

cursor.close()
conn.close()
