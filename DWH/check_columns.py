import pymssql
import os
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("serverenv", "localhost")
database = os.getenv("databaseenv", "MSSQL_DW")
user = os.getenv("usernameenv")
password = os.getenv("passwordenv")

parts = server.replace(",", ":").split(":")
host = parts[0]
port = int(parts[1]) if len(parts) > 1 else 1433
if host == "localhost":
    host = "sqlserver-dw"
    port = 1433

conn = pymssql.connect(
    server=host,
    port=port,
    user=user,
    password=password,
    database=database
)

cur = conn.cursor()
cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA='staging' AND TABLE_NAME='supabase_users' 
    ORDER BY ORDINAL_POSITION
""")

print("Columnas en staging.supabase_users:")
for row in cur.fetchall():
    print(f"  - {row[0]} ({row[1]})")

conn.close()
