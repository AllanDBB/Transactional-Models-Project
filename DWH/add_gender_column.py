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

# Agregar columna gender a staging.supabase_users
cur.execute("""
    ALTER TABLE staging.supabase_users 
    ADD gender NVARCHAR(20) NULL
""")

conn.commit()
print("✓ Columna 'gender' agregada a staging.supabase_users")

conn.close()
