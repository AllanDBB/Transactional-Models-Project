from supabase import create_client, Client
import pyodbc

SUPABASE_URL = "https://vzcwfryxmtzocmjpayfz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ6Y3dmcnl4bXR6b2NtanBheWZ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI2NDg2NDQsImV4cCI6MjA3ODIyNDY0NH0.Azkwt-2uzwOVql0Cv-b0juvCK5ZPs7A-HT9QsPfGcWg"

MSSQL_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1434;"
    "DATABASE=MSSQL_DW;"
    "UID=sa;"
    "PWD=BasesDatos2!;"
    "TrustServerCertificate=yes;"
)

def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Falta algo de supa.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_dw_connection():
    conn = pyodbc.connect(MSSQL_CONNECTION_STRING)
    conn.autocommit = False
    return conn