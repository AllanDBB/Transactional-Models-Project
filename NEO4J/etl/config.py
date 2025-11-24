"""
Configuración de conexiones para ETL Neo4j -> DWH MSSQL.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class NeoConfig:
    """Parámetros de conexión a Neo4j."""

    URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    USER = os.getenv("NEO4J_USER", "neo4j")
    PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
    DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


class DWConfig:
    """Parámetros del DWH MSSQL."""

    DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
    SERVER = os.getenv("MSSQL_DW_SERVER", "localhost")
    PORT = int(os.getenv("MSSQL_DW_PORT", "1434"))
    DATABASE = os.getenv("MSSQL_DW_DATABASE", "MSSQL_DW")
    USER = os.getenv("MSSQL_DW_USER", "sa")
    PASSWORD = os.getenv("MSSQL_DW_PASSWORD", "BasesDatos2!")

    @staticmethod
    def connection_string() -> str:
        return (
            f"Driver={DWConfig.DRIVER};"
            f"Server={DWConfig.SERVER},{DWConfig.PORT};"
            f"Database={DWConfig.DATABASE};"
            f"UID={DWConfig.USER};"
            f"PWD={DWConfig.PASSWORD}"
        )


class ETLConfig:
    LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("ETL_LOG_FILE", "neo4j_etl.log")
    SOURCE_SYSTEM = "NEO4J"
