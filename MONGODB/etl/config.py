"""
Configuración de conexiones y parámetros del ETL de MongoDB -> DWH MSSQL.
Lee variables de entorno desde .env y expone helpers para obtener cadenas de conexión.
"""
import os
from typing import Dict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class MongoConfig:
    """Parámetros de conexión a MongoDB transaccional."""

    # Para Atlas, puedes definir MONGO_URI completo (mongodb+srv://...); si no, usa host/port/user/pass.
    URI = os.getenv("MONGO_URI")
    HOST = os.getenv("MONGO_HOST", "localhost")
    PORT = int(os.getenv("MONGO_PORT", "27017"))
    USER = os.getenv("MONGO_ROOT_USERNAME", "admin")
    PASSWORD = os.getenv("MONGO_ROOT_PASSWORD", "admin123")
    DATABASE = os.getenv("MONGO_DATABASE", "transactional_db")

    @staticmethod
    def uri() -> str:
        """Construye el URI de conexión."""
        if MongoConfig.URI:
            return MongoConfig.URI
        user = MongoConfig.USER
        pwd = MongoConfig.PASSWORD
        return f"mongodb://{user}:{pwd}@{MongoConfig.HOST}:{MongoConfig.PORT}/"


class DWConfig:
    """Parámetros para conectarse al Data Warehouse SQL Server."""

    DB = {
        "driver": os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        "server": os.getenv("MSSQL_DW_SERVER", "localhost"),
        "port": int(os.getenv("MSSQL_DW_PORT", "1434")),
        "database": os.getenv("MSSQL_DW_DATABASE", "MSSQL_DW"),
        "uid": os.getenv("MSSQL_DW_USER", "sa"),
        "pwd": os.getenv("MSSQL_DW_PASSWORD", "BasesDatos2!"),
    }

    @staticmethod
    def connection_string() -> str:
        """Cadena de conexión ODBC."""
        cfg = DWConfig.DB
        return (
            f"Driver={cfg['driver']};"
            f"Server={cfg['server']},{cfg['port']};"
            f"Database={cfg['database']};"
            f"UID={cfg['uid']};"
            f"PWD={cfg['pwd']}"
        )


class ETLConfig:
    """Parámetros generales del ETL."""

    LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("ETL_LOG_FILE", "mongodb_etl.log")
    BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "1000"))
    SOURCE_SYSTEM = "MONGODB"
