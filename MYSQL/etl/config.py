"""
Configuracion de conexiones a bases de datos MySQL
Lee de variables de entorno (.env file)
Permite conexiones locales y remotas (multi-equipo)
"""
import os
from typing import Dict

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ImportError:
    pass

class DatabaseConfig:
    """Configuracion para conectarse a MySQL"""

    # Base de datos transaccional MySQL (fuente)
    SOURCE_DB = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'database': os.getenv('MYSQL_DATABASE', 'transactional_db'),
        'user': os.getenv('MYSQL_USER', 'user'),
        'password': os.getenv('MYSQL_PASSWORD', 'user123')
    }

    # Data Warehouse MSSQL (destino)
    DW_DB = {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': os.getenv('MSSQL_DW_SERVER', 'localhost'),
        'port': int(os.getenv('MSSQL_DW_PORT', '1434')),
        'database': 'MSSQL_DW',
        'uid': os.getenv('MSSQL_DW_USER', 'admin'),
        'pwd': os.getenv('MSSQL_DW_PASSWORD', 'admin123')
    }

    @staticmethod
    def get_source_connection_params() -> Dict:
        """Retorna parametros de conexion para MySQL"""
        return DatabaseConfig.SOURCE_DB

    @staticmethod
    def get_dw_connection_string() -> str:
        """Retorna la cadena de conexion para el DWH (MSSQL)"""
        config = DatabaseConfig.DW_DB
        return (f"Driver={config['driver']};"
                f"Server={config['server']},{config['port']};"
                f"Database={config['database']};"
                f"UID={config['uid']};"
                f"PWD={config['pwd']}")


class ETLConfig:
    """Configuracion general del ETL"""

    BATCH_SIZE = 1000
    LOG_LEVEL = 'INFO'
    LOG_FILE = 'etl_mysql_process.log'
    MAX_ERRORS = 100
