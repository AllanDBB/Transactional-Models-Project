"""
Configuracion de conexiones a bases de datos MySQL
Lee de variables de entorno (.env file en MYSQL/)
Permite conexiones locales y remotas (multi-equipo)
"""
import os
from typing import Dict
from pathlib import Path

try:
    from dotenv import load_dotenv  # type: ignore
    # Cargar .env desde el directorio MYSQL/
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    pass

class DatabaseConfig:
    """Configuracion para conectarse a MySQL"""

    # Base de datos transaccional MySQL (fuente)
    SOURCE_DB = {
        'host': os.getenv('MYSQL_HOST', 'mysql-transactional'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'database': os.getenv('MYSQL_DATABASE', 'sales_mysql'),
        'user': os.getenv('MYSQL_USER', 'user'),
        'password': os.getenv('MYSQL_PASSWORD', 'user123')
    }

    # Data Warehouse MSSQL (destino)
    DW_DB = {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': os.getenv('MSSQL_DW_SERVER', 'localhost'),
        'port': int(os.getenv('MSSQL_DW_PORT', '1434')),
        'database': 'MSSQL_DW',
        'uid': os.getenv('MSSQL_DW_USER', 'sa'),
        'pwd': os.getenv('MSSQL_DW_PASSWORD', 'BasesDatos2!')
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

    # Crear directorio de logs si no existe
    LOG_DIR = Path(__file__).parent / 'logs'
    LOG_DIR.mkdir(exist_ok=True)

    # Archivo de log con timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    LOG_FILE = str(LOG_DIR / f'etl_mysql_{timestamp}.log')

    MAX_ERRORS = 100
    DEFAULT_CRC_USD_RATE = 515.0  # Fallback si ExchangeRateHelper falla
