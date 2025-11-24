"""
Configuración de conexiones a bases de datos
Lee de variables de entorno (.env file)
Permite conexiones locales y remotas (multi-equipo)
"""
import os
from typing import Dict

# Intentar cargar variables de .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Si no está instalado python-dotenv, usar solo variables de entorno

class DatabaseConfig:
    """Configuración para conectarse a SQL Server"""
    
    # Base de datos transaccional (fuente)
    # Valores por defecto para conexión local, sobrescribibles con .env
    SOURCE_DB = {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': os.getenv('MSSQL_SERVER', 'localhost'),
        'port': int(os.getenv('MSSQL_PORT', '1433')),
        'database': 'SalesDB_MSSQL',
        'uid': os.getenv('MSSQL_USER', 'sa'),
        'pwd': os.getenv('MSSQL_PASSWORD', 'BasesDatos2!')
    }
    
    # Data Warehouse (destino)
    # Para multi-equipo: usar DW_PORT=1434 (puerto expuesto en docker-compose)
    DW_DB = {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': os.getenv('MSSQL_DW_SERVER', 'localhost'),
        'port': int(os.getenv('MSSQL_DW_PORT', '1434')),  # Puerto DWH (1434)
        'database': 'MSSQL_DW',
        'uid': os.getenv('MSSQL_DW_USER', 'sa'),
        'pwd': os.getenv('MSSQL_DW_PASSWORD', 'BasesDatos2!')
    }
    
    @staticmethod
    def get_source_connection_string() -> str:
        """Retorna la cadena de conexión para la BD transaccional"""
        config = DatabaseConfig.SOURCE_DB
        return (f"Driver={config['driver']};"
                f"Server={config['server']},{config['port']};"
                f"Database={config['database']};"
                f"UID={config['uid']};"
                f"PWD={config['pwd']}")
    
    @staticmethod
    def get_dw_connection_string() -> str:
        """Retorna la cadena de conexión para el DWH"""
        config = DatabaseConfig.DW_DB
        return (f"Driver={config['driver']};"
                f"Server={config['server']},{config['port']};"
                f"Database={config['database']};"
                f"UID={config['uid']};"
                f"PWD={config['pwd']}")


class ETLConfig:
    """Configuración general del ETL"""
    
    # Tamaño de batch para procesar datos
    BATCH_SIZE = 1000
    
    # Logging
    LOG_LEVEL = 'INFO'
    LOG_FILE = 'etl_process.log'
    
    # Tolerancia de errores (cantidad máxima antes de fallar)
    MAX_ERRORS = 100
