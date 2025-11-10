"""
Load Module - MySQL ETL
"""

import os
import mysql.connector
from typing import List, Dict
from sqlalchemy import create_engine, text
import pandas as pd


def get_mysql_connection():
    """Crea conexión a MySQL"""
    try:
        host = os.getenv('MYSQL_HOST', 'localhost')
        port = int(os.getenv('MYSQL_PORT', '3306'))
        user = os.getenv('MYSQL_USER', 'user')
        password = os.getenv('MYSQL_PASSWORD', 'user123')
        database = os.getenv('MYSQL_DATABASE', 'transactional_db')
        
        engine = create_engine(
            f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
        )
        
        return engine
    except Exception as e:
        print(f"❌ Error connecting to MySQL: {e}")
        raise


def load_to_mysql(data: List[Dict]) -> Dict:
    """Carga datos a MySQL"""
    if not data:
        return {'inserted': 0, 'failed': 0}
    
    try:
        engine = get_mysql_connection()
        df = pd.DataFrame(data)
        
        # Cargar a tabla
        table_name = os.getenv('MYSQL_TABLE', 'etl_data')
        df.to_sql(table_name, engine, if_exists='append', index=False)
        
        return {'inserted': len(data), 'failed': 0}
    except Exception as e:
        print(f"❌ Error loading to MySQL: {e}")
        raise
