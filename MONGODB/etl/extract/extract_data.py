"""
Extract Module - MongoDB ETL
Extrae datos de diferentes fuentes: CSV, JSON, APIs, otras BDs
"""

import os
import json
import pandas as pd
import requests
from typing import List, Dict


def extract_from_csv(file_path: str) -> List[Dict]:
    """
    Extrae datos desde archivo CSV
    """
    try:
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []


def extract_from_json(file_path: str) -> List[Dict]:
    """
    Extrae datos desde archivo JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        print(f"Error reading JSON: {e}")
        return []


def extract_from_api(url: str, params: Dict = None) -> List[Dict]:
    """
    Extrae datos desde una API REST
    """
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        print(f"Error fetching from API: {e}")
        return []


def extract_from_mysql() -> List[Dict]:
    """
    Extrae datos desde MySQL
    """
    try:
        from sqlalchemy import create_engine
        
        # Configurar conexión
        engine = create_engine(
            f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:"
            f"{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}"
            f":{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
        )
        
        # Ejecutar query
        query = "SELECT * FROM your_table LIMIT 1000"
        df = pd.read_sql(query, engine)
        
        return df.to_dict('records')
    except Exception as e:
        print(f"Error extracting from MySQL: {e}")
        return []


def extract_from_sources() -> List[Dict]:
    """
    Función principal de extracción
    Combina datos de múltiples fuentes
    """
    all_data = []
    
    # Directorio de datos
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    
    # Extraer desde CSV si existe
    csv_file = os.path.join(data_dir, 'sample_data.csv')
    if os.path.exists(csv_file):
        print(f"  - Extracting from CSV: {csv_file}")
        all_data.extend(extract_from_csv(csv_file))
    
    # Extraer desde JSON si existe
    json_file = os.path.join(data_dir, 'sample_data.json')
    if os.path.exists(json_file):
        print(f"  - Extracting from JSON: {json_file}")
        all_data.extend(extract_from_json(json_file))
    
    # Ejemplo: Extraer desde API pública
    # all_data.extend(extract_from_api('https://api.example.com/data'))
    
    # Si no hay datos, crear datos de ejemplo
    if not all_data:
        print("  - No source files found, creating sample data...")
        all_data = create_sample_data()
    
    return all_data


def create_sample_data() -> List[Dict]:
    """
    Crea datos de ejemplo para testing
    """
    from datetime import datetime, timedelta
    import random
    
    sample_data = []
    
    for i in range(100):
        sample_data.append({
            'id': i + 1,
            'name': f'Product {i + 1}',
            'category': random.choice(['Electronics', 'Books', 'Clothing', 'Food']),
            'price': round(random.uniform(10, 500), 2),
            'stock': random.randint(0, 200),
            'supplier': f'Supplier {random.randint(1, 10)}',
            'created_at': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            'active': random.choice([True, False])
        })
    
    return sample_data
