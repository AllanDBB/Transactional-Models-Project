"""
Transform Module - MongoDB ETL
Limpia, valida y transforma los datos
"""

from typing import List, Dict
from datetime import datetime
import re


def clean_string(value: str) -> str:
    """
    Limpia strings: trim, normaliza espacios
    """
    if not isinstance(value, str):
        return value
    return re.sub(r'\s+', ' ', value.strip())


def validate_email(email: str) -> bool:
    """
    Valida formato de email
    """
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))


def normalize_date(date_value) -> datetime:
    """
    Normaliza diferentes formatos de fecha
    """
    if isinstance(date_value, datetime):
        return date_value
    
    if isinstance(date_value, str):
        try:
            return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
        except:
            try:
                return datetime.strptime(date_value, '%Y-%m-%d')
            except:
                return datetime.now()
    
    return datetime.now()


def transform_record(record: Dict) -> Dict:
    """
    Transforma un registro individual
    """
    transformed = {}
    
    # Limpiar strings
    for key, value in record.items():
        if isinstance(value, str):
            transformed[key] = clean_string(value)
        else:
            transformed[key] = value
    
    # Normalizar fechas
    if 'created_at' in transformed:
        transformed['created_at'] = normalize_date(transformed['created_at'])
    else:
        transformed['created_at'] = datetime.now()
    
    # Agregar timestamp de procesamiento
    transformed['processed_at'] = datetime.now()
    
    # Agregar metadata
    transformed['metadata'] = {
        'source': 'ETL_Process',
        'version': '1.0',
        'validated': True
    }
    
    # Validaciones específicas
    if 'email' in transformed and not validate_email(str(transformed['email'])):
        transformed['email'] = None
        transformed['metadata']['validated'] = False
    
    # Normalizar valores numéricos
    if 'price' in transformed:
        try:
            transformed['price'] = float(transformed['price'])
        except:
            transformed['price'] = 0.0
    
    if 'stock' in transformed:
        try:
            transformed['stock'] = int(transformed['stock'])
        except:
            transformed['stock'] = 0
    
    return transformed


def transform_data(raw_data: List[Dict]) -> List[Dict]:
    """
    Transforma un conjunto de datos
    """
    transformed_data = []
    
    for record in raw_data:
        try:
            transformed_record = transform_record(record)
            transformed_data.append(transformed_record)
        except Exception as e:
            print(f"  ⚠ Error transforming record: {e}")
            continue
    
    # Eliminar duplicados basados en 'id' si existe
    if transformed_data and 'id' in transformed_data[0]:
        seen_ids = set()
        unique_data = []
        for record in transformed_data:
            if record['id'] not in seen_ids:
                seen_ids.add(record['id'])
                unique_data.append(record)
        transformed_data = unique_data
    
    return transformed_data
