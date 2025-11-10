"""
Load Module - MongoDB ETL
Carga los datos transformados a MongoDB
"""

import os
from typing import List, Dict
from pymongo import MongoClient, errors
from pymongo.collection import Collection


def get_mongodb_connection():
    """
    Crea conexión a MongoDB
    """
    try:
        # Leer configuración desde variables de entorno
        mongo_host = os.getenv('MONGO_HOST', 'localhost')
        mongo_port = int(os.getenv('MONGO_PORT', '27017'))
        mongo_user = os.getenv('MONGO_ROOT_USERNAME', 'admin')
        mongo_password = os.getenv('MONGO_ROOT_PASSWORD', 'admin123')
        mongo_database = os.getenv('MONGO_DATABASE', 'transactional_db')
        
        # Construir URI de conexión
        connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
        
        # Crear cliente
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # Verificar conexión
        client.admin.command('ping')
        
        # Retornar base de datos
        return client[mongo_database]
        
    except errors.ServerSelectionTimeoutError:
        print("❌ Error: Cannot connect to MongoDB. Make sure the server is running.")
        raise
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        raise


def load_to_collection(collection: Collection, data: List[Dict], batch_size: int = 100) -> Dict:
    """
    Carga datos a una colección de MongoDB en batches
    """
    result = {
        'inserted': 0,
        'failed': 0,
        'errors': []
    }
    
    # Dividir en batches
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        
        try:
            # Insertar batch
            insert_result = collection.insert_many(batch, ordered=False)
            result['inserted'] += len(insert_result.inserted_ids)
            
        except errors.BulkWriteError as bwe:
            # Manejar errores parciales
            result['inserted'] += bwe.details['nInserted']
            result['failed'] += len(bwe.details['writeErrors'])
            result['errors'].extend(bwe.details['writeErrors'])
            
        except Exception as e:
            print(f"  ⚠ Error inserting batch: {e}")
            result['failed'] += len(batch)
            result['errors'].append(str(e))
    
    return result


def load_to_mongodb(data: List[Dict]) -> Dict:
    """
    Función principal de carga a MongoDB
    """
    if not data:
        print("  ⚠ No data to load")
        return {'inserted': 0, 'failed': 0, 'errors': []}
    
    try:
        # Conectar a MongoDB
        db = get_mongodb_connection()
        
        # Seleccionar o crear colección
        collection_name = os.getenv('MONGO_COLLECTION', 'etl_data')
        collection = db[collection_name]
        
        # Crear índices si no existen
        try:
            if 'id' in data[0]:
                collection.create_index('id', unique=True)
            collection.create_index('created_at')
            collection.create_index('processed_at')
        except Exception as e:
            print(f"  ⚠ Warning creating indexes: {e}")
        
        # Cargar datos
        print(f"  - Loading to collection: {collection_name}")
        result = load_to_collection(collection, data)
        
        # Mostrar estadísticas
        if result['failed'] > 0:
            print(f"  ⚠ {result['failed']} records failed to insert")
        
        return result
        
    except Exception as e:
        print(f"❌ Error in load process: {e}")
        raise


def upsert_to_mongodb(data: List[Dict], key_field: str = 'id') -> Dict:
    """
    Actualiza o inserta datos (upsert) basado en un campo clave
    """
    result = {
        'inserted': 0,
        'updated': 0,
        'failed': 0
    }
    
    try:
        db = get_mongodb_connection()
        collection_name = os.getenv('MONGO_COLLECTION', 'etl_data')
        collection = db[collection_name]
        
        for record in data:
            try:
                if key_field in record:
                    filter_query = {key_field: record[key_field]}
                    update_result = collection.replace_one(
                        filter_query,
                        record,
                        upsert=True
                    )
                    
                    if update_result.upserted_id:
                        result['inserted'] += 1
                    else:
                        result['updated'] += 1
                else:
                    collection.insert_one(record)
                    result['inserted'] += 1
                    
            except Exception as e:
                print(f"  ⚠ Error upserting record: {e}")
                result['failed'] += 1
        
        return result
        
    except Exception as e:
        print(f"❌ Error in upsert process: {e}")
        raise
