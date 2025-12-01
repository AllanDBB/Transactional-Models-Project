"""
Loader para MongoDB - Carga datos de prueba en MongoDB
Usa los modelos de Mongoose definidos en MONGODB/server/src/models
"""
import os
import sys
from datetime import datetime, timedelta
import random
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv

load_dotenv()

# Configuración
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://UsuarioBases2:BasesPassword@cluster0.mmx82lq.mongodb.net/SalesMongoDB-API?appName=Cluster0")
DB_NAME = "SalesMongoDB-API"

# Datos de ejemplo
NOMBRES = ["Juan Pérez", "María García", "Carlos López", "Ana Martínez", "Luis Rodríguez", 
           "Carmen Fernández", "José González", "Isabel Sánchez", "Francisco Morales", "Laura Jiménez"]
EMAILS = ["juan@example.com", "maria@example.com", "carlos@example.com", "ana@example.com", "luis@example.com",
          "carmen@example.com", "jose@example.com", "isabel@example.com", "francisco@example.com", "laura@example.com"]
GENEROS = ["Masculino", "Femenino", "Otro"]
CANALES = ["Online", "Tienda", "Telefono"]

PRODUCTOS = [
    {"codigo_mongo": "MN-001", "nombre": "Laptop HP", "categoria": "Electrónica", "sku": "SKU-001", "alt": "ALT-001"},
    {"codigo_mongo": "MN-002", "nombre": "Mouse Logitech", "categoria": "Accesorios", "sku": "SKU-002", "alt": "ALT-002"},
    {"codigo_mongo": "MN-003", "nombre": "Teclado Mecánico", "categoria": "Accesorios", "sku": "SKU-003", "alt": "ALT-003"},
    {"codigo_mongo": "MN-004", "nombre": "Monitor Samsung", "categoria": "Electrónica", "sku": "SKU-004", "alt": "ALT-004"},
    {"codigo_mongo": "MN-005", "nombre": "Silla Ergonómica", "categoria": "Muebles", "sku": "SKU-005", "alt": "ALT-005"},
    {"codigo_mongo": "MN-006", "nombre": "Escritorio", "categoria": "Muebles", "sku": "SKU-006", "alt": "ALT-006"},
    {"codigo_mongo": "MN-007", "nombre": "Audífonos Sony", "categoria": "Audio", "sku": "SKU-007", "alt": "ALT-007"},
    {"codigo_mongo": "MN-008", "nombre": "Webcam Logitech", "categoria": "Accesorios", "sku": "SKU-008", "alt": "ALT-008"},
    {"codigo_mongo": "MN-009", "nombre": "Impresora HP", "categoria": "Electrónica", "sku": "SKU-009", "alt": "ALT-009"},
    {"codigo_mongo": "MN-010", "nombre": "Lámpara LED", "categoria": "Iluminación", "sku": "SKU-010", "alt": "ALT-010"},
]

def get_client():
    """Conectar a MongoDB"""
    return MongoClient(MONGODB_URI)

def clear_collections(db):
    """Limpiar todas las colecciones"""
    print("🧹 Limpiando colecciones...")
    db.clientes.delete_many({})
    db.productos.delete_many({})
    db.ordens.delete_many({})
    db.orden_items.delete_many({})
    print("   ✅ Colecciones limpiadas")

def load_clientes(db, cantidad=10):
    """Cargar clientes"""
    print(f"👤 Cargando {cantidad} clientes...")
    clientes = []
    
    for i in range(cantidad):
        cliente = {
            "nombre": NOMBRES[i % len(NOMBRES)],
            "email": f"cliente{i+1}@example.com" if i >= len(EMAILS) else EMAILS[i],
            "genero": random.choice(GENEROS),
            "preferencias": {
                "canal": random.choice(CANALES)
            },
            "creado": datetime.now() - timedelta(days=random.randint(30, 365))
        }
        clientes.append(cliente)
    
    result = db.clientes.insert_many(clientes)
    print(f"   ✅ {len(result.inserted_ids)} clientes insertados")
    return result.inserted_ids

def load_productos(db, cantidad=100):
    """Cargar productos (genera cantidad especificada reutilizando base)"""
    base_count = len(PRODUCTOS)
    print(f"📦 Cargando {cantidad} productos...")
    productos = []
    
    for i in range(cantidad):
        p_base = PRODUCTOS[i % base_count]
        producto = {
            "codigo_mongo": f"{p_base['codigo_mongo']}-{i+1}",
            "nombre": f"{p_base['nombre']} v{i+1}",
            "categoria": p_base["categoria"],
            "equivalencias": {
                "sku": f"{p_base['sku']}-{i+1}",
                "alt": f"{p_base['alt']}-{i+1}"
            }
        }
        productos.append(producto)
    
    result = db.productos.insert_many(productos)
    print(f"   ✅ {len(result.inserted_ids)} productos insertados")
    return result.inserted_ids

def load_ordenes(db, cliente_ids, producto_ids, cantidad=20):
    """Cargar órdenes con items embebidos"""
    print(f"🛒 Cargando {cantidad} órdenes...")
    ordenes = []
    
    for i in range(cantidad):
        # Seleccionar cliente
        client_id = random.choice(cliente_ids)
        
        # Generar items (1-4 items por orden)
        num_items = random.randint(1, 4)
        items = []
        total = 0
        
        for _ in range(num_items):
            producto_id = random.choice(producto_ids)
            cantidad_item = random.randint(1, 3)
            # Precios en CRC (enteros)
            precio_unit = random.randint(5000, 50000)
            
            items.append({
                "producto_id": producto_id,
                "cantidad": cantidad_item,
                "precio_unit": precio_unit
            })
            total += cantidad_item * precio_unit
        
        # Crear orden
        orden = {
            "client_id": client_id,
            "fecha": datetime.now() - timedelta(days=random.randint(0, 90)),
            "canal": random.choice(CANALES),
            "moneda": "CRC",  # Solo CRC según spec
            "total": total,  # Entero en CRC
            "items": items,
            "metadatos": {
                "cupon": f"CUPON{random.randint(1, 100)}" if random.random() > 0.7 else None
            }
        }
        ordenes.append(orden)
    
    result = db.ordens.insert_many(ordenes)
    print(f"   ✅ {len(result.inserted_ids)} órdenes insertadas con items embebidos")
    return result.inserted_ids

def main():
    """Función principal"""
    try:
        print("=" * 60)
        print("🚀 MongoDB Data Loader")
        print("=" * 60)
        
        # Conectar
        client = get_client()
        db = client[DB_NAME]
        
        # Verificar conexión
        db.command("ping")
        print(f"✅ Conectado a MongoDB: {DB_NAME}\n")
        
        # Limpiar
        clear_collections(db)
        print()
        
        # Cargar datos (requisitos: ≥3000 clientes total, ≥500 productos total, ≥25000 órdenes total)
        # MongoDB: 500 clientes, 100 productos, 5000 órdenes
        cliente_ids = load_clientes(db, cantidad=500)
        print()
        
        producto_ids = load_productos(db, cantidad=100)
        print()
        
        orden_ids = load_ordenes(db, cliente_ids, producto_ids, cantidad=5000)
        print()
        
        # Resumen
        print("=" * 60)
        print("📊 RESUMEN")
        print("=" * 60)
        print(f"Clientes:  {db.clientes.count_documents({})}")
        print(f"Productos: {db.productos.count_documents({})}")
        print(f"Órdenes:   {db.ordens.count_documents({})}")
        print("=" * 60)
        print("✅ Carga completada exitosamente!")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
