"""
Loader para Neo4j - Carga datos de prueba en Neo4j
Crea nodos y relaciones según el modelo de grafo del proyecto
"""
import os
import sys
from datetime import datetime, timedelta
import random
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# Configuración de Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://b83602fd.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "b-i-T9n9xKzCc9c5BIPcc72yLufOeK_TysnSW19UMmo")

# Datos de ejemplo
CLIENTES = [
    {"id": "CL-001", "nombre": "Juan Pérez", "email": "juan@example.com", "pais": "CR"},
    {"id": "CL-002", "nombre": "María García", "email": "maria@example.com", "pais": "CR"},
    {"id": "CL-003", "nombre": "Carlos López", "email": "carlos@example.com", "pais": "CR"},
    {"id": "CL-004", "nombre": "Ana Martínez", "email": "ana@example.com", "pais": "MX"},
    {"id": "CL-005", "nombre": "Luis Rodríguez", "email": "luis@example.com", "pais": "CR"},
]

CATEGORIAS = [
    {"nombre": "Electrónica"},
    {"nombre": "Accesorios"},
    {"nombre": "Muebles"},
    {"nombre": "Audio"},
    {"nombre": "Iluminación"},
]

PRODUCTOS = [
    {"id": "PROD-001", "codigo": "SKU-001", "nombre": "Laptop HP", "precio": 1200.00, "categoria": "Electrónica"},
    {"id": "PROD-002", "codigo": "SKU-002", "nombre": "Mouse Logitech", "precio": 25.00, "categoria": "Accesorios"},
    {"id": "PROD-003", "codigo": "SKU-003", "nombre": "Teclado Mecánico", "precio": 80.00, "categoria": "Accesorios"},
    {"id": "PROD-004", "codigo": "SKU-004", "nombre": "Monitor Samsung", "precio": 300.00, "categoria": "Electrónica"},
    {"id": "PROD-005", "codigo": "SKU-005", "nombre": "Silla Ergonómica", "precio": 250.00, "categoria": "Muebles"},
    {"id": "PROD-006", "codigo": "SKU-006", "nombre": "Escritorio", "precio": 400.00, "categoria": "Muebles"},
    {"id": "PROD-007", "codigo": "SKU-007", "nombre": "Audífonos Sony", "precio": 150.00, "categoria": "Audio"},
    {"id": "PROD-008", "codigo": "SKU-008", "nombre": "Webcam Logitech", "precio": 60.00, "categoria": "Accesorios"},
]

def get_driver():
    """Conectar a Neo4j"""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def clear_database(session):
    """Limpiar toda la base de datos"""
    print("🧹 Limpiando base de datos...")
    session.run("MATCH (n) DETACH DELETE n")
    print("   ✅ Base de datos limpiada")

def create_constraints(session):
    """Crear constraints e índices"""
    print("🔧 Creando constraints e índices...")
    
    constraints = [
        "CREATE CONSTRAINT cliente_id IF NOT EXISTS FOR (c:Cliente) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT producto_id IF NOT EXISTS FOR (p:Producto) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT categoria_nombre IF NOT EXISTS FOR (c:Categoria) REQUIRE c.nombre IS UNIQUE",
        "CREATE INDEX orden_fecha IF NOT EXISTS FOR (o:Orden) ON (o.fecha)",
    ]
    
    for cypher in constraints:
        session.run(cypher)
    
    print("   ✅ Constraints e índices creados")

def load_clientes(session):
    """Cargar nodos Cliente"""
    print(f"👤 Cargando {len(CLIENTES)} clientes...")
    
    for c in CLIENTES:
        session.run("""
            CREATE (c:Cliente {
                id: $id,
                nombre: $nombre,
                email: $email,
                pais: $pais,
                fecha_registro: datetime()
            })
        """, **c)
    
    print(f"   ✅ {len(CLIENTES)} clientes creados")

def load_categorias(session):
    """Cargar nodos Categoría"""
    print(f"📂 Cargando {len(CATEGORIAS)} categorías...")
    
    for cat in CATEGORIAS:
        session.run("""
            CREATE (c:Categoria {nombre: $nombre})
        """, **cat)
    
    print(f"   ✅ {len(CATEGORIAS)} categorías creadas")

def load_productos(session):
    """Cargar nodos Producto y relaciones con Categoría"""
    print(f"📦 Cargando {len(PRODUCTOS)} productos...")
    
    for p in PRODUCTOS:
        session.run("""
            MATCH (cat:Categoria {nombre: $categoria})
            CREATE (p:Producto {
                id: $id,
                codigo: $codigo,
                nombre: $nombre,
                precio: $precio
            })
            CREATE (p)-[:PERTENECE_A]->(cat)
        """, **p)
    
    print(f"   ✅ {len(PRODUCTOS)} productos creados con relaciones PERTENECE_A")

def create_equivalencias(session):
    """Crear relaciones de equivalencia entre productos"""
    print("🔗 Creando relaciones de equivalencia...")
    
    # Ejemplo: SKU-001 equivale a ALT-001
    equivalencias = [
        ("PROD-001", "PROD-002"),  # Laptop y Mouse son complementarios
        ("PROD-003", "PROD-004"),  # Teclado y Monitor
    ]
    
    for prod1, prod2 in equivalencias:
        session.run("""
            MATCH (p1:Producto {id: $prod1})
            MATCH (p2:Producto {id: $prod2})
            CREATE (p1)-[:EQUIVALE_A]->(p2)
            CREATE (p2)-[:EQUIVALE_A]->(p1)
        """, prod1=prod1, prod2=prod2)
    
    print(f"   ✅ {len(equivalencias)} relaciones EQUIVALE_A creadas")

def load_ordenes(session, cantidad=15):
    """Cargar órdenes y relaciones"""
    print(f"🛒 Cargando {cantidad} órdenes...")
    
    orden_count = 0
    
    for i in range(cantidad):
        # Seleccionar cliente aleatorio
        cliente = random.choice(CLIENTES)
        
        # Crear orden
        orden_id = f"ORD-{i+1:04d}"
        fecha = datetime.now() - timedelta(days=random.randint(0, 90))
        canal = random.choice(["WEB", "TIENDA", "APP"])
        
        session.run("""
            MATCH (c:Cliente {id: $cliente_id})
            CREATE (o:Orden {
                id: $orden_id,
                fecha: $fecha,
                canal: $canal,
                moneda: 'USD'
            })
            CREATE (c)-[:REALIZO]->(o)
        """, cliente_id=cliente["id"], orden_id=orden_id, fecha=fecha, canal=canal)
        
        # Agregar productos a la orden (1-3 productos)
        num_productos = random.randint(1, 3)
        productos_seleccionados = random.sample(PRODUCTOS, num_productos)
        
        for prod in productos_seleccionados:
            cantidad = random.randint(1, 3)
            precio_unit = prod["precio"]
            
            session.run("""
                MATCH (o:Orden {id: $orden_id})
                MATCH (p:Producto {id: $producto_id})
                CREATE (o)-[:CONTIENE {
                    cantidad: $cantidad,
                    precio_unit: $precio_unit
                }]->(p)
            """, orden_id=orden_id, producto_id=prod["id"], cantidad=cantidad, precio_unit=precio_unit)
        
        orden_count += 1
    
    print(f"   ✅ {orden_count} órdenes creadas con relaciones REALIZO y CONTIENE")

def print_stats(session):
    """Imprimir estadísticas"""
    print("\n" + "=" * 60)
    print("📊 RESUMEN")
    print("=" * 60)
    
    result = session.run("MATCH (c:Cliente) RETURN count(c) as total")
    print(f"Clientes:   {result.single()['total']}")
    
    result = session.run("MATCH (p:Producto) RETURN count(p) as total")
    print(f"Productos:  {result.single()['total']}")
    
    result = session.run("MATCH (cat:Categoria) RETURN count(cat) as total")
    print(f"Categorías: {result.single()['total']}")
    
    result = session.run("MATCH (o:Orden) RETURN count(o) as total")
    print(f"Órdenes:    {result.single()['total']}")
    
    result = session.run("MATCH ()-[r]->() RETURN count(r) as total")
    print(f"Relaciones: {result.single()['total']}")
    
    print("=" * 60)

def main():
    """Función principal"""
    try:
        print("=" * 60)
        print("🚀 Neo4j Data Loader")
        print("=" * 60)
        
        # Conectar
        driver = get_driver()
        
        with driver.session() as session:
            # Verificar conexión
            session.run("RETURN 1")
            print("✅ Conectado a Neo4j\n")
            
            # Limpiar
            clear_database(session)
            print()
            
            # Crear constraints
            create_constraints(session)
            print()
            
            # Cargar datos
            load_clientes(session)
            print()
            
            load_categorias(session)
            print()
            
            load_productos(session)
            print()
            
            create_equivalencias(session)
            print()
            
            load_ordenes(session, cantidad=15)
            print()
            
            # Estadísticas
            print_stats(session)
        
        driver.close()
        print("\n✅ Carga completada exitosamente!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
