"""
Script para limpiar las 3 bases de datos (MongoDB, Neo4j, Supabase)
antes de hacer una carga limpia de datos
"""
import os
import sys
from pymongo import MongoClient
from neo4j import GraphDatabase
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Configuración MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB_NAME = "SalesMongoDB-API"

# Configuración Neo4j
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Configuración Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def clear_mongodb():
    """Limpiar MongoDB"""
    print("\n🔥 Limpiando MongoDB...")
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGO_DB_NAME]
        
        db.clientes.delete_many({})
        db.productos.delete_many({})
        db.ordens.delete_many({})
        db.orden_items.delete_many({})
        
        print("   ✅ MongoDB limpiado")
        client.close()
    except Exception as e:
        print(f"   ❌ Error limpiando MongoDB: {e}")

def clear_neo4j():
    """Limpiar Neo4j"""
    print("\n🔥 Limpiando Neo4j...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("   ✅ Neo4j limpiado")
        driver.close()
    except Exception as e:
        print(f"   ❌ Error limpiando Neo4j: {e}")

def clear_supabase():
    """Limpiar Supabase"""
    print("\n🔥 Limpiando Supabase...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Orden correcto por FKs
        supabase.table("orden_detalle").delete().neq("orden_detalle_id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("orden").delete().neq("orden_id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("producto").delete().neq("producto_id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("cliente").delete().neq("cliente_id", "00000000-0000-0000-0000-000000000000").execute()
        
        print("   ✅ Supabase limpiado")
    except Exception as e:
        print(f"   ❌ Error limpiando Supabase: {e}")

def main():
    print("=" * 60)
    print("🧹 LIMPIEZA DE BASES DE DATOS")
    print("=" * 60)
    print("⚠️  Esto eliminará TODOS los datos de:")
    print("   - MongoDB (clientes, productos, órdenes)")
    print("   - Neo4j (todos los nodos y relaciones)")
    print("   - Supabase (clientes, productos, órdenes)")
    print()
    
    confirm = input("¿Estás seguro? (escribe 'SI' para continuar): ")
    if confirm.upper() != "SI":
        print("\n❌ Operación cancelada")
        sys.exit(0)
    
    clear_mongodb()
    clear_neo4j()
    clear_supabase()
    
    print("\n" + "=" * 60)
    print("✅ Limpieza completada!")
    print("=" * 60)
    print("\nAhora puedes ejecutar:")
    print("  python load_mongodb.py")
    print("  python load_neo4j.py")
    print("  python load_supabase.py")

if __name__ == "__main__":
    main()
