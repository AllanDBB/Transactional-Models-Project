"""
Loader para Supabase - Carga datos de prueba en Supabase
Usa las tablas: users, products, orders, order_items
"""
import os
import sys
from datetime import datetime, timedelta
import random
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Configuración
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://vzcwfryxmtzocmjpayfz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ6Y3dmcnl4bXR6b2NtanBheWZ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI2NDg2NDQsImV4cCI6MjA3ODIyNDY0NH0.Azkwt-2uzwOVql0Cv-b0juvCK5ZPs7A-HT9QsPfGcWg")

# Datos de ejemplo
CLIENTES = [
    {"nombre": "Juan Pérez", "email": "juan.perez@example.com", "genero": "M", "pais": "Costa Rica"},
    {"nombre": "María García", "email": "maria.garcia@example.com", "genero": "F", "pais": "Costa Rica"},
    {"nombre": "Carlos López", "email": "carlos.lopez@example.com", "genero": "M", "pais": "México"},
    {"nombre": "Ana Martínez", "email": "ana.martinez@example.com", "genero": "F", "pais": "España"},
    {"nombre": "Luis Rodríguez", "email": "luis.rodriguez@example.com", "genero": "M", "pais": "Argentina"},
    {"nombre": "Sofia Torres", "email": "sofia.torres@example.com", "genero": "F", "pais": "Chile"},
    {"nombre": "Diego Ramírez", "email": "diego.ramirez@example.com", "genero": "M", "pais": "Colombia"},
    {"nombre": "Valentina Cruz", "email": "valentina.cruz@example.com", "genero": "F", "pais": "Perú"},
]

PRODUCTOS = [
    {"sku": "SUPA-LAPTOP-001", "nombre": "Laptop Dell XPS 15", "categoria": "Electrónica"},
    {"sku": "SUPA-PHONE-001", "nombre": "iPhone 14 Pro", "categoria": "Electrónica"},
    {"sku": "SUPA-AUDIO-001", "nombre": "Sony WH-1000XM5", "categoria": "Audio"},
    {"sku": "SUPA-MONITOR-001", "nombre": "Samsung 4K Monitor", "categoria": "Electrónica"},
    {"sku": "SUPA-MOUSE-001", "nombre": "Logitech MX Master 3", "categoria": "Accesorios"},
    {"sku": "SUPA-KEYBOARD-001", "nombre": "Mechanical Keyboard RGB", "categoria": "Accesorios"},
    {"sku": "SUPA-HUB-001", "nombre": "USB-C Hub 7-in-1", "categoria": "Accesorios"},
    {"sku": "SUPA-WEBCAM-001", "nombre": "Webcam HD 1080p", "categoria": "Electrónica"},
    {"sku": "SUPA-LAMP-001", "nombre": "Desk Lamp LED", "categoria": "Oficina"},
    {"sku": "SUPA-CHAIR-001", "nombre": "Ergonomic Chair", "categoria": "Muebles"},
]

def get_client():
    """Obtener cliente de Supabase"""
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def clear_tables(supabase: Client):
    """Limpiar todas las tablas"""
    print("🧹 Limpiando tablas...")
    
    try:
        # Orden correcto por FKs - usar gt para borrar todos (mayor que UUID mínimo)
        supabase.table("orden_detalle").delete().gt("orden_detalle_id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("orden").delete().gt("orden_id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("producto").delete().gt("producto_id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("cliente").delete().gt("cliente_id", "00000000-0000-0000-0000-000000000000").execute()
    except Exception as e:
        print(f"   ⚠️  Advertencia al limpiar: {e}")
        print("   Continuando de todas formas...")
    
    print("   ✅ Tablas limpiadas")

def load_clientes(supabase: Client):
    """Cargar clientes (genera 600 clientes reutilizando base)"""
    base_count = len(CLIENTES)
    target_count = 600
    print(f"👤 Cargando {target_count} clientes...")
    
    cliente_ids = []
    
    for i in range(target_count):
        cliente_base = CLIENTES[i % base_count]
        cliente = {
            "nombre": f"{cliente_base['nombre']} {i+1}",
            "email": f"{cliente_base['email'].split('@')[0]}.{i+1}@example.com",
            "genero": cliente_base["genero"],
            "pais": cliente_base["pais"]
        }
        data = {
            "nombre": cliente["nombre"],
            "email": cliente["email"],
            "genero": cliente["genero"],
            "pais": cliente["pais"]
        }
        
        
        response = supabase.table("cliente").insert(data).execute()
        if response.data:
            cliente_id = response.data[0]["cliente_id"]
            cliente_ids.append(cliente_id)
    
    print(f"   ✅ {len(cliente_ids)} clientes insertados")
    return cliente_ids

def load_productos(supabase: Client):
    """Cargar productos (genera 100 productos reutilizando base)"""
    base_count = len(PRODUCTOS)
    target_count = 100
    print(f"📦 Cargando {target_count} productos...")
    
    producto_ids = []
    
    for i in range(target_count):
        prod_base = PRODUCTOS[i % base_count]
        prod = {
            "sku": f"{prod_base['sku']}-{i+1}",
            "nombre": f"{prod_base['nombre']} v{i+1}",
            "categoria": prod_base["categoria"]
        }
        data = {
            "sku": prod["sku"],
            "nombre": prod["nombre"],
            "categoria": prod["categoria"]
        }
        
        
        response = supabase.table("producto").insert(data).execute()
        if response.data:
            producto_id = response.data[0]["producto_id"]
            producto_ids.append(producto_id)
    
    print(f"   ✅ {len(producto_ids)} productos insertados")
    return producto_ids

def load_ordenes(supabase: Client, cliente_ids, producto_ids, cantidad=20):
    """Cargar órdenes con items (optimizado con inserciones por lotes)"""
    print(f"🛒 Cargando {cantidad} órdenes...")
    
    # Valores según CHECK constraint orden_canal_check
    canales = ["WEB", "APP", "PARTNER"]
    monedas = ["USD", "CRC"]
    
    # Generar todas las órdenes primero (batch insert)
    ordenes_batch = []
    for i in range(cantidad):
        cliente_id = random.choice(cliente_ids)
        canal = random.choice(canales)
        moneda = random.choice(monedas)
        fecha = (datetime.now() - timedelta(days=random.randint(0, 730))).isoformat()  # 2 años
        total = round(random.uniform(100, 5000), 2)
        
        ordenes_batch.append({
            "cliente_id": cliente_id,
            "fecha": fecha,
            "canal": canal,
            "moneda": moneda,
            "total": total
        })
    
    # Insertar órdenes en lotes de 500 (límite de Supabase)
    print(f"   Insertando órdenes en lotes...")
    batch_size = 500
    orden_ids = []
    
    for i in range(0, len(ordenes_batch), batch_size):
        batch = ordenes_batch[i:i+batch_size]
        response = supabase.table("orden").insert(batch).execute()
        if response.data:
            orden_ids.extend([o["orden_id"] for o in response.data])
        print(f"   → {len(response.data)} órdenes insertadas ({i+len(batch)}/{cantidad})")
    
    print(f"   ✅ {len(orden_ids)} órdenes insertadas")
    
    # Generar items para las órdenes (2-5 items por orden)
    print(f"   Generando items...")
    items_batch = []
    for orden_id in orden_ids:
        num_items = random.randint(2, 5)
        selected_products = random.sample(producto_ids, min(num_items, len(producto_ids)))
        
        for producto_id in selected_products:
            items_batch.append({
                "orden_id": orden_id,
                "producto_id": producto_id,
                "cantidad": random.randint(1, 3),
                "precio_unit": round(random.uniform(50, 500), 2)
            })
    
    # Insertar items en lotes
    print(f"   Insertando {len(items_batch)} items en lotes...")
    for i in range(0, len(items_batch), batch_size):
        batch = items_batch[i:i+batch_size]
        supabase.table("orden_detalle").insert(batch).execute()
        print(f"   → {len(batch)} items insertados ({i+len(batch)}/{len(items_batch)})")
    
    print(f"   ✅ {len(items_batch)} items insertados")

def print_stats(supabase: Client):
    """Imprimir estadísticas"""
    print("\n" + "=" * 60)
    print("📊 RESUMEN")
    print("=" * 60)
    
    clientes = supabase.table("cliente").select("cliente_id", count="exact").execute()
    print(f"Clientes:   {clientes.count if clientes.count else 0}")
    
    productos = supabase.table("producto").select("producto_id", count="exact").execute()
    print(f"Productos:  {productos.count if productos.count else 0}")
    
    ordenes = supabase.table("orden").select("orden_id", count="exact").execute()
    print(f"Órdenes:    {ordenes.count if ordenes.count else 0}")
    
    items = supabase.table("orden_detalle").select("orden_detalle_id", count="exact").execute()
    print(f"Items:      {items.count if items.count else 0}")
    
    ordenes_data = supabase.table("orden").select("total").execute()
    total = sum(o["total"] for o in ordenes_data.data) if ordenes_data.data else 0
    print(f"Total:      ${total:,.2f}")
    
    print("=" * 60)

def main():
    """Función principal"""
    try:
        print("=" * 60)
        print("🚀 Supabase Data Loader")
        print("=" * 60)
        
        # Conectar
        supabase = get_client()
        print(f"✅ Conectado a Supabase: {SUPABASE_URL}\n")
        
        # Limpiar
        clear_tables(supabase)
        print()
        
        # Cargar datos (requisitos: ≥3000 clientes total, ≥500 productos total, ≥25000 órdenes total)
        cliente_ids = load_clientes(supabase)
        print()
        
        producto_ids = load_productos(supabase)
        print()
        
        load_ordenes(supabase, cliente_ids, producto_ids, cantidad=8000)
        print()
        
        # Estadísticas
        print_stats(supabase)
        
        print("\n✅ Carga completada exitosamente!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
