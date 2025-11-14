#!/usr/bin/env python3
"""Cargar datos de prueba en BD transaccional MSSQL"""

import pyodbc
from config import DatabaseConfig
from datetime import datetime, timedelta
import random

conn = pyodbc.connect(DatabaseConfig.get_source_connection_string())
cursor = conn.cursor()

print("=" * 80)
print("[CARGANDO DATOS DE PRUEBA EN MSSQL TRANSACCIONAL]")
print("=" * 80)

# 1. Insertar clientes
print("\n[1] Insertando clientes...")
clientes = [
    ("Juan Pérez", "juan@example.com", "Masculino", "Costa Rica"),
    ("María García", "maria@example.com", "Femenino", "Costa Rica"),
    ("Carlos López", "carlos@example.com", "Masculino", "Panama"),
    ("Ana Martínez", "ana@example.com", "Femenino", "Costa Rica"),
    ("Luis Fernández", "luis@example.com", "Masculino", "Nicaragua"),
    ("Sofia Rodríguez", "sofia@example.com", "Femenino", "Costa Rica"),
    ("Diego Gómez", "diego@example.com", "Masculino", "El Salvador"),
    ("Laura Torres", "laura@example.com", "Femenino", "Costa Rica"),
    ("Miguel Soto", "miguel@example.com", "Masculino", "Honduras"),
    ("Patricia Ramírez", "patricia@example.com", "Femenino", "Costa Rica"),
]

for nombre, email, genero, pais in clientes:
    try:
        cursor.execute("""
            INSERT INTO sales_ms.Cliente (Nombre, Email, Genero, Pais, FechaRegistro)
            VALUES (?, ?, ?, ?, GETDATE())
        """, (nombre, email, genero, pais))
    except pyodbc.IntegrityError:
        print(f"  - {email} ya existe, omitiendo")

conn.commit()
print(f"  OK: Cargados {len(clientes)} clientes")

# 2. Insertar productos
print("\n[2] Insertando productos...")
productos = [
    ("SKU001", "Laptop Dell 15\"", "Computadoras"),
    ("SKU002", "Mouse Inalámbrico", "Accesorios"),
    ("SKU003", "Teclado Mecánico RGB", "Accesorios"),
    ("SKU004", "Monitor LG 24\"", "Monitores"),
    ("SKU005", "Webcam HD 1080p", "Accesorios"),
    ("SKU006", "Auriculares Sony WH-CH", "Audio"),
    ("SKU007", "Mousepad XL", "Accesorios"),
    ("SKU008", "Cable HDMI 2m", "Cables"),
    ("SKU009", "SSD Samsung 500GB", "Almacenamiento"),
    ("SKU010", "RAM DDR4 16GB", "Memoria"),
]

for sku, nombre, categoria in productos:
    try:
        cursor.execute("""
            INSERT INTO sales_ms.Producto (SKU, Nombre, Categoria)
            VALUES (?, ?, ?)
        """, (sku, nombre, categoria))
    except pyodbc.IntegrityError:
        print(f"  - {sku} ya existe, omitiendo")

conn.commit()
print(f"  OK: Cargados {len(productos)} productos")

# 3. Obtener IDs
cursor.execute("SELECT ClienteId FROM sales_ms.Cliente")
cliente_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT ProductoId FROM sales_ms.Producto")
producto_ids = [row[0] for row in cursor.fetchall()]

# 4. Insertar órdenes y detalles
print("\n[3] Insertando órdenes y detalles...")
random.seed(42)
fecha_base = datetime.now() - timedelta(days=30)
canales = ['WEB', 'TIENDA', 'APP']
ordenes_count = 0

for i in range(20):  # 20 órdenes
    cliente_id = random.choice(cliente_ids)
    fecha = fecha_base + timedelta(days=random.randint(0, 30))
    canal = random.choice(canales)
    
    # Generar detalles de orden
    num_items = random.randint(1, 5)
    total = 0
    
    try:
        cursor.execute("""
            INSERT INTO sales_ms.Orden (ClienteId, Fecha, Canal, Moneda, Total)
            VALUES (?, ?, ?, 'USD', 0)
        """, (cliente_id, fecha, canal))
        
        # Obtener el OrdenId que se acaba de insertar
        cursor.execute("SELECT @@IDENTITY")
        orden_id = cursor.fetchone()[0]
        
        # Insertar detalles
        detalle_total = 0
        for j in range(num_items):
            producto_id = random.choice(producto_ids)
            cantidad = random.randint(1, 5)
            precio_unit = round(random.uniform(20, 1500), 2)
            descuento_pct = round(random.uniform(0, 20), 2) if random.random() > 0.7 else None
            
            precio_final = precio_unit * cantidad
            if descuento_pct:
                precio_final = precio_final * (1 - descuento_pct / 100)
            
            detalle_total += precio_final
            
            cursor.execute("""
                INSERT INTO sales_ms.OrdenDetalle (OrdenId, ProductoId, Cantidad, PrecioUnit, DescuentoPct)
                VALUES (?, ?, ?, ?, ?)
            """, (orden_id, producto_id, cantidad, precio_unit, descuento_pct))
        
        # Actualizar total de la orden
        cursor.execute("""
            UPDATE sales_ms.Orden SET Total = ? WHERE OrdenId = ?
        """, (detalle_total, orden_id))
        
        ordenes_count += 1
    except Exception as e:
        print(f"  ERROR en orden {i}: {str(e)}")

conn.commit()
print(f"  OK: Cargadas {ordenes_count} órdenes con detalles")

# 5. Verificar cargas
print("\n" + "=" * 80)
print("[VERIFICACIÓN]")
print("=" * 80)

cursor.execute("SELECT COUNT(*) FROM sales_ms.Cliente")
print(f"  Clientes: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM sales_ms.Producto")
print(f"  Productos: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM sales_ms.Orden")
print(f"  Órdenes: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM sales_ms.OrdenDetalle")
print(f"  Detalles: {cursor.fetchone()[0]}")

cursor.execute("SELECT SUM(Total) FROM sales_ms.Orden")
total_ventas = cursor.fetchone()[0]
print(f"  Total de ventas: ${total_ventas:,.2f}")

cursor.close()
conn.close()
print("\n" + "=" * 80)
print("[OK] Datos de prueba cargados exitosamente")
print("=" * 80)
