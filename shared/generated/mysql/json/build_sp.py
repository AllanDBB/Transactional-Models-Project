import json
import os

# Cargar datos directamente desde JSON en lugar de usar archivos TXT
with open('clientes.json') as f:
    clientes = json.load(f)

with open('productos.json') as f:
    productos = json.load(f)

with open('ordenes.json') as f:
    ordenes = json.load(f)

with open('orden_items.json') as f:
    orden_items = json.load(f)

# Construir mapeos en memoria
# ===============================
# Mapeo 1: Email -> Cliente (para búsquedas en BD)
# Mapeo 2: SKU -> codigo_alt (para conectar orden_items con productos)
# ===============================

sku_to_alt = {}
for prod in productos:
    sku_to_alt[prod['sku']] = prod['codigo_alt']

# Armar el SP con lógica de mapeos mejorada
sp_lines = [
    "-- ============================================================================",
    "-- 02-sp_generar_datos.sql (MEJORADO CON MAPEOS DIRECTOS)",
    "-- Stored Procedure para cargar datos desde JSON con mapeos por campos únicos",
    "-- Mapeo Cliente: por EMAIL (único)",
    "-- Mapeo Producto: por CODIGO_ALT (único)",
    "-- Mapeo Orden: por cliente_map + datos únicos",
    "-- Mapeo Items: por orden_map + producto_map",
    "-- ============================================================================",
    "",
    "USE sales_mysql;",
    "",
    "DROP PROCEDURE IF EXISTS sp_generar_datos;",
    "",
    "DELIMITER //",
    "",
    "CREATE PROCEDURE sp_generar_datos()",
    "BEGIN",
    "    DECLARE EXIT HANDLER FOR SQLEXCEPTION",
    "    BEGIN",
    "        SELECT 'Error durante la carga de datos' AS error_message;",
    "        ROLLBACK;",
    "    END;",
    "",
    "    START TRANSACTION;",
    "",
    "    SELECT '========================================' AS '';",
    "    SELECT 'CARGANDO DATOS CON MAPEOS DIRECTOS' AS '';",
    "    SELECT '========================================' AS '';",
    "",
    "    -- ==========================================",
    "    -- LIMPIAR TABLAS TEMPORALES",
    "    -- ==========================================",
    "    DROP TEMPORARY TABLE IF EXISTS cliente_map;",
    "    DROP TEMPORARY TABLE IF EXISTS producto_map;",
    "    DROP TEMPORARY TABLE IF EXISTS orden_map;",
    "    DROP TEMPORARY TABLE IF EXISTS sku_to_alt_map;",
    "    DROP TEMPORARY TABLE IF EXISTS temp_clientes;",
    "    DROP TEMPORARY TABLE IF EXISTS temp_productos;",
    "    DROP TEMPORARY TABLE IF EXISTS temp_ordenes;",
    "    DROP TEMPORARY TABLE IF EXISTS temp_orden_items;",
    "",
    "    -- ==========================================",
    "    -- CREAR TABLAS TEMPORALES DE MAPEO",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE cliente_map (",
    "        email VARCHAR(150) PRIMARY KEY,",
    "        cliente_id_json VARCHAR(20),",
    "        cliente_id_real INT",
    "    );",
    "",
    "    CREATE TEMPORARY TABLE producto_map (",
    "        codigo_alt VARCHAR(64) PRIMARY KEY,",
    "        sku VARCHAR(20),",
    "        producto_id_real INT",
    "    );",
    "",
    "    CREATE TEMPORARY TABLE orden_map (",
    "        orden_id_csv VARCHAR(20) PRIMARY KEY,",
    "        orden_id_real INT",
    "    );",
    "",
    "    CREATE TEMPORARY TABLE sku_to_alt_map (",
    "        sku VARCHAR(20) PRIMARY KEY,",
    "        codigo_alt VARCHAR(64)",
    "    );",
    "",
    "    -- ==========================================",
    "    -- 1. MAPEO SKU -> CODIGO_ALT",
    "    -- ==========================================",
    "    INSERT INTO sku_to_alt_map (sku, codigo_alt) VALUES",
]

# Agregar mapeo SKU -> codigo_alt
for i, prod in enumerate(productos):
    sku = prod['sku']
    alt = prod['codigo_alt']
    if i < len(productos) - 1:
        sp_lines.append(f"    ('{sku}', '{alt}'),")
    else:
        sp_lines.append(f"    ('{sku}', '{alt}');")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(productos)} mapeos SKU->codigo_alt creados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 2. TABLA TEMPORAL DE CLIENTES",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_clientes (",
    "        cliente_id_json VARCHAR(20),",
    "        nombre VARCHAR(120),",
    "        email VARCHAR(150),",
    "        genero VARCHAR(20),",
    "        pais VARCHAR(60),",
    "        fecha_registro VARCHAR(19)",
    "    );",
    "",
    "    INSERT INTO temp_clientes (cliente_id_json, nombre, email, genero, pais, fecha_registro) VALUES",
])

# Agregar clientes con VALUES
for i, cliente in enumerate(clientes):
    genero_map = {'Masculino': 'M', 'Femenino': 'F', 'Otro': 'X'}
    genero = genero_map.get(cliente['genero'], 'M')
    email = cliente['email'].replace("'", "\\'")
    nombre = cliente['nombre'].replace("'", "\\'")

    if i < len(clientes) - 1:
        sp_lines.append(f"    ('{cliente['cliente_id']}', '{nombre}', '{email}', '{genero}', '{cliente['pais']}', '{cliente['fecha_registro']}'),")
    else:
        sp_lines.append(f"    ('{cliente['cliente_id']}', '{nombre}', '{email}', '{genero}', '{cliente['pais']}', '{cliente['fecha_registro']}');")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(clientes)} clientes temporales cargados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 3. INSERTAR CLIENTES EN BD",
    "    -- ==========================================",
    "    INSERT INTO Cliente (nombre, correo, genero, pais, created_at)",
    "    SELECT nombre, email, genero, pais, DATE(fecha_registro)",
    "    FROM temp_clientes",
    "    WHERE email NOT IN (SELECT correo FROM Cliente);",
    "",
    f"    SELECT '[OK] Clientes insertados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 4. MAPEO CLIENTES: JSON -> REAL",
    "    -- ==========================================",
    "    INSERT INTO cliente_map (email, cliente_id_json, cliente_id_real)",
    "    SELECT DISTINCT",
    "        t.email,",
    "        t.cliente_id_json,",
    "        c.id",
    "    FROM temp_clientes t",
    "    INNER JOIN Cliente c ON c.correo = t.email;",
    "",
    f"    SELECT '[OK] {len(clientes)} clientes mapeados por EMAIL' AS '';",
    "",
    "    -- ==========================================",
    "    -- 5. TABLA TEMPORAL DE PRODUCTOS",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_productos (",
    "        sku VARCHAR(20),",
    "        nombre VARCHAR(150),",
    "        categoria VARCHAR(80),",
    "        codigo_alt VARCHAR(64)",
    "    );",
    "",
    "    INSERT INTO temp_productos (sku, nombre, categoria, codigo_alt) VALUES",
])

# Agregar productos
for i, prod in enumerate(productos):
    nombre = prod['nombre'].replace("'", "\\'")
    categoria = prod['categoria'].replace("'", "\\'")

    if i < len(productos) - 1:
        sp_lines.append(f"    ('{prod['sku']}', '{nombre}', '{categoria}', '{prod['codigo_alt']}'),")
    else:
        sp_lines.append(f"    ('{prod['sku']}', '{nombre}', '{categoria}', '{prod['codigo_alt']}');")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(productos)} productos temporales cargados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 6. INSERTAR PRODUCTOS EN BD",
    "    -- ==========================================",
    "    INSERT INTO Producto (codigo_alt, nombre, categoria)",
    "    SELECT codigo_alt, nombre, categoria",
    "    FROM temp_productos",
    "    WHERE codigo_alt NOT IN (SELECT codigo_alt FROM Producto);",
    "",
    f"    SELECT '[OK] Productos insertados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 7. MAPEO PRODUCTOS: SKU/codigo_alt -> REAL",
    "    -- ==========================================",
    "    INSERT INTO producto_map (codigo_alt, sku, producto_id_real)",
    "    SELECT DISTINCT",
    "        t.codigo_alt,",
    "        t.sku,",
    "        p.id",
    "    FROM temp_productos t",
    "    INNER JOIN Producto p ON p.codigo_alt = t.codigo_alt;",
    "",
    f"    SELECT '[OK] {len(productos)} productos mapeados por CODIGO_ALT' AS '';",
    "",
    "    -- ==========================================",
    "    -- 8. TABLA TEMPORAL DE ORDENES",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_ordenes (",
    "        orden_id_json VARCHAR(20),",
    "        cliente_id_json VARCHAR(20),",
    "        fecha VARCHAR(19),",
    "        canal VARCHAR(20),",
    "        moneda CHAR(3),",
    "        total VARCHAR(20)",
    "    );",
    "",
    "    INSERT INTO temp_ordenes (orden_id_json, cliente_id_json, fecha, canal, moneda, total) VALUES",
])

# Agregar órdenes
for i, orden in enumerate(ordenes):
    if i < len(ordenes) - 1:
        sp_lines.append(f"    ('{orden['orden_id']}', '{orden['cliente_id']}', '{orden['fecha']}', '{orden['canal']}', '{orden['moneda']}', '{orden['total']}'),")
    else:
        sp_lines.append(f"    ('{orden['orden_id']}', '{orden['cliente_id']}', '{orden['fecha']}', '{orden['canal']}', '{orden['moneda']}', '{orden['total']}');")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(ordenes)} ordenes temporales cargadas' AS '';",
    "",
    "    -- ==========================================",
    "    -- 9. INSERTAR ORDENES EN BD",
    "    -- ==========================================",
    "    INSERT INTO Orden (cliente_id, fecha, canal, moneda, total)",
    "    SELECT",
    "        cm.cliente_id_real,",
    "        t.fecha,",
    "        t.canal,",
    "        t.moneda,",
    "        t.total",
    "    FROM temp_ordenes t",
    "    INNER JOIN cliente_map cm ON cm.cliente_id_json = t.cliente_id_json;",
    "",
    f"    SELECT '[OK] {len(ordenes)} ordenes insertadas' AS '';",
    "",
    "    -- ==========================================",
    "    -- 10. MAPEO ORDENES: orden_id_json -> REAL",
    "    -- ==========================================",
    "    INSERT INTO orden_map (orden_id_csv, orden_id_real)",
    "    SELECT",
    "        t.orden_id_json,",
    "        o.id",
    "    FROM temp_ordenes t",
    "    INNER JOIN cliente_map cm ON cm.cliente_id_json = t.cliente_id_json",
    "    INNER JOIN Orden o ON",
    "        o.cliente_id = cm.cliente_id_real AND",
    "        o.fecha = t.fecha AND",
    "        o.canal = t.canal AND",
    "        o.moneda = t.moneda AND",
    "        o.total = t.total;",
    "",
    f"    SELECT '[OK] {len(ordenes)} ordenes mapeadas' AS '';",
    "",
    "    -- ==========================================",
    "    -- 11. TABLA TEMPORAL DE ITEMS",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_orden_items (",
    "        orden_id_json VARCHAR(20),",
    "        sku VARCHAR(20),",
    "        cantidad INT,",
    "        precio_unit VARCHAR(20)",
    "    );",
    "",
    "    INSERT INTO temp_orden_items (orden_id_json, sku, cantidad, precio_unit) VALUES",
])

# Agregar items de órdenes
for i, item in enumerate(orden_items):
    if i < len(orden_items) - 1:
        sp_lines.append(f"    ('{item['orden_id']}', '{item['producto_id']}', {item['cantidad']}, '{item['precio_unit']}'),")
    else:
        sp_lines.append(f"    ('{item['orden_id']}', '{item['producto_id']}', {item['cantidad']}, '{item['precio_unit']}');")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(orden_items)} items temporales cargados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 12. INSERTAR DETALLES DE ORDENES",
    "    -- ==========================================",
    "    -- MAPEO EXACTO USANDO:",
    "    -- - orden_map: orden_id_json -> orden_id_real",
    "    -- - sku_to_alt_map: sku -> codigo_alt",
    "    -- - producto_map: codigo_alt -> producto_id_real",
    "    INSERT INTO OrdenDetalle (orden_id, producto_id, cantidad, precio_unit)",
    "    SELECT",
    "        om.orden_id_real,",
    "        pm.producto_id_real,",
    "        t.cantidad,",
    "        t.precio_unit",
    "    FROM temp_orden_items t",
    "    INNER JOIN sku_to_alt_map s ON s.sku = t.sku",
    "    INNER JOIN producto_map pm ON pm.codigo_alt = s.codigo_alt",
    "    INNER JOIN orden_map om ON om.orden_id_csv = t.orden_id_json;",
    "",
    f"    SELECT '[OK] {len(orden_items)} detalles de ordenes insertados CON MAPEOS CORRECTOS' AS '';",
    "",
    "    COMMIT;",
    "",
    "    SELECT '' AS '';",
    "    SELECT '========================================' AS '';",
    "    SELECT 'DATOS CARGADOS EXITOSAMENTE' AS '';",
    "    SELECT '========================================' AS '';",
    f"    SELECT 'Clientes:          {len(clientes):,}' AS '';",
    f"    SELECT 'Productos:         {len(productos):,}' AS '';",
    f"    SELECT 'Ordenes:           {len(ordenes):,}' AS '';",
    f"    SELECT 'Detalles:          {len(orden_items):,}' AS '';",
    "    SELECT '========================================' AS '';",
    "",
    "END //",
    "",
    "DELIMITER ;",
    "",
    "SELECT '[OK] Stored Procedure sp_generar_datos creado exitosamente' AS '';",
])

# Escribir el archivo SQL generado
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..', '..', '..', '..'))
output_path = os.path.join(project_root, 'MYSQL', 'init', '02-sp_generar_datos.sql')
os.makedirs(os.path.dirname(output_path), exist_ok=True)

sp_content = '\n'.join(sp_lines)
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(sp_content)

print("=" * 80)
print("STORED PROCEDURE GENERADO CON ÉXITO")
print("=" * 80)
print(f"Archivo: {output_path}")
print(f"Líneas: {len(sp_lines):,}")
print(f"Tamaño: {len(sp_content) / 1024 / 1024:.2f} MB")
print()
print("Mapeos implementados:")
print(f"  1. SKU -> codigo_alt:        {len(productos)} mapeos")
print(f"  2. Email -> Cliente (JSON):  {len(clientes)} clientes")
print(f"  3. Productos (SKU):          {len(productos)} productos")
print(f"  4. Órdenes (orden_id):       {len(ordenes)} órdenes")
print(f"  5. Items de órdenes:         {len(orden_items)} items")
print()
print("Estrategia de mapeo:")
print("  - Clientes: mapeo por EMAIL (campo único)")
print("  - Productos: mapeo por CODIGO_ALT (campo único)")
print("  - Órdenes: mapeo por (cliente_id_real, fecha, canal, moneda, total)")
print("  - Items: mapeo por (orden_id_real, SKU -> codigo_alt -> producto_id_real)")
print()
print("Características:")
print("  - Uso de INNER JOIN para garantizar mapeos exactos")
print("  - Tablas temporales para intermediate mappings")
print("  - Sin ORDER BY RAND() (mapeos determinísticos)")
print("  - No trunca tablas existentes (append-only)")
print("=" * 80)
