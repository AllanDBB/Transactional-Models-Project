import json
import os

# Leer los archivos con valores generados
with open('cliente_values.txt') as f:
    cliente_values = [line.rstrip(',\n') for line in f if line.strip()]

with open('producto_values.txt') as f:
    producto_values = [line.rstrip(',\n') for line in f if line.strip()]

with open('orden_values.txt') as f:
    orden_values = [line.rstrip(',\n') for line in f if line.strip()]

with open('orden_item_values.txt') as f:
    orden_item_values = [line.rstrip(',\n') for line in f if line.strip()]

# Armar el SP
sp_lines = [
    "-- ============================================================================",
    "-- 02-sp_generar_datos.sql (OPTIMIZADO)",
    "-- Stored Procedure para cargar datos desde JSON con mapeos correctos",
    "-- SIN ORDER BY RAND() - Mapeos exactos por JOIN",
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
    "    SELECT 'CARGANDO DATOS CON MAPEOS CORRECTOS' AS '';",
    "    SELECT '========================================' AS '';",
    "",
    "    -- ==========================================",
    "    -- 1. CREAR TABLAS TEMPORALES PARA MAPEO",
    "    -- ==========================================",
    "    DROP TEMPORARY TABLE IF EXISTS cliente_map;",
    "    DROP TEMPORARY TABLE IF EXISTS producto_map;",
    "    DROP TEMPORARY TABLE IF EXISTS orden_map;",
    "    DROP TEMPORARY TABLE IF EXISTS temp_ordenes_mapping;",
    "    DROP TEMPORARY TABLE IF EXISTS temp_orden_items_mapping;",
    "",
    "    CREATE TEMPORARY TABLE cliente_map (",
    "        cliente_id_csv VARCHAR(20) PRIMARY KEY,",
    "        cliente_id_real INT",
    "    );",
    "",
    "    CREATE TEMPORARY TABLE producto_map (",
    "        codigo_alt VARCHAR(64) PRIMARY KEY,",
    "        producto_id_real INT",
    "    );",
    "",
    "    CREATE TEMPORARY TABLE orden_map (",
    "        orden_id_csv VARCHAR(20) PRIMARY KEY,",
    "        orden_id_real INT",
    "    );",
    "",
    "    -- ==========================================",
    "    -- 2. TABLA TEMPORAL CON CLIENTES DEL JSON",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_clientes_mapping (",
    "        cliente_id_csv VARCHAR(20),",
    "        nombre VARCHAR(120),",
    "        email VARCHAR(150),",
    "        genero CHAR(1),",
    "        pais VARCHAR(60),",
    "        fecha VARCHAR(10)",
    "    );",
    "",
    "    INSERT INTO temp_clientes_mapping VALUES",
]

# Agregar valores de clientes
for i, val in enumerate(cliente_values):
    if i < len(cliente_values) - 1:
        sp_lines.append(val + ",")
    else:
        sp_lines.append(val + ";")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(cliente_values)} clientes temporales cargados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 3. INSERTAR CLIENTES EN BD (sin truncar)",
    "    -- ==========================================",
    "    INSERT INTO Cliente (nombre, correo, genero, pais, created_at)",
    "    SELECT nombre, email, genero, pais, fecha FROM temp_clientes_mapping;",
    "",
    f"    SELECT '[OK] {len(cliente_values)} clientes insertados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 4. MAPEAR CLIENTES: cliente_id_csv -> id_real",
    "    -- ==========================================",
    "    INSERT INTO cliente_map (cliente_id_csv, cliente_id_real)",
    "    SELECT",
    "        t.cliente_id_csv,",
    "        c.id",
    "    FROM temp_clientes_mapping t",
    "    INNER JOIN Cliente c ON",
    "        c.nombre = t.nombre AND",
    "        c.email = t.email AND",
    "        c.pais = t.pais;",
    "",
    f"    SELECT '[OK] {len(cliente_values)} clientes mapeados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 5. TABLA TEMPORAL CON PRODUCTOS",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_productos_mapping (",
    "        codigo_alt VARCHAR(64),",
    "        nombre VARCHAR(150),",
    "        categoria VARCHAR(80)",
    "    );",
    "",
    "    INSERT INTO temp_productos_mapping VALUES",
])

# Agregar valores de productos
for i, val in enumerate(producto_values):
    if i < len(producto_values) - 1:
        sp_lines.append(val + ",")
    else:
        sp_lines.append(val + ";")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(producto_values)} productos temporales cargados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 6. INSERTAR PRODUCTOS EN BD (sin truncar)",
    "    -- ==========================================",
    "    INSERT INTO Producto (codigo_alt, nombre, categoria)",
    "    SELECT codigo_alt, nombre, categoria FROM temp_productos_mapping",
    "    WHERE codigo_alt NOT IN (SELECT codigo_alt FROM Producto);",
    "",
    f"    SELECT '[OK] {len(producto_values)} productos insertados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 7. MAPEAR PRODUCTOS: codigo_alt -> id_real",
    "    -- ==========================================",
    "    INSERT INTO producto_map (codigo_alt, producto_id_real)",
    "    SELECT codigo_alt, id FROM Producto",
    "    WHERE codigo_alt NOT IN (SELECT codigo_alt FROM producto_map);",
    "",
    f"    SELECT '[OK] {len(producto_values)} productos mapeados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 8. TABLA TEMPORAL CON ORDENES DEL JSON",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_ordenes_mapping (",
    "        orden_id_csv VARCHAR(20),",
    "        cliente_id_csv VARCHAR(20),",
    "        fecha DATETIME,",
    "        canal VARCHAR(20),",
    "        moneda CHAR(3),",
    "        total VARCHAR(20)",
    "    );",
    "",
    "    INSERT INTO temp_ordenes_mapping VALUES",
])

# Agregar valores de órdenes
for i, val in enumerate(orden_values):
    if i < len(orden_values) - 1:
        sp_lines.append(val + ",")
    else:
        sp_lines.append(val + ";")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(orden_values)} ordenes temporales cargadas' AS '';",
    "",
    "    -- ==========================================",
    "    -- 9. INSERTAR ORDENES EN BD CON MAPEO DE CLIENTES",
    "    -- ==========================================",
    "    INSERT INTO Orden (cliente_id, fecha, canal, moneda, total)",
    "    SELECT",
    "        COALESCE(cm.cliente_id_real, 1) AS cliente_id,",
    "        DATE(t.fecha) AS fecha,",
    "        t.canal,",
    "        t.moneda,",
    "        t.total",
    "    FROM temp_ordenes_mapping t",
    "    LEFT JOIN cliente_map cm ON t.cliente_id_csv = cm.cliente_id_csv;",
    "",
    f"    SELECT '[OK] {len(orden_values)} ordenes insertadas' AS '';",
    "",
    "    -- ==========================================",
    "    -- 10. MAPEAR ORDENES: orden_id_csv -> id_real",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE orden_ids_temp AS",
    "    SELECT",
    "        t.orden_id_csv,",
    "        o.id AS orden_id_real,",
    "        ROW_NUMBER() OVER (PARTITION BY t.orden_id_csv ORDER BY o.id DESC) as rn",
    "    FROM temp_ordenes_mapping t",
    "    INNER JOIN Orden o ON",
    "        COALESCE((SELECT cm.cliente_id_real FROM cliente_map cm WHERE cm.cliente_id_csv = t.cliente_id_csv), 1) = o.cliente_id AND",
    "        DATE(t.fecha) = DATE(o.fecha) AND",
    "        t.canal = o.canal AND",
    "        t.moneda = o.moneda AND",
    "        t.total = o.total;",
    "",
    "    INSERT INTO orden_map (orden_id_csv, orden_id_real)",
    "    SELECT orden_id_csv, orden_id_real",
    "    FROM orden_ids_temp",
    "    WHERE rn = 1;",
    "",
    f"    SELECT '[OK] {len(orden_values)} ordenes mapeadas' AS '';",
    "",
    "    -- ==========================================",
    "    -- 11. TABLA TEMPORAL CON ITEMS DEL JSON",
    "    -- ==========================================",
    "    CREATE TEMPORARY TABLE temp_orden_items_mapping (",
    "        orden_id_csv VARCHAR(20),",
    "        codigo_alt VARCHAR(64),",
    "        cantidad INT,",
    "        precio_unit VARCHAR(20)",
    "    );",
    "",
    "    INSERT INTO temp_orden_items_mapping VALUES",
])

# Agregar valores de orden_items
for i, val in enumerate(orden_item_values):
    if i < len(orden_item_values) - 1:
        sp_lines.append(val + ",")
    else:
        sp_lines.append(val + ";")

sp_lines.extend([
    "",
    f"    SELECT '[OK] {len(orden_item_values)} items temporales cargados' AS '';",
    "",
    "    -- ==========================================",
    "    -- 12. INSERTAR DETALLES DE ORDENES",
    "    -- ==========================================",
    "    -- MAPEO EXACTO: orden_id_csv -> orden_id_real, codigo_alt -> producto_id_real",
    "    -- NO USAR ORDER BY RAND() - Cada item va a su orden y producto correcto",
    "    INSERT INTO OrdenDetalle (orden_id, producto_id, cantidad, precio_unit)",
    "    SELECT",
    "        om.orden_id_real,",
    "        pm.producto_id_real,",
    "        t.cantidad,",
    "        t.precio_unit",
    "    FROM temp_orden_items_mapping t",
    "    INNER JOIN orden_map om ON t.orden_id_csv = om.orden_id_csv",
    "    INNER JOIN producto_map pm ON t.codigo_alt = pm.codigo_alt;",
    "",
    f"    SELECT '[OK] {len(orden_item_values)} detalles de ordenes insertados con mapeos CORRECTOS' AS '';",
    "",
    "    COMMIT;",
    "",
    "    SELECT '' AS '';",
    "    SELECT '========================================' AS '';",
    "    SELECT 'DATOS CARGADOS EXITOSAMENTE' AS '';",
    "    SELECT '========================================' AS '';",
    f"    SELECT 'Clientes:          {len(cliente_values):,}' AS '';",
    f"    SELECT 'Productos:         {len(producto_values):,}' AS '';",
    f"    SELECT 'Ordenes:           {len(orden_values):,}' AS '';",
    f"    SELECT 'Detalles:          {len(orden_item_values):,}' AS '';",
    "    SELECT '========================================' AS '';",
    "",
    "END //",
    "",
    "DELIMITER ;",
    "",
    "SELECT '[OK] Stored Procedure sp_generar_datos creado exitosamente' AS '';",
])

# Escribir el archivo
# Ruta: desde shared/generated/mysql/json/ subir a raíz del proyecto (4 niveles)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..', '..', '..', '..'))
output_path = os.path.join(project_root, 'MYSQL', 'init', '02-sp_generar_datos.sql')
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(sp_lines))

sp_content = '\n'.join(sp_lines)
print(f"SP optimizado generado: {output_path}")
print(f"Líneas: {len(sp_lines)}")
print(f"Tamaño: {len(sp_content) / 1024 / 1024:.2f} MB")
print()
print("Cambios principales:")
print("  - SIN ORDER BY RAND()")
print("  - Mapeo exacto cliente_id_csv -> id_real")
print("  - Mapeo exacto codigo_alt -> producto_id_real")
print("  - Mapeo exacto orden_id_csv -> id_real")
print("  - No trunca tablas existentes")
print("  - Usar INNER JOIN en lugar de subqueries")
