import json
from datetime import datetime

# Leer JSONs
with open('clientes.json') as f:
    clientes = json.load(f)

with open('productos.json') as f:
    productos = json.load(f)

with open('ordenes.json') as f:
    ordenes = json.load(f)

with open('orden_items.json') as f:
    orden_items = json.load(f)

# Mapeo de géneros
genero_map = {
    'Femenino': 'F',
    'Masculino': 'M',
    'Otro': 'X',
}

# Crear mapeo SKU → codigo_alt
sku_to_codigo = {p['sku']: p['codigo_alt'] for p in productos}

# Helper para escapar comillas
def escape_sql(s):
    if s is None:
        return 'NULL'
    return str(s).replace("'", "''")

# Generar VALUES para clientes
cliente_values = []
for c in clientes:
    fecha = c['fecha_registro'].split('T')[0]  # Solo la fecha
    genero = genero_map.get(c['genero'], 'M')
    values_line = f"('{escape_sql(c['nombre'])}', '{escape_sql(c['email'])}', '{genero}', '{escape_sql(c['pais'])}', '{fecha}')"
    cliente_values.append(values_line)

# Generar VALUES para productos
producto_values = []
for p in productos:
    values_line = f"('{escape_sql(p['codigo_alt'])}', '{escape_sql(p['nombre'])}', '{escape_sql(p['categoria'])}')"
    producto_values.append(values_line)

# Generar VALUES para órdenes
orden_values = []
for o in ordenes:
    fecha_str = o['fecha'].replace('T', ' ')[:19]  # YYYY-MM-DD HH:MM:SS
    values_line = f"('{escape_sql(o['orden_id'])}', '{escape_sql(o['cliente_id'])}', '{fecha_str}', '{escape_sql(o['canal'])}', '{escape_sql(o['moneda'])}', '{escape_sql(str(o['total']))}')"
    orden_values.append(values_line)

# Generar VALUES para orden_items
orden_item_values = []
for oi in orden_items:
    codigo_alt = sku_to_codigo.get(oi['producto_id'], 'UNKNOWN')
    values_line = f"('{escape_sql(oi['orden_id'])}', '{codigo_alt}', {oi['cantidad']}, '{escape_sql(str(oi['precio_unit']))}')"
    orden_item_values.append(values_line)

# Imprimir estadísticas
print(f"Clientes: {len(clientes)}")
print(f"Productos: {len(productos)}")
print(f"Órdenes: {len(ordenes)}")
print(f"Items: {len(orden_items)}")
print(f"\nSample cliente VALUES: {cliente_values[0]}")
print(f"Sample producto VALUES: {producto_values[0]}")
print(f"Sample orden VALUES: {orden_values[0]}")
print(f"Sample item VALUES: {orden_item_values[0]}")

# Guardar para el siguiente paso
with open('cliente_values.txt', 'w') as f:
    for val in cliente_values:
        f.write(val + ',\n')

with open('producto_values.txt', 'w') as f:
    for val in producto_values:
        f.write(val + ',\n')

with open('orden_values.txt', 'w') as f:
    for val in orden_values:
        f.write(val + ',\n')

with open('orden_item_values.txt', 'w') as f:
    for val in orden_item_values:
        f.write(val + ',\n')

print("\nArchivos generados: cliente_values.txt, producto_values.txt, orden_values.txt, orden_item_values.txt")
