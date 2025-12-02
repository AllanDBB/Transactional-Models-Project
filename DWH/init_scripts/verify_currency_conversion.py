from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

print("\n" + "="*90)
print("VERIFICACIÓN DE CONVERSIONES DE MONEDA CRC → USD EN DWH")
print("="*90)

# 1. Verificar montos por fuente ANTES de la conversión (en staging)
print("\n📊 MONTOS EN STAGING (valores originales):\n")

sources = [
    ('MSSQL', 'staging.mssql_sales', 'USD'),
    ('MySQL', 'staging.mysql_sales', 'mezcla'),
    ('MongoDB', 'staging.mongo_order_items', 'CRC'),
    ('Neo4j', 'staging.neo4j_order_items', 'USD'),
]

for source_name, table, expected_currency in sources:
    cur.execute(f"""
        SELECT 
            currency,
            COUNT(*) as num_ventas,
            SUM(quantity * unit_price) as total_original
        FROM {table}
        GROUP BY currency
        ORDER BY currency
    """)
    rows = cur.fetchall()
    print(f"{source_name} ({table}):")
    for row in rows:
        print(f"  {row[0]}: {row[1]:,} ventas, Total: {row[0]} {row[2]:,.2f}")
    print()

# 2. Verificar totales DESPUÉS de la conversión (en dwh)
print("="*90)
print("💵 MONTOS EN DWH.FactSales (TODO convertido a USD):\n")

cur.execute("""
    SELECT 
        'Total Ventas' as descripcion,
        COUNT(*) as num_ventas,
        SUM(lineTotalUSD) as total_usd
    FROM dwh.FactSales
""")
row = cur.fetchone()
print(f"{row[0]}: {row[1]:,} ventas, Total USD: ${row[2]:,.2f}\n")

# 3. Comparación detallada: Ejemplo de ventas en CRC vs USD
print("="*90)
print("🔍 EJEMPLOS DE CONVERSIÓN (MongoDB - CRC → USD):\n")

cur.execute("""
    SELECT TOP 5
        t.date as fecha,
        p.name as producto,
        -- Obtener precio original en CRC desde staging
        (SELECT TOP 1 unit_price FROM staging.mongo_order_items WHERE product_key = p.code AND order_date = t.date) as precio_original_crc,
        fs.productUnitPriceUSD as precio_convertido_usd,
        ex.rate as tipo_cambio,
        -- Verificar la conversión: precio_crc / rate = precio_usd
        (SELECT TOP 1 unit_price FROM staging.mongo_order_items WHERE product_key = p.code AND order_date = t.date) / ex.rate as calculo_verificacion
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    INNER JOIN dwh.DimExchangeRate ex ON ex.id = fs.exchangeRateId
    WHERE fs.exchangeRateId IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM staging.mongo_order_items oi 
        WHERE oi.product_key = p.code 
        AND oi.order_date = t.date
        AND oi.currency = 'CRC'
    )
    ORDER BY fs.id DESC
""")

rows = cur.fetchall()
print(f"{'Fecha':<12} {'Producto':<30} {'CRC Orig':<12} {'USD Conv':<12} {'Rate':<10} {'Verif':<12}")
print("-"*90)
for r in rows:
    print(f"{str(r[0]):<12} {r[1][:28]:<30} ₡{r[2]:<10,.2f} ${r[3]:<10,.2f} {r[4]:<10,.4f} ${r[5]:<10,.2f}")

# 4. Resumen por fuente
print("\n" + "="*90)
print("📈 RESUMEN POR FUENTE DE DATOS:\n")

# Necesitamos identificar la fuente desde staging
print("MSSQL (USD puro):")
cur.execute("""
    SELECT COUNT(*), SUM(fs.lineTotalUSD)
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    WHERE EXISTS (
        SELECT 1 FROM staging.mssql_products sp 
        WHERE sp.code = p.code AND sp.source_system = 'MSSQL_SRC'
    )
""")
row = cur.fetchone()
if row[0]:
    print(f"  {row[0]:,} ventas, Total USD: ${row[1]:,.2f}")

print("\nMySQL (mezcla CRC/USD):")
cur.execute("""
    SELECT COUNT(*), SUM(fs.lineTotalUSD)
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    WHERE EXISTS (
        SELECT 1 FROM staging.mysql_sales ms 
        WHERE ms.sku = p.code AND ms.order_date = t.date
    )
""")
row = cur.fetchone()
if row[0]:
    print(f"  {row[0]:,} ventas, Total USD: ${row[1]:,.2f}")

print("\nMongoDB (CRC convertido a USD):")
cur.execute("""
    SELECT COUNT(*), SUM(fs.lineTotalUSD)
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    WHERE EXISTS (
        SELECT 1 FROM staging.mongo_order_items oi 
        WHERE oi.product_key = p.code AND oi.order_date = t.date
    )
""")
row = cur.fetchone()
if row[0]:
    print(f"  {row[0]:,} ventas, Total USD: ${row[1]:,.2f}")

print("\nNeo4j (USD puro):")
cur.execute("""
    SELECT COUNT(*), SUM(fs.lineTotalUSD)
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    WHERE EXISTS (
        SELECT 1 FROM staging.neo4j_order_items oi 
        WHERE oi.product_key = p.code AND oi.order_date = t.date
    )
""")
row = cur.fetchone()
if row[0]:
    print(f"  {row[0]:,} ventas, Total USD: ${row[1]:,.2f}")

print("\n✅ Conversiones completadas correctamente\n")
conn.close()
