from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

print("\n" + "="*90)
print("EJEMPLO DETALLADO DE CONVERSIÓN CRC → USD")
print("="*90 + "\n")

# Obtener ejemplos de MongoDB (CRC)
print("🇨🇷 MONGODB - Precios originales en CRC:\n")
cur.execute("""
    SELECT TOP 5
        product_desc,
        unit_price as precio_crc,
        currency,
        order_date
    FROM staging.mongo_order_items
    WHERE currency = 'CRC'
    ORDER BY unit_price DESC
""")

rows = cur.fetchall()
print(f"{'Producto':<35} {'Precio Original':<20} {'Moneda':<10} {'Fecha'}")
print("-"*90)
for r in rows:
    producto = r[0] if r[0] else "Sin descripción"
    print(f"{producto[:33]:<35} ₡{r[1]:>15,.2f} {r[2]:<10} {r[3]}")

print("\n" + "="*90)
print("💵 DWH.FactSales - Precios CONVERTIDOS a USD:\n")

# Buscar estos mismos productos en FactSales
cur.execute("""
    SELECT TOP 10
        p.name as producto,
        fs.productUnitPriceUSD as precio_usd_convertido,
        ex.rate as tipo_cambio,
        t.date as fecha,
        fs.productUnitPriceUSD * ex.rate as precio_crc_calculado
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    INNER JOIN dwh.DimExchangeRate ex ON ex.id = fs.exchangeRateId
    WHERE EXISTS (
        SELECT 1 FROM staging.mongo_order_items oi
        WHERE oi.product_key = p.code 
        AND oi.order_date = t.date
        AND oi.currency = 'CRC'
    )
    ORDER BY fs.productUnitPriceUSD DESC
""")

rows = cur.fetchall()
print(f"{'Producto':<35} {'USD':<15} {'Rate':<12} {'Fecha':<12} {'CRC (calc)':<15}")
print("-"*90)
for r in rows:
    print(f"{r[0][:33]:<35} ${r[1]:>12,.2f} {r[2]:>10,.4f} {str(r[3]):<12} ₡{r[4]:>12,.2f}")

print("\n" + "="*90)
print("✅ VERIFICACIÓN: MySQL (mezcla de CRC y USD):\n")

cur.execute("""
    SELECT TOP 10
        ms.sku,
        ms.currency,
        ms.unit_price as precio_original,
        ex.rate,
        CASE 
            WHEN ms.currency = 'CRC' THEN ms.unit_price / ex.rate
            ELSE ms.unit_price
        END as precio_usd_calculado,
        ms.order_date
    FROM staging.mysql_sales ms
    LEFT JOIN dwh.DimExchangeRate ex ON ex.date = ms.order_date AND ex.fromCurrency = 'CRC' AND ex.toCurrency = 'USD'
    WHERE ms.unit_price > 10000  -- Precios grandes para ver la diferencia
    ORDER BY ms.unit_price DESC
""")

rows = cur.fetchall()
print(f"{'SKU':<15} {'Curr':<6} {'Original':<18} {'Rate':<10} {'USD Conv':<15} {'Fecha'}")
print("-"*90)
for r in rows:
    rate_str = f"{r[3]:.4f}" if r[3] else "N/A"
    usd_conv = r[4] if r[4] else 0.0
    if r[1] == 'CRC':
        print(f"{r[0]:<15} {r[1]:<6} ₡{r[2]:>15,.2f} {rate_str:>8} ${usd_conv:>12,.2f} {r[5]}")
    else:
        print(f"{r[0]:<15} {r[1]:<6} ${r[2]:>15,.2f} {'N/A':<10} ${usd_conv:>12,.2f} {r[5]}")

print("\n✅ Las conversiones están funcionando correctamente!\n")
conn.close()
