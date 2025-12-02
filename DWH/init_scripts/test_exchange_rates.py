from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

print("\n" + "="*80)
print("VERIFICACIÓN DE CONVERSIÓN DE MONEDA EN DWH")
print("="*80)

# 1. Estadísticas generales
cur.execute("""
    SELECT 
        COUNT(*) as total_ventas,
        COUNT(exchangeRateId) as con_exchange_rate,
        SUM(CASE WHEN exchangeRateId IS NULL THEN 1 ELSE 0 END) as sin_exchange_rate
    FROM dwh.FactSales
""")
row = cur.fetchone()
print(f"\n📊 ESTADÍSTICAS:")
print(f"   Total ventas: {row[0]:,}")
print(f"   Con exchange rate: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
print(f"   Sin exchange rate: {row[2]:,} ({row[2]/row[0]*100:.2f}%)")

# 2. Muestra de conversiones
print("\n" + "="*80)
print("EJEMPLO DE CONVERSIONES DE MONEDA (Últimas 10 ventas)")
print("="*80 + "\n")

cur.execute("""
    SELECT TOP 10
        t.date,
        p.name as producto,
        fs.productUnitPriceUSD,
        fs.lineTotalUSD,
        ex.rate,
        fs.productUnitPriceUSD * ex.rate as precio_unit_crc,
        fs.lineTotalUSD * ex.rate as total_crc
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    INNER JOIN dwh.DimProduct p ON p.id = fs.productId
    INNER JOIN dwh.DimExchangeRate ex ON ex.id = fs.exchangeRateId
    ORDER BY fs.id DESC
""")

rows = cur.fetchall()
for i, r in enumerate(rows, 1):
    print(f"Venta #{i}")
    print(f"  Fecha:           {r[0]}")
    print(f"  Producto:        {r[1][:50]}")
    print(f"  Precio Unit:     ${r[2]:,.2f} USD")
    print(f"  Total Línea:     ${r[3]:,.2f} USD")
    print(f"  Tipo Cambio:     {r[4]:.4f} CRC/USD")
    print(f"  Precio Unit CRC: ₡{r[5]:,.2f}")
    print(f"  Total Línea CRC: ₡{r[6]:,.2f}")
    print()

# 3. Resumen por fecha
print("="*80)
print("RESUMEN DE VENTAS POR FECHA CON CONVERSIÓN")
print("="*80 + "\n")

cur.execute("""
    SELECT TOP 10
        t.date,
        COUNT(*) as num_ventas,
        SUM(fs.lineTotalUSD) as total_usd,
        AVG(ex.rate) as tipo_cambio_promedio,
        SUM(fs.lineTotalUSD * ex.rate) as total_crc
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    INNER JOIN dwh.DimExchangeRate ex ON ex.id = fs.exchangeRateId
    GROUP BY t.date
    ORDER BY t.date DESC
""")

rows = cur.fetchall()
print(f"{'Fecha':<12} {'Ventas':<10} {'Total USD':<20} {'TC Prom':<12} {'Total CRC':<20}")
print("-"*80)
for r in rows:
    print(f"{str(r[0]):<12} {r[1]:<10,} ${r[2]:<18,.2f} {r[3]:<12,.4f} ₡{r[4]:<18,.2f}")

print("\n✅ Verificación completa\n")
conn.close()
