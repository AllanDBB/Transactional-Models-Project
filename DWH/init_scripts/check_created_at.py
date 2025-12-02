from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

print("\n" + "="*90)
print("VERIFICACIÓN DE CAMPOS created_at EN DWH")
print("="*90 + "\n")

# 1. DimCustomer
print("📊 DimCustomer - created_at:")
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(created_at) as con_fecha,
        MIN(created_at) as fecha_min,
        MAX(created_at) as fecha_max
    FROM dwh.DimCustomer
""")
row = cur.fetchone()
print(f"  Total clientes: {row[0]:,}")
print(f"  Con created_at: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
if row[2] and row[3]:
    print(f"  Rango: {row[2]} a {row[3]}")
print()

# 2. DimProduct
print("📦 DimProduct - created_at:")
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(created_at) as con_fecha,
        MIN(created_at) as fecha_min,
        MAX(created_at) as fecha_max
    FROM dwh.DimProduct
""")
row = cur.fetchone()
print(f"  Total productos: {row[0]:,}")
print(f"  Con created_at: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
if row[2] and row[3]:
    print(f"  Rango: {row[2]} a {row[3]}")
print()

# 3. FactSales
print("💰 FactSales - created_at:")
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(created_at) as con_fecha,
        MIN(created_at) as fecha_min,
        MAX(created_at) as fecha_max
    FROM dwh.FactSales
""")
row = cur.fetchone()
print(f"  Total ventas: {row[0]:,}")
print(f"  Con created_at: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
if row[2] and row[3]:
    print(f"  Rango: {row[2]} a {row[3]}")
print()

# 4. Comparar created_at de FactSales vs order_date (DimTime)
print("="*90)
print("⚠️  COMPARACIÓN: created_at vs order_date en FactSales")
print("="*90 + "\n")

cur.execute("""
    SELECT TOP 10
        fs.id,
        fs.created_at as fact_created_at,
        t.date as order_date,
        DATEDIFF(day, t.date, fs.created_at) as dias_diferencia
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    ORDER BY fs.id DESC
""")

rows = cur.fetchall()
print(f"{'ID':<10} {'FactSales.created_at':<25} {'Order Date':<15} {'Diferencia'}")
print("-"*90)
for r in rows:
    print(f"{r[0]:<10} {str(r[1]):<25} {str(r[2]):<15} {r[3]:,} días")

print("\n" + "="*90)
print("🔍 ANÁLISIS DE FECHAS EN STAGING vs DWH")
print("="*90 + "\n")

# Verificar si las fechas originales se preservan
print("MSSQL - Fechas originales en staging:")
cur.execute("""
    SELECT TOP 5
        source_key,
        order_date,
        created_at as staging_created_at
    FROM staging.mssql_sales
    ORDER BY staging_id DESC
""")
rows = cur.fetchall()
print(f"{'Source Key':<20} {'Order Date':<15} {'Staging created_at'}")
print("-"*60)
for r in rows:
    print(f"{r[0]:<20} {str(r[1]):<15} {r[2]}")

print("\n" + "="*90)
print("⚠️  PROBLEMA DETECTADO:")
print("="*90)
print("""
El campo 'created_at' en FactSales está usando GETDATE() (fecha actual del transform),
NO la fecha original de la transacción.

OPCIONES:
1. ✅ CORRECTO: created_at = fecha de cuándo se insertó en el DWH (auditoría)
2. ❌ PROBLEMA: Si queremos la fecha original de la transacción, debemos usar
   order_date o traer el created_at_src desde staging.

¿Qué debería representar 'created_at'?
- Fecha de CARGA al DWH (actual: GETDATE()) ✅
- Fecha ORIGINAL de la transacción (debería ser order_date o un nuevo campo)
""")

conn.close()
