from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

print("\n" + "="*90)
print("VERIFICACIÓN DE CAMPOS created_at / FECHAS EN DWH")
print("="*90 + "\n")

# 1. DimCustomer.created_at
print("📊 DimCustomer.created_at:")
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(created_at) as con_fecha,
        MIN(created_at) as fecha_min,
        MAX(created_at) as fecha_max,
        SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) as nulos
    FROM dwh.DimCustomer
""")
row = cur.fetchone()
print(f"  Total clientes: {row[0]:,}")
print(f"  Con created_at: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
print(f"  Nulos: {row[4]:,}")
if row[2] and row[3]:
    print(f"  Rango: {row[2]} a {row[3]}")
print()

# Muestra
cur.execute("""
    SELECT TOP 5 id, name, email, created_at
    FROM dwh.DimCustomer
    ORDER BY id DESC
""")
rows = cur.fetchall()
print("  Muestra:")
print(f"  {'ID':<8} {'Name':<25} {'Email':<30} {'created_at'}")
print("  " + "-"*85)
for r in rows:
    name = (r[1][:23] if r[1] else "NULL") 
    email = (r[2][:28] if r[2] else "NULL")
    created = str(r[3]) if r[3] else "NULL"
    print(f"  {r[0]:<8} {name:<25} {email:<30} {created}")

# 2. FactSales.created_at
print("\n" + "="*90)
print("💰 FactSales.created_at:")
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

# 3. PROBLEMA: created_at vs order_date
print("="*90)
print("⚠️  ANÁLISIS: FactSales.created_at vs order_date")
print("="*90 + "\n")

cur.execute("""
    SELECT TOP 10
        fs.id,
        t.date as order_date,
        fs.created_at as dwh_created_at,
        DATEDIFF(day, t.date, fs.created_at) as dias_diferencia
    FROM dwh.FactSales fs
    INNER JOIN dwh.DimTime t ON t.id = fs.timeId
    ORDER BY fs.id DESC
""")

rows = cur.fetchall()
print(f"{'ID':<10} {'Order Date':<15} {'DWH created_at':<25} {'Diferencia'}")
print("-"*75)
for r in rows:
    print(f"{r[0]:<10} {str(r[1]):<15} {str(r[2]):<25} {r[3]:,} días")

# 4. Verificar si todas las ventas tienen el mismo created_at (GETDATE())
print("\n" + "="*90)
print("🔍 ¿Todas las ventas tienen el MISMO created_at?")
print("="*90 + "\n")

cur.execute("""
    SELECT 
        created_at,
        COUNT(*) as num_ventas
    FROM dwh.FactSales
    GROUP BY created_at
    ORDER BY num_ventas DESC
""")

rows = cur.fetchall()
print(f"{'created_at':<30} {'# Ventas'}")
print("-"*45)
for r in rows[:5]:
    print(f"{str(r[0]):<30} {r[1]:,}")

if len(rows) == 1:
    print(f"\n⚠️  PROBLEMA: Todas las {rows[0][1]:,} ventas tienen el mismo created_at!")
    print("    Esto significa que se está usando GETDATE() en el INSERT.")
    print("    created_at representa la fecha de CARGA al DWH, no la fecha original.\n")
else:
    print(f"\n✅ Las ventas tienen diferentes created_at ({len(rows)} fechas únicas)")

print("="*90)
print("CONCLUSIÓN:")
print("="*90)
print("""
El campo 'created_at' actualmente representa:
  ✅ DimCustomer.created_at: Fecha original del cliente (de staging)
  ⚠️  FactSales.created_at: Fecha de CARGA al DWH (GETDATE() - auditoría)
  
Si necesitas la fecha ORIGINAL de la transacción en FactSales:
  → Ya la tienes en DimTime (a través de timeId)
  → order_date = fecha real de la venta
  → created_at = fecha de auditoría (cuándo se cargó al DWH)

Esto es CORRECTO para un DWH de auditoría.
""")

conn.close()
