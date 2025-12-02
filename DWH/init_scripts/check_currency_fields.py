from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

tables = ['mssql_sales', 'mysql_sales', 'mongo_order_items', 'neo4j_order_items', 'supabase_order_items']

print("\n" + "="*80)
print("VERIFICACIÓN DE CAMPOS DE MONEDA EN STAGING")
print("="*80 + "\n")

for table in tables:
    try:
        cur.execute(f"SELECT TOP 1 * FROM staging.{table}")
        cols = [desc[0] for desc in cur.description]
        print(f"staging.{table}:")
        print(f"  Columnas: {', '.join(cols)}")
        
        # Check if currency column exists
        if 'currency' in cols:
            cur.execute(f"SELECT DISTINCT currency, COUNT(*) as count FROM staging.{table} GROUP BY currency")
            currencies = cur.fetchall()
            print(f"  ✓ Tiene campo 'currency':")
            for curr, count in currencies:
                print(f"    - {curr}: {count:,} registros")
        else:
            print(f"  ✗ NO tiene campo 'currency'")
        print()
    except Exception as e:
        print(f"  Error: {e}\n")

conn.close()
