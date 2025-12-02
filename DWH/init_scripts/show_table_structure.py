from db_utils import get_connection

conn = get_connection()
cur = conn.cursor()

print("\n" + "="*90)
print("ESTRUCTURA DE TABLAS DWH")
print("="*90 + "\n")

tables = ['dwh.DimCustomer', 'dwh.DimProduct', 'dwh.DimCategory', 'dwh.FactSales', 'dwh.DimExchangeRate']

for table in tables:
    cur.execute(f"SELECT TOP 0 * FROM {table}")
    cols = [desc[0] for desc in cur.description]
    print(f"{table}:")
    print(f"  {', '.join(cols)}")
    print()

conn.close()
