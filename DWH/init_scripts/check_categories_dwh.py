from db_utils import get_connection

conn = get_connection()
cur = conn.cursor(as_dict=True)

print("\n=== DimCategory ===")
cur.execute("SELECT id, name FROM dwh.DimCategory ORDER BY name")
for row in cur:
    print(f"  {row['id']}: {row['name']}")

print("\n=== DimProduct (con categoría) ===")
cur.execute("""
    SELECT p.id, p.name, p.code, c.name as category
    FROM dwh.DimProduct p
    LEFT JOIN dwh.DimCategory c ON c.id = p.categoryId
    ORDER BY p.name
""")
for row in cur:
    print(f"  {row['id']}: {row['name']} ({row['code']}) - {row['category']}")

conn.close()
