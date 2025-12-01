from db_utils import get_connection

conn = get_connection()
cur = conn.cursor(as_dict=True)

print("\n=== Categorías Neo4j ===")
cur.execute("SELECT node_label, node_key, props_json FROM staging.neo4j_nodes WHERE node_label = 'Categoria'")
for row in cur:
    print(f"{row['node_label']}: {row['node_key']}")
    print(f"  Props: {row['props_json']}")

print("\n=== Relaciones PERTENECE_A ===")
cur.execute("SELECT TOP 5 edge_type, from_label, from_key, to_label, to_key FROM staging.neo4j_edges WHERE edge_type = 'PERTENECE_A'")
for row in cur:
    print(f"{row['from_label']}:{row['from_key']} -[{row['edge_type']}]-> {row['to_label']}:{row['to_key']}")

print("\n=== Productos Neo4j ===")
cur.execute("SELECT TOP 3 node_key, props_json FROM staging.neo4j_nodes WHERE node_label = 'Producto'")
for row in cur:
    print(f"{row['node_key']}: {row['props_json']}")

conn.close()
