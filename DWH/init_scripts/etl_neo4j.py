import json
import logging
import os
from datetime import datetime, date

from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.time import DateTime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_neo4j")
load_dotenv()


def get_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "password123")
    return GraphDatabase.driver(uri, auth=(user, pwd))


def serialize_neo4j_value(obj):
    """Serializar valores de Neo4j a JSON"""
    if isinstance(obj, DateTime):
        return obj.iso_format()
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: serialize_neo4j_value(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_neo4j_value(i) for i in obj]
    return obj


def load_nodes_and_edges():
    clear_table("staging.neo4j_nodes")
    clear_table("staging.neo4j_edges")
    driver = get_driver()
    nodes = []
    edges = []
    with driver.session() as session:
        # Clientes
        result = session.run("MATCH (c:Cliente) RETURN labels(c) AS lbls, c.id AS id, properties(c) AS props")
        for r in result:
            nodes.append(
                (
                    "NEO4J",
                    ",".join(r["lbls"]),
                    r["id"],
                    json.dumps(serialize_neo4j_value(dict(r["props"]))),
                )
            )
        # Productos
        result = session.run("MATCH (p:Producto) RETURN labels(p) AS lbls, p.id AS id, properties(p) AS props")
        for r in result:
            nodes.append(
                (
                    "NEO4J",
                    ",".join(r["lbls"]),
                    r["id"],
                    json.dumps(serialize_neo4j_value(dict(r["props"]))),
                )
            )
        # Ordenes
        result = session.run("MATCH (o:Orden) RETURN labels(o) AS lbls, o.id AS id, properties(o) AS props")
        for r in result:
            nodes.append(
                (
                    "NEO4J",
                    ",".join(r["lbls"]),
                    r["id"],
                    json.dumps(serialize_neo4j_value(dict(r["props"]))),
                )
            )
        # Relaciones
        result = session.run(
            """
            MATCH (a)-[r]->(b)
            WHERE a.id IS NOT NULL AND b.id IS NOT NULL
            RETURN type(r) AS type, labels(a) AS from_lbls, a.id AS from_id, labels(b) AS to_lbls, b.id AS to_id, properties(r) AS props
            """
        )
        for r in result:
            # Validar que from_id y to_id no sean None
            if r["from_id"] is not None and r["to_id"] is not None:
                edges.append(
                    (
                        "NEO4J",
                        r["type"],
                        ",".join(r["from_lbls"]),
                        str(r["from_id"]),
                        ",".join(r["to_lbls"]),
                        str(r["to_id"]),
                        json.dumps(serialize_neo4j_value(dict(r["props"]))),
                    )
                )

    executemany_chunks(
        "staging.neo4j_nodes",
        ["source_system", "node_label", "node_key", "props_json"],
        nodes,
        chunk_size=5000,
    )
    executemany_chunks(
        "staging.neo4j_edges",
        ["source_system", "edge_type", "from_label", "from_key", "to_label", "to_key", "props_json"],
        edges,
        chunk_size=5000,
    )
    driver.close()


def load_order_items():
    clear_table("staging.neo4j_order_items")
    driver = get_driver()
    rows = []
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Cliente)-[:REALIZO]->(o:Orden)-[r:CONTIENE]->(p:Producto)
            OPTIONAL MATCH (p)-[:Perteneciente_A|:PERTENECE_A]->(cat:Categoria)
            RETURN o.id AS order_id,
                   p.id AS product_id,
                   c.id AS customer_id,
                   cat.id AS category_id,
                   r,
                   o,
                   p
            """
        )
        for rec in result:
            rel_props = dict(rec["r"].items()) if rec.get("r") else {}
            order_node = rec.get("o")
            qty = rel_props.get("cantidad") or rel_props.get("quantity") or 1
            price = rel_props.get("precio_unit") or rel_props.get("unit_price")
            currency = rel_props.get("moneda") or "USD"
            
            # Serializar order_date (puede ser DateTime de Neo4j)
            order_date = None
            if order_node:
                fecha = order_node.get("fecha")
                if fecha:
                    order_date = serialize_neo4j_value(fecha)
                    if isinstance(order_date, str):
                        # Si es string ISO, convertir a date
                        try:
                            from datetime import datetime
                            order_date = datetime.fromisoformat(order_date.replace('Z', '')).date()
                        except:
                            order_date = None
            
            rows.append(
                (
                    "NEO4J",
                    f"{rec['order_id']}-{rec['product_id']}",
                    str(rec["order_id"]),
                    str(rec["product_id"]),
                    str(rec["customer_id"]) if rec["customer_id"] else None,
                    str(rec["category_id"]) if rec["category_id"] else None,
                    float(qty) if qty else 1.0,
                    float(price) if price else 0.0,
                    str(currency),
                    order_date,
                    json.dumps(serialize_neo4j_value(rel_props)),
                )
            )
    executemany_chunks(
        "staging.neo4j_order_items",
        [
            "source_system",
            "source_key",
            "order_key",
            "product_key",
            "customer_key",
            "category_key",
            "quantity",
            "unit_price",
            "currency",
            "order_date",
            "payload_json",
        ],
        rows,
        chunk_size=5000,
    )
    driver.close()


def main():
    load_nodes_and_edges()
    load_order_items()


if __name__ == "__main__":
    main()
