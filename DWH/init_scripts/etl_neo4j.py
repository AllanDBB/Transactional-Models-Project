import json
import logging
import os

from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_neo4j")
load_dotenv()


def get_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "password123")
    return GraphDatabase.driver(uri, auth=(user, pwd))


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
                    json.dumps(dict(r["props"])),
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
                    json.dumps(dict(r["props"])),
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
                    json.dumps(dict(r["props"])),
                )
            )
        # Relaciones
        result = session.run(
            """
            MATCH (a)-[r]->(b)
            RETURN type(r) AS type, labels(a) AS from_lbls, a.id AS from_id, labels(b) AS to_lbls, b.id AS to_id, properties(r) AS props
            """
        )
        for r in result:
            edges.append(
                (
                    "NEO4J",
                    r["type"],
                    ",".join(r["from_lbls"]),
                    r["from_id"],
                    ",".join(r["to_lbls"]),
                    r["to_id"],
                    json.dumps(dict(r["props"])),
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
            order_date = order_node.get("fecha") if order_node else None
            rows.append(
                (
                    "NEO4J",
                    f"{rec['order_id']}-{rec['product_id']}",
                    rec["order_id"],
                    rec["product_id"],
                    rec["customer_id"],
                    rec["category_id"],
                    qty,
                    price,
                    currency,
                    order_date,
                    json.dumps(rel_props),
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
